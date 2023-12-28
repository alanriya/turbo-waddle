from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

TODAY_DATE = datetime.today().strftime('%Y%m%d') 

def write_to_postgres(input_path='/'):
    conn = PostgresHook("timescale_local_cluster_conn").get_conn()
    import os
    import glob
    directories = os.listdir(input_path)
    today_data_list = []
    for dir in directories:
        if TODAY_DATE in dir:
            today_data_list.append(dir)
    for today_data in today_data_list:
        os.chdir(f'{input_path}/{today_data}')
        filenames = glob.glob("*.csv")
        for filename in filenames:
            # select from timescaledb and get the last date
            with conn.cursor() as cur:
                cur.execute('select count(*) from stock_px')
                result = cur.fetchone()[0]
                if result == 0:
                    # load the whole file
                    PostgresHook("timescale_local_cluster_conn").bulk_load('stock_data.public.stock_px', f'{input_path}/{today_data}/{filename}')
                else:
                    # get the last row in new data set, if does not exists, insert else ignore
                    import pandas as pd
                    df = pd.read_csv(f'{input_path}/{today_data}/{filename}', header=None, delimiter='\t', names=['ts', 'symbol', 'volume', 'open_px', 'high_px', 'low_px', 'close_px'])
                    last_row = df.iloc[-1,:].to_list()
                    cur.execute(f"""select count(*) from stock_px where ts >= '{last_row[0]}'""")
                    result = cur.fetchone()[0]
                    if result == 0:
                        print("inserting new entry")
                        query = f'''INSERT INTO stock_px VALUES ('{last_row[0]}', '{last_row[1]}', {last_row[2]}, {last_row[3]}, {last_row[4]}, {last_row[5]}, {last_row[6]})'''
                        cur.execute(query)
                        conn.commit()
                    else:
                        print("skipping update")
    conn.close()

def check_stock_table_exists():
    hook = PostgresHook('timescale_local_cluster_conn')
    conn = hook.get_conn()
    result = False
    with conn.cursor() as cur:
        cur.execute('''select exists(SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'stock_px')''')
        result = cur.fetchone()[0]
    conn.close()
    if result:
        return 'load_data_to_db'
    else:
        return 'create_table_if_not_exists' 

with DAG('spark_aggregate_px', 
        #  schedule=CronTriggerTimetable("* * * * *", timezone='UTC'),
         schedule = '@once',
         start_date=datetime(2023,12,1),
         catchup=False) as dag:
    submit_spark_job = SparkSubmitOperator(
        task_id = "submit_spark_job",
        application = "/opt/airflow/dags/spark_job/pyspark_aggregate_px.py",
        conn_id = "spark_local_cluster_conn",
        total_executor_cores = '2',
        executor_cores = '1',
        executor_memory = '2g',
        num_executors= '1',
        driver_memory = '4g',
        verbose = False
    )

    check_table_exists = BranchPythonOperator(
        task_id = 'check_table_exists',
        python_callable = check_stock_table_exists
    )

    # create table if not exists in timescale
    create_table_if_not_exists = PostgresOperator(
        task_id = 'create_table_if_not_exists',
        postgres_conn_id = "timescale_local_cluster_conn",
        autocommit = True,
        sql = "sql/create_ohlc_table.sql"
    )

    # load csv into cassandra
    load_data_to_db = PythonOperator(
        task_id = "load_data_to_db",
        python_callable = write_to_postgres,
        op_args = [f'/opt/airflow/app-result'],
        trigger_rule = 'none_failed_min_one_success'
    )

    submit_spark_job >> check_table_exists >> create_table_if_not_exists
    create_table_if_not_exists >> load_data_to_db
    check_table_exists >> load_data_to_db




