from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.timetables.trigger import CronTriggerTimetable
from datetime import datetime


with DAG('spark_aggregate_px', 
         schedule=CronTriggerTimetable("* * * * *", timezone='UTC'),
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
        verbose = True
    )