from kafka import KafkaConsumer
import os, sys
sys.path.append(os.getcwd())
from ingestion.utils import load_json
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings 
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import SimpleStringSchema

config = load_json('ingestion/config.json')

TOPIC_NAME = 'alternative_data_visa_application'
KAFKA_SERVER = f"{config.get('KAFKA_HOST')}:{config.get('KAFKA_PORT')}"

def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)

    settings = EnvironmentSettings.new_instance()\
                      .in_streaming_mode() \
                      .build()

    # create table environment
    tbl_env = StreamTableEnvironment.create(stream_execution_environment=env,
                                            environment_settings=settings)

    # add kafka connector dependency
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka_3.0.2-1.18.jar')
    
    tbl_env.get_config()\
            .get_configuration()\
            .set_string("pipeline.jars", "file://{}".format(kafka_jar))
    
    src_ddl = f"""
    CREATE TABLE visa_datasource (
        company VARCHAR,
        `year` VARCHAR,
        `quarter` VARCHAR,
        symbol VARCHAR,
        caseNumber VARCHAR,
        caseStatus VARCHAR,
        receivedDate DATE,
        visaClass VARCHAR,
        jobTitle VARCHAR,
        socCode VARCHAR,
        fullTimePosition VARCHAR,
        beginDate DATE,
        endDate DATE,
        employerName VARCHAR,
        worksiteAddress VARCHAR,
        worksiteCity VARCHAR,
        worksiteCounty VARCHAR,
        worksiteState VARCHAR,
        worksitePostalCode VARCHAR,
        wageRangeFrom DOUBLE,
        wageRangeTo DOUBLE,
        wageUnitOfPay VARCHAR,
        wageLevel VARCHAR,
        h1bDependent VARCHAR,
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'alternative_data_visa_application',
        'properties.bootstrap.servers' = '{KAFKA_SERVER}',
        'properties.group.id' = 'test-consumer-group',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
    """
    
    tbl_env.execute_sql(src_ddl)

    # create and initiate loading of source Table
    tbl = tbl_env.from_path('visa_datasource')

    print('\nSource Schema')
    tbl.print_schema()

    sql = """
    select jobTitle as job_title,
        TUMBLE_END(proctime, INTERVAL '60' SECONDS) as window_end,
        avg(wageRangeFrom) as average_lower_bound
        from visa_datasource
        GROUP BY
            TUMBLE_END(proctime, INTERVAL '60' SECONDS),
            jobTitle
    """

    avg_wage_tbl = tbl_env.sql_query(sql)

    print('\nProcess Sink Schema')
    avg_wage_tbl.print_schema()

    sink_ddl = """
        CREATE TABLE wage_sink (
            job_title VARCHAR,
            window_end TIMESTAMP(3),
            average_lower_bound DOUBLE
        ) WITH (
            'connector' =  'jdbc',
            'url' = 'jdbc:postgresql://alan:pwd@localhost:5433/stock_data'
            'table-name' = 'wage_sink_kafka'
        )
    """

    tbl_env.execute_sql(sink_ddl)

    # write time windowed aggregations to sink table
    avg_wage_tbl.execute_insert('wage_sink').wait()

    tbl_env.execute('wage_sink_compute')

if __name__ == '__main__':
    main()
