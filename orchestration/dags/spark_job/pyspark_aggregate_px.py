from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# import col, substring, min, max, first, last, sum
from datetime import datetime
import os

TICKERS = ['BINANCEBTCUSDT']

def aggregate_1m_ohlc(input_path, output_path):
    with SparkSession.builder.config('job.local.dir', '/opt/airflow/app-result') \
                            .appName("aggregation_ohlc") \
                            .master("local[*]") \
                            .getOrCreate() as spark:

        df = spark.read.csv(input_path, header=True, inferSchema=True)
        # can aggregate to here and save as ohlc

        #  substring(df['timestamp'], 0, 16).alias('min_timestamp')).show()
        transformed_df = df.withColumn('min_timestamp', F.col('timestamp').substr(0, 16)) \
        .select('symbol', 'last_px', 'volume', 'min_timestamp') \
        .groupBy("min_timestamp") \
        .agg( 
            F.first('symbol').alias('symbol'),
            F.sum("volume").alias("volume"),
            F.first("last_px").alias("open"),
            F.max("last_px").alias("high"),
            F.min("last_px").alias("low"),
            F.last("last_px").alias("close")
        ).orderBy(F.col("min_timestamp").asc())
        transformed_df.write.mode("overwrite").options(header='False', delimiter='\t').csv(output_path)

if __name__ == "__main__":
    today_str = datetime.today().strftime('%Y%m%d') 
    for ticker in TICKERS:
        input_path = f'/opt/airflow/app-data/{ticker}_{today_str}.csv'
        # output_path = f'/opt/airflow/app-result/{ticker}_{today_str}_processed.csv'
        output_path = f'file:///opt/airflow/app-result/{ticker}_{today_str}'
        aggregate_1m_ohlc(input_path, output_path)