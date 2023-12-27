from pyspark.sql import SparkSession
from datetime import datetime
import os

TICKERS = ['BINANCEBTCUSDT']

def aggregate_1m_ohlc(input_path, output_path):
    with SparkSession.builder.appName("aggregation_ohlc").getOrCreate() as spark:
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        print(df.head(10))
        print("readedddddd")
        df.write.csv(output_path, header=True)

if __name__ == "__main__":
    today_str = datetime.today().strftime('%Y%m%d') 
    for ticker in TICKERS:
        print(os.listdir('/opt'))
        input_path = f'/opt/airflow/app-data/{ticker}_{today_str}.csv'
        output_path = f'/opt/airflow/app-result/{ticker}_{today_str}_processed.csv'
        print(f"INPUT: {input_path}")
        print(f"OUTPUT: {output_path}")
        aggregate_1m_ohlc(input_path, output_path)