from kafka import KafkaConsumer
import os, sys
sys.path.append(os.getcwd())
from ingestion.utils import load_json
from ingestion.schema_finnhub import schema
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json

config = load_json('ingestion/config.json')

class FinnhubConsumer:
    def __init__(self, topic, servers):
        self.topic = topic
        self.servers = servers
        self.sink = config.get('FINNHUB_DIRECTORY')
        self.col_names = ['symbol', 'last_px', 'timestamp', 'volume', 'condition', 'message_type', 'sys_timestamp', 'offset']
        
    def consume(self):
        consume_messages = KafkaConsumer(self.topic, bootstrap_servers=self.servers)
        for message in consume_messages:
            try:
                decoded_message = message.value.decode("utf-8").replace("\'", "\"")
                decoded_message = json.loads(decoded_message)
                decoded_message["timestamp"] = datetime.datetime.fromtimestamp(decoded_message.get("timestamp")//1000).strftime('%Y-%m-%d %H:%M:%S')
                decoded_message['sys_timestamp'] = datetime.datetime.fromtimestamp(message.timestamp//1000).strftime('%Y-%m-%d %H:%M:%S')
                # stream the data to parquet file
                decoded_message['offset'] = message.offset
                self.stream_to_csv(decoded_message)

            except Exception as e:
                print(f"Unable to decode: {e}, skipping this entry")
    
    def stream_to_json(self, data:dict):
        '''
        data: dict
        '''
        symbol = data.get("symbol").replace(":","")
        current_date = data.get("timestamp")[:10].replace("-", "")
        # df = pd.DataFrame(data, index=[0])
        with open(f'{self.sink}/{symbol}_{current_date}.json', mode='a+') as file:
            file.write(str(data))
            file.write("\n")

    def stream_to_csv(self, data:dict):
        symbol = data.get("symbol").replace(":","")
        current_date = data.get("timestamp")[:10].replace("-", "")
        if not os.path.exists(f'{self.sink}/{symbol}_{current_date}.csv'):
            with open(f'{self.sink}/{symbol}_{current_date}.csv', mode='a+') as file:
                file.write(','.join(self.col_names))
                file.write("\n")
        with open(f'{self.sink}/{symbol}_{current_date}.csv', mode='a+') as file: 
            val_list = [] 
            for col in self.col_names:
                val_list.append(str(data.get(col,''))) 
            file.write(','.join(val_list))
            file.write("\n")

if __name__ == "__main__":
    TOPIC_NAME = 'tick_trade_px'
    KAFKA_SERVER = f"{config.get('KAFKA_HOST')}:{config.get('KAFKA_PORT')}"
    consumer = FinnhubConsumer(TOPIC_NAME,servers=KAFKA_SERVER)
    consumer.consume()