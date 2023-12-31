# Ingest Data from finnhub, publish to batch layer as parquet file
import websocket
import json
import os, sys

from kafka import KafkaProducer 
import kafka

sys.path.append(os.getcwd())
from ingestion.utils import load_json

class FinnhubTradeNormalizer:
    def __init__(self):
        pass

    @staticmethod
    def process_trade_data(data: dict):
        if not isinstance(data, dict):
            raise Exception("Expected data to be in dictionary format")
        data_type = data.get('type')
        dict_list = data.get('data')
        for data_dict in dict_list:
            returned_data = {}
            returned_data["symbol"] = data_dict.get('s')
            returned_data["last_px"] = data_dict.get('p')
            returned_data["timestamp"] = data_dict.get('t')
            returned_data["volume"] = data_dict.get('v')
            returned_data["condition"] = '' if data_dict.get('c') is None else data_dict.get('c')
            returned_data["message_type"] = data_type
            yield returned_data 
    
    @staticmethod
    def publish_price(kafka_producer: KafkaProducer, data: dict, topic: str):
        if not isinstance(kafka_producer, kafka.producer.kafka.KafkaProducer): 
            raise Exception(f"kafka_producer must be a KafkaProducer Object, detected {type(kafka_producer)}")
        if not isinstance(data, dict): 
            raise Exception("Expected data to be in dictionary format")
        if not isinstance(topic, str) and len(topic) > 0:
            raise Exception("Ensure that the topic is string and the length of topic is > 0")
        kafka_producer.send(topic=topic, value=bytes(str(data), "utf-8"))
        kafka_producer.flush()

config = load_json('ingestion/config.json')
TOPIC_NAME = 'tick_trade_px'
KAFKA_SERVER = f"{config.get('KAFKA_HOST')}:{config.get('KAFKA_PORT')}"
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, api_version_auto_timeout_ms=20000)
def on_message(ws, message):
    # print(message)
    parsed_message = json.loads(message)
    # Run normaliser
    for kafka_msg in FinnhubTradeNormalizer.process_trade_data(parsed_message):
        # can pump to kafka, kafka will organise and then set sink as parquet file to be stored on S3.
        FinnhubTradeNormalizer.publish_price(producer, kafka_msg, TOPIC_NAME)

def on_error(ws, error):
    print(error)

def on_close(ws, status_code, status_message):
    print(f"Closing websocket connection with status code 0")

def on_open(ws):
    print("Opening Websocket Connections")
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

if __name__ == "__main__":
    config = load_json('ingestion/config.json')
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={config.get('FINHUB_API_KEY')}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()