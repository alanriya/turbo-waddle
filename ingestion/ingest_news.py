import json
import os, sys
from kafka import KafkaProducer 
import kafka
import logging
import finnhub
import time
sys.path.append(os.getcwd())
from ingestion.utils import load_json
import random

def process_visa_application_data(data:dict):
    if not isinstance(data, dict):
        raise Exception('data should be of dict datatype')
    if len(data.get('data')) == 0:
        logging.warning(f"No data for {data.get('symbol')}")
        return
    company = data.get('symbol')
    if len(data.get('data')) != 0:
        for record in data.get('data'):
            returned_data = {}
            returned_data['company'] = company
            for key in record.keys():
                returned_data[key] = record[key]
            kafka_publish(producer, data)
            logging.info(f"published to kafka {data.get('company')} {data.get('jobTitle')}")


config = load_json('ingestion/config.json')
TOPIC_NAME = 'alternative_data_visa_application'
KAFKA_SERVER = f"{config.get('KAFKA_HOST')}:{config.get('KAFKA_PORT')}"
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, api_version_auto_timeout_ms=20000)

def kafka_publish(kafka_producer: KafkaProducer, data: dict, topic: str = TOPIC_NAME):
    if not isinstance(kafka_producer, kafka.producer.kafka.KafkaProducer): 
        raise Exception(f"kafka_producer must be a KafkaProducer Object, detected {type(kafka_producer)}")
    if not isinstance(data, dict): 
        raise Exception("Expected data to be in dictionary format")
    if not isinstance(topic, str) and len(topic) > 0:
        raise Exception("Ensure that the topic is string and the length of topic is > 0")
    kafka_producer.send(topic=topic, value=bytes(str(data), "utf-8"))
    kafka_producer.flush()

if __name__ == "__main__":
    config = load_json('ingestion/config.json')
    finnhub_client = finnhub.Client(api_key=config.get('FINHUB_API_KEY'))
    # data = finnhub_client.stock_visa_application("MSFT", "2021-01-01", "2021-")
    CHOICES = ['AAPL', 'META', "AMZN", "TSLA", "NVDA", "GOOG", "META"]
    DATE_CHOICES = [("2021-01-01", "2021-12-31"), ("2020-01-01", "2020-12-31"), ("2019-01-01", "2019-12-31")]
    # import pdb
    # pdb.set_trace()
    while True:
        company = random.choice(CHOICES)
        data_choice = random.choice(DATE_CHOICES)
        print(f"getting data for company: {company}")
        print(f"getting date range: {data_choice}")
        data = finnhub_client.stock_visa_application(company, data_choice[0], data_choice[1])
        # sudden influx of data pump to kafka that needs on demand processing
        process_visa_application_data(data)
        time.sleep(random.randint(50, 300))
        

