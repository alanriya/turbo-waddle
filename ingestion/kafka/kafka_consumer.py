from kafka import KafkaConsumer
import os, sys
sys.path.append(os.getcwd())
from ingestion.utils import load_json
import datetime
import json

config = load_json('ingestion/config.json')

TOPIC_NAME = 'alternative_data_visa_application'
KAFKA_SERVER = f"{config.get('KAFKA_HOST')}:{config.get('KAFKA_PORT')}"

consumer = KafkaConsumer(TOPIC_NAME,bootstrap_servers=KAFKA_SERVER)

for message in consumer:
    decoded_message = message.value.decode("utf-8").replace("\'", "\"")
    # print(json.loads(decoded_message))
    print(message)


