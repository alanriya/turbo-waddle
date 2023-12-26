from kafka import KafkaConsumer
import os, sys
sys.path.append(os.getcwd())
from ingestion.utils import load_json

config = load_json('ingestion/config.json')

TOPIC_NAME = 'test_pump_1'
KAFKA_SERVER = f"{config.get('KAFKA_HOST')}:{config.get('KAFKA_PORT')}"

consumer = KafkaConsumer(TOPIC_NAME,bootstrap_servers=KAFKA_SERVER)

for message in consumer:
    print(message)