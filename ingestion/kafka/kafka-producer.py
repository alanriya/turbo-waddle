from kafka import KafkaProducer
import os, sys
sys.path.append(os.getcwd())
from ingestion.utils import load_json

config = load_json('ingestion/config.json')

TOPIC_NAME = 'test_pump_1'
KAFKA_SERVER = f"{config.get('KAFKA_HOST')}:{config.get('KAFKA_PORT')}"

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, api_version_auto_timeout_ms=20000)

for i in range(1,11):
    producer.send(TOPIC_NAME, b'test boy')
    producer.flush()