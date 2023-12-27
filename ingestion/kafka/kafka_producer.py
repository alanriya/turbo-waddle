from kafka import KafkaProducer
import os, sys
sys.path.append(os.getcwd())
from ingestion.utils import load_json

config = load_json('ingestion/config.json')

TOPIC_NAME = 'tick_trade_px'
KAFKA_SERVER = f"{config.get('KAFKA_HOST')}:{config.get('KAFKA_PORT')}"

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, api_version_auto_timeout_ms=20000)

class CustomedProducer(KafkaProducer):
    def __init__(self, topic: str = '', configs: dict = {}):
        if topic == '':
            raise Exception(f'topic for kafka messages cannot be empty string {topic}' )
        if not isinstance(configs, dict):
            raise Exception(f"{configs} must be a dictionary object")
        super(CustomedProducer, self).__init__(self, **configs)
        self.topic = topic

for i in range(1,11):
    producer.send(TOPIC_NAME, b'test boy')
    producer.flush()