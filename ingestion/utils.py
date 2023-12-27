import json
import logging 

def load_json(file_path):
    data = {}
    with open(file_path, 'r') as f:
        data = json.load(f)
        logging.info(f'json data from {file_path} is successfully loaded')
    return data