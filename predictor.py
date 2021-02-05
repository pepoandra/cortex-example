import os
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pickle
import sys
import pandas as pd
from redis import StrictRedis


class PythonPredictor:
    def __init__(self, config):
        if('bucket' in config.keys()):
            if os.environ.get("AWS_ACCESS_KEY_ID"):
                s3 = boto3.client("s3")  # client will use your credentials if available
            else:
                s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))  # anonymous client
            s3.download_file(config["bucket"], config["key"], "/tmp/model.pkl")
            self.model = pickle.load(open("/tmp/model.pkl", "rb"))
            self.source = 's3'
        else:
            dirname = os.path.dirname(__file__)
            filename = os.path.join(dirname, 'model.pkl')
            self.model = pickle.load(open(filename, "rb"))
            self.source = 'file'
            
        self.redis = StrictRedis(host=config['host'],
                                 port= config['port'],
                                 encoding="utf-8",
                                 decode_responses=True)
    
    def predict(self, payload):
        response = {
                'env': 'cortex',
                'source': self.source,
                'usecase': self.model.to_model_info().usecase,
                'model': self.model.to_model_info().model,
                'version': self.model.to_model_info().version,
                'timestamp': self.model.to_model_info().timestamp,
                'predictions': []
            }
        member_id = payload['memberId']
        user_data_json = self.redis.get(f'scores:u:{member_id}')
        if user_data_json is not None:
            user_data = pd.read_json(user_data_json)
            raw_predictions = self.model.predict(user_data)
        
            if raw_predictions is None or len(raw_predictions.brand.keys()) < 1:
                return response
    
            for x in range(len(raw_predictions.brand.keys())):
                item = {
                    'brand_id': raw_predictions.brand[x],
                    'gender': raw_predictions.gender[x],
                    'score': raw_predictions.score[x],
                    'liked': raw_predictions.liked[x]
                }
                response['predictions'].append(item)
            
        return response
