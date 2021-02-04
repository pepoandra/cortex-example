import os
import pickle
import sys
import pandas as pd
from redis import StrictRedis



class PythonPredictor:
    def __init__(self, config):
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, 'model.pkl')
        self.model = pickle.load(open(filename, "rb"))

        self.redis = StrictRedis(host=config['host'],
                                 port= config['port'],
                                 encoding="utf-8",
                                 decode_responses=True)
    
    def predict(self, payload):
        member_id = payload['memberId']
        user_data_json = self.redis.get(f'scores:u:{member_id}')
        return user_data_json
        if user_data_json is not None:
            user_data = pd.read_json(user_data_json)
            raw_predictions = self.model.predict(user_data)
        
        if raw_predictions is None or len(raw_predictions.brand.keys()) < 1:
            return self.config
            return {
                'usecase': self.model.to_model_info().usecase,
                'model': self.model.to_model_info().model,
                'version': self.model.to_model_info().version,
                'timestamp': self.model.to_model_info().timestamp,
            }
        result = {
            'predictions': []
        }
        for x in range(len(raw_predictions.brand.keys())):
            item = {
                'brand_id': raw_predictions.brand[x],
                'gender': raw_predictions.gender[x],
                'score': raw_predictions.score[x],
                'liked': raw_predictions.liked[x]
            }
            result['predictions'].append(item)
        return self.config

