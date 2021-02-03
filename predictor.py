import os
import pickle
import sys
import pandas as pd


class PythonPredictor:
    def __init__(self, config):
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, 'model.pkl')
        self.model = pickle.load(open(filename, "rb"))

    def predict(self, payload):
        
        data = payload["user_data"]
        if data is not None:
            user_data = pd.DataFrame(data)
            raw_predictions = self.model.predict(user_data)
        else:
            user_data = pd.DataFrame()
        
        if raw_predictions is None or len(raw_predictions.brand.keys()) < 1:
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
        return result

