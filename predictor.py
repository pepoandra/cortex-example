import os
import pickle
import sys


class PythonPredictor:
    def __init__(self, config):
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, 'model.pkl')
        self.model = pickle.load(open(filename, "rb"))

    def predict(self, payload):
        data = payload["user_data"]
        raw_predictions = self.model.predict(data)
        
        if raw_predictions is None or len(raw_predictions.brand.keys()) < 1:
            return []

        predictions = list(map(
            lambda k: Prediction(
                brand_id=raw_predictions.brand[k],
                gender=raw_predictions.gender[k],
                score=raw_predictions.score[k],
                liked=raw_predictions.liked[k]
            ), raw_predictions.brand.keys()))

        return predictions

