import os
import pickle
import sys


class PythonPredictor:
    def __init__(self):
        self.model = pickle.load(open(os.path.join(sys.path[0], "model.pkl"), "rb"))

    def predict(self, payload):
        data = payload["user_data"]
        return self.model.predict(data)
