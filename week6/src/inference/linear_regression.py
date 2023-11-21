from sklearn.linear_model import LinearRegression
import pickle
from inference.base import Predictor


class LinearRegression(Predictor):
    def __init__(self):
        self.model = LinearRegression()
    
    def load_model(self, model_path: str):
        self.model = pickle.dumps(model_path)
        
    def predict(self, input):
        return self.predict(input)