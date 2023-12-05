from abc import ABC, abstractmethod

class Predictor(ABC):
    @abstractmethod
    def load_model(model_path: str):
        pass
    
    @abstractmethod
    def predict(input):
        pass