# services/prediction.py
import time
import random
from models.preprocessed_measurement import PreprocessedMeasurement

def predict(preprocessed: PreprocessedMeasurement) -> float:
    # Имитация ML-модели с таймаутом до 3 сек
    time.sleep(0.1)  # Имитация задержки
    return round(random.uniform(0.1, 0.9), 4)