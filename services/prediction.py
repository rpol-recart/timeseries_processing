# services/prediction.py
import time
import random
from models.preprocessed_measurement import PreprocessedMeasurement
from config import DEGRADATION_CONFIG

def degradation_shift_calculator(measurements_on_sensor):
    # Расчет деградации от количества использований
    e_coef=DEGRADATION_CONFIG.linear_slope_e*measurements_on_sensor+DEGRADATION_CONFIG.linear_intercept_e
    i_coef=DEGRADATION_CONFIG.linear_slope_i*measurements_on_sensor+DEGRADATION_CONFIG.linear_intercept_i
    return (e_coef,i_coef)

def predict(preprocessed: PreprocessedMeasurement) -> float:
    # Имитация ML-модели с таймаутом до 3 сек
    time.sleep(0.1)  # Имитация задержки
    #todo keypoint detector
    degradation_coefs=degradation_shift_calculator(preprocessed.measurements_count)
    # prediction=model(key_points_list,preeprocessed.param1,preprocessed.param2,degradation_coefs)
    return round(random.uniform(0.1, 0.9), 4)