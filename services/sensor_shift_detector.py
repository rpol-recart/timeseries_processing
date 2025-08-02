# services/sensor_shift_detector.py
from models.measurement_data import MeasurementData

def detect_measurement_shift(measurements: list[MeasurementData]) -> tuple[float, float]:
    # Заглушка: вычисляет param1 и param2 на основе 4 измерений
    avg_len = sum(sum(len(ts["ts"]) for ts in m.raw_data) for m in measurements) / len(measurements)
    param1 = avg_len * 0.001
    param2 = avg_len * 0.002
    return round(param1, 4), round(param2, 4)