# services/sensor_shift_detector.py
from models.measurement_data import MeasurementData

# services/sensor_shift_detector.py
from models.measurement_data import MeasurementData

def detect_measurement_shift(
    old_measurements: list[MeasurementData], 
    new_measurements: list[MeasurementData]
) -> tuple[float, float]:
    """
    Анализирует сдвиг сигнала при смене сенсора.
    old_measurements: список измерений со старого сенсора (по одному на устройство).
    new_measurements: список измерений с нового сенсора (по одному на устройство).
    Длина списков одинакова, устройства соответствуют.
    """
    if len(old_measurements) != len(new_measurements):
        raise ValueError("Mismatched number of old and new measurements")

    total_shift = 0.0
    for i, (old_m, new_m) in enumerate(zip(old_measurements, new_measurements)):
        # Пример: сравнение средней длины временных рядов
        old_len = sum(len(ts["ts"]) for ts in old_m.raw_data)
        new_len = sum(len(ts["ts"]) for ts in new_m.raw_data)
        shift = abs(new_len - old_len) / old_len  # относительное изменение
        total_shift += shift

    avg_shift = total_shift / len(old_measurements)
    param1 = avg_shift * 1.5
    param2 = avg_shift * 2.0

    return round(param1, 4), round(param2, 4)