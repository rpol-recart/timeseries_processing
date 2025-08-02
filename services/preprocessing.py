# services/preprocessing.py
from models.measurement_data import MeasurementData
from models.preprocessed_measurement import PreprocessedMeasurement

def preprocess(measurement: MeasurementData) -> PreprocessedMeasurement:
    # Заглушка: возвращает структуру с "обработанными" данными
    processed = {
        "sensor_id": measurement.sensor_id,
        "device_id": measurement.device_id,
        "param1": measurement.param1,
        "param2": measurement.param2,
        "length": sum(len(ts["ts"]) for ts in measurement.raw_data)
    }
    return PreprocessedMeasurement(processed)