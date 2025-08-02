# services/preprocessing.py
from models.measurement_data import MeasurementData
from models.preprocessed_measurement import PreprocessedMeasurement

def preprocess(measurement: MeasurementData) -> PreprocessedMeasurement:
    # Заглушка: возвращает структуру с "обработанными" данными
    # todo normalize_time_series
    processed = {
        "sensor_id": measurement.sensor_id,
        "device_id": measurement.device_id,
        "param1": measurement.param1,
        "param2": measurement.param2,
        "measurement_count":measurement.measurement_count,
        "measurement_time": measurement.measurement_time,
        "time_series":measurement.raw_data, # must be normalized timeseries
        "length": sum(len(ts["ts"]) for ts in measurement.raw_data)
    }
    return PreprocessedMeasurement(processed)