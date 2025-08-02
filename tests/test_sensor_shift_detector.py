# tests/test_sensor_shift_detector.py
import pytest
from models.measurement_data import MeasurementData
from services.sensor_shift_detector import detect_measurement_shift

def test_detect_measurement_shift():
    """Тест обнаружения сдвига между измерениями."""
    old_measurements = [
        MeasurementData(sensor_id=1, device_id=1, measurement_time=None, raw_data=[{"ts": [1,2,3]}]),
        MeasurementData(sensor_id=1, device_id=2, measurement_time=None, raw_data=[{"ts": [1,2,3,4]}])
    ]
    
    new_measurements = [
        MeasurementData(sensor_id=2, device_id=1, measurement_time=None, raw_data=[{"ts": [1,2,3,4,5]}]),
        MeasurementData(sensor_id=2, device_id=2, measurement_time=None, raw_data=[{"ts": [1,2,3,4,5,6]}])
    ]
    
    param1, param2 = detect_measurement_shift(old_measurements, new_measurements)
    
    assert isinstance(param1, float)
    assert isinstance(param2, float)
    assert param2 > param1  # param2 должен быть больше param1 по логике реализации

def test_detect_measurement_shift_mismatched_length():
    """Тест ошибки при несоответствии количества измерений."""
    old_measurements = [MeasurementData(sensor_id=1, device_id=1, measurement_time=None, raw_data=[])]
    new_measurements = [
        MeasurementData(sensor_id=2, device_id=1, measurement_time=None, raw_data=[]),
        MeasurementData(sensor_id=2, device_id=2, measurement_time=None, raw_data=[])
    ]
    
    with pytest.raises(ValueError, match="Mismatched number"):
        detect_measurement_shift(old_measurements, new_measurements)
        