# tests/test_measurement_data.py
import pytest
from datetime import datetime
from models.measurement_data import MeasurementData

def test_measurement_data_initialization():
    """Тест инициализации объекта измерения."""
    sensor_id = 1
    device_id = 2
    measurement_time = datetime.now()
    raw_data = [{"ts": [1,2,3], "feat1": [4,5,6]}]
    
    measurement = MeasurementData(sensor_id, device_id, measurement_time, raw_data)
    
    assert measurement.sensor_id == sensor_id
    assert measurement.device_id == device_id
    assert measurement.measurement_time == measurement_time
    assert measurement.raw_data == raw_data
    assert measurement.param1 is None
    assert measurement.param2 is None

def test_add_params():
    """Тест добавления параметров калибровки."""
    measurement = MeasurementData(1, 1, None, [])
    measurement.add_params(1.5, 2.5)
    
    assert measurement.param1 == 1.5
    assert measurement.param2 == 2.5