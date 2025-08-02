# tests/test_sensor_change_detector.py
import pytest
from unittest.mock import MagicMock
from models.measurement_data import MeasurementData
from services.sensor_change_detector import SensorChangeDetector

def test_detect_change_no_change():
    """Тест обнаружения смены сенсора (без изменений)."""
    data_fetcher = MagicMock()
    detector = SensorChangeDetector(data_fetcher)
    
    current_sensor = 1
    measurements = [
        MeasurementData(sensor_id=1, device_id=1, measurement_time=None, raw_data=[]),
        MeasurementData(sensor_id=1, device_id=2, measurement_time=None, raw_data=[])
    ]
    
    changed, new_sensor = detector.detect_change(current_sensor, measurements)
    
    assert not changed
    assert new_sensor is None

def test_detect_change_with_change():
    """Тест обнаружения смены сенсора (с изменением)."""
    data_fetcher = MagicMock()
    detector = SensorChangeDetector(data_fetcher)
    
    current_sensor = 1
    measurements = [
        MeasurementData(sensor_id=2, device_id=1, measurement_time=None, raw_data=[]),
        MeasurementData(sensor_id=2, device_id=2, measurement_time=None, raw_data=[])
    ]
    
    changed, new_sensor = detector.detect_change(current_sensor, measurements)
    
    assert changed
    assert new_sensor == 2