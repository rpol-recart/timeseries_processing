# tests/test_measurement_processor.py
import pytest
from unittest.mock import MagicMock
from models.measurement_data import MeasurementData
from services.measurement_processor import MeasurementProcessor

def test_process_batch_success():
    """Тест успешной обработки батча измерений."""
    db = MagicMock()
    processor = MeasurementProcessor(db)
    
    measurements = [
        MeasurementData(sensor_id=1, device_id=1, measurement_time=None, raw_data=[]),
        MeasurementData(sensor_id=1, device_id=2, measurement_time=None, raw_data=[])
    ]
    
    processed_count = processor.process_batch(measurements, (1.0, 2.0))
    
    assert processed_count == 2
    db.insert_prediction.assert_called()

def test_process_batch_with_failures(caplog):
    """Тест обработки батча с ошибками."""
    db = MagicMock()
    db.insert_prediction.side_effect = [None, Exception("DB error")]
    processor = MeasurementProcessor(db)
    
    measurements = [
        MeasurementData(sensor_id=1, device_id=1, measurement_time=None, raw_data=[]),
        MeasurementData(sensor_id=1, device_id=2, measurement_time=None, raw_data=[])
    ]
    
    with caplog.at_level("ERROR"):
        processed_count = processor.process_batch(measurements, (1.0, 2.0))
    
    assert processed_count == 1
    assert "Failed to process measurement" in caplog.text
    