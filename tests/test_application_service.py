# tests/test_application_service.py
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from models.measurement_data import MeasurementData
from services.application_service import ApplicationService

@pytest.fixture
def mock_services():
    """Создает моки всех зависимых сервисов."""
    db = MagicMock()
    data_fetcher = MagicMock()
    sensor_change_detector = MagicMock()
    calibration_service = MagicMock()
    measurement_processor = MagicMock()
    
    return {
        'db': db,
        'data_fetcher': data_fetcher,
        'sensor_change_detector': sensor_change_detector,
        'calibration_service': calibration_service,
        'measurement_processor': measurement_processor
    }

def test_process_measurements_no_context(mock_services, caplog):
    """Тест обработки без контекста (нет последнего предсказания)."""
    mock_services['data_fetcher'].get_last_prediction.return_value = None
    
    app_service = ApplicationService()
    app_service.db = mock_services['db']
    app_service.data_fetcher = mock_services['data_fetcher']
    
    with caplog.at_level("ERROR"):
        app_service.process_measurements()
    
    assert "Cannot establish processing context" in caplog.text

def test_process_measurements_no_new_data(mock_services, caplog):
    """Тест обработки без новых измерений."""
    mock_services['data_fetcher'].get_last_prediction.return_value = {
        "prediction_time": datetime.now(),
        "sensor_id": 1
    }
    mock_services['data_fetcher'].get_new_measurements.return_value = []
    
    app_service = ApplicationService()
    app_service.db = mock_services['db']
    app_service.data_fetcher = mock_services['data_fetcher']
    
    with caplog.at_level("INFO"):
        app_service.process_measurements()
    
    assert "No new measurements to process" in caplog.text

def test_process_measurements_with_sensor_change(mock_services):
    """Тест обработки со сменой сенсора."""
    context = {
        "prediction_time": datetime.now(),
        "sensor_id": 1,
        "param1": 1.0,
        "param2": 2.0
    }
    measurements = [
        MeasurementData(sensor_id=2, device_id=1, measurement_time=datetime.now(), raw_data=[]),
        MeasurementData(sensor_id=2, device_id=2, measurement_time=datetime.now(), raw_data=[])
    ]
    
    mock_services['data_fetcher'].get_last_prediction.return_value = context
    mock_services['data_fetcher'].get_new_measurements.return_value = measurements
    mock_services['sensor_change_detector'].detect_change.return_value = (True, 2)
    mock_services['calibration_service'].recalibrate_for_sensor_change.return_value = (1.5, 2.5)
    
    app_service = ApplicationService()
    app_service.db = mock_services['db']
    app_service.data_fetcher = mock_services['data_fetcher']
    app_service.sensor_change_detector = mock_services['sensor_change_detector']
    app_service.calibration_service = mock_services['calibration_service']
    app_service.measurement_processor = mock_services['measurement_processor']
    
    app_service.process_measurements()
    
    mock_services['sensor_change_detector'].detect_change.assert_called_once_with(1, measurements)
    mock_services['calibration_service'].recalibrate_for_sensor_change.assert_called_once()
    mock_services['measurement_processor'].process_batch.assert_called_once_with(measurements, (1.5, 2.5))

def test_cleanup_closes_db_pool(mock_services, caplog):
    """Тест корректного закрытия пула соединений."""
    app_service = ApplicationService()
    app_service.db = mock_services['db']
    
    with caplog.at_level("INFO"):
        app_service.cleanup()
    
    mock_services['db'].close_pool.assert_called_once()
    assert "Database resources cleaned up" in caplog.text