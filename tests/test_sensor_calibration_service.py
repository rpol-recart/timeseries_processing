# tests/test_sensor_calibration_service.py
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from models.measurement_data import MeasurementData
from services.sensor_calibration_service import SensorCalibrationService

@pytest.fixture
def calibration_service():
    return SensorCalibrationService()

def test_recalibrate_for_sensor_change_success(calibration_service):
    """Тест успешной перекалибровки при смене сенсора."""
    old_sensor = 1
    new_sensor = 2
    context = {"prediction_time": datetime.now()}

    # Создаем измерения с разной длиной временных рядов для демонстрации сдвига
    old_measurement1 = MeasurementData(sensor_id=1, device_id=1, 
                                     measurement_time=None, 
                                     raw_data=[{"ts": [1,2,3], "feat1": [1,2,3], "feat2": [1,2,3]}])
    old_measurement2 = MeasurementData(sensor_id=1, device_id=2, 
                                     measurement_time=None, 
                                     raw_data=[{"ts": [1,2,3,4], "feat1": [1,2,3,4], "feat2": [1,2,3,4]}])
    
    new_measurement1 = MeasurementData(sensor_id=2, device_id=1, 
                                     measurement_time=None, 
                                     raw_data=[{"ts": [1,2,3,4,5], "feat1": [1,2,3,4,5], "feat2": [1,2,3,4,5]}])
    new_measurement2 = MeasurementData(sensor_id=2, device_id=2, 
                                     measurement_time=None, 
                                     raw_data=[{"ts": [1,2,3,4,5,6], "feat1": [1,2,3,4,5,6], "feat2": [1,2,3,4,5,6]}])

    measurements = [new_measurement1, new_measurement2]

    with patch.object(calibration_service, '_get_calibration_pairs',
                    return_value=([old_measurement1, old_measurement2],
                                [new_measurement1, new_measurement2])):
        params = calibration_service.recalibrate_for_sensor_change(
            old_sensor, new_sensor, measurements, context
        )
    
    # Проверяем что возвращаются значения float и param2 > param1
    assert isinstance(params[0], float)
    assert isinstance(params[1], float)
    assert params[1] > params[0]  # По логике реализации param2 должен быть больше param1

def test_recalibrate_for_sensor_change_insufficient_data(calibration_service, caplog):
    """Тест ошибки при недостаточных данных для калибровки."""
    old_sensor = 1
    new_sensor = 2
    context = {"prediction_time": datetime.now()}
    
    measurements = [MeasurementData(sensor_id=2, device_id=1, measurement_time=None, raw_data=[])]
    
    with patch.object(calibration_service, '_get_calibration_pairs', return_value=([], [])):
        with caplog.at_level("ERROR"):
            with pytest.raises(ValueError, match="Insufficient calibration data"):
                calibration_service.recalibrate_for_sensor_change(
                    old_sensor, new_sensor, measurements, context
                )
    
    assert "Calibration failed" in caplog.text

def test_get_calibration_pairs(calibration_service):
    """Тест получения парных измерений для калибровки."""
    old_sensor = 1
    new_sensor = 2
    context = {"prediction_time": datetime.now()}
    
    measurements = [
        MeasurementData(sensor_id=2, device_id=1, measurement_time=None, raw_data=[]),
        MeasurementData(sensor_id=2, device_id=2, measurement_time=None, raw_data=[])
    ]
    
    with patch.object(calibration_service, '_group_measurements_by_device', return_value={1: None, 2: None}):
        with patch.object(calibration_service, '_get_old_measurements_for_devices', return_value={1: None, 2: None}):
            old_m, new_m = calibration_service._get_calibration_pairs(
                old_sensor, new_sensor, measurements, context
            )
    
    assert len(old_m) == 2
    assert len(new_m) == 2