# services/sensor_calibration_service.py
import logging
from typing import Tuple, List
from models.measurement_data import MeasurementData
from services.sensor_shift_detector import detect_measurement_shift

logger = logging.getLogger(__name__)

class SensorCalibrationService:
    """Сервис для калибровки параметров при смене сенсора."""
    
    def recalibrate_for_sensor_change(
        self, 
        old_sensor: int, 
        new_sensor: int, 
        measurements: List[MeasurementData], 
        context: dict
    ) -> Tuple[float, float]:
        """
        Выполняет перекалибровку параметров при смене сенсора.
        
        Args:
            old_sensor: ID старого сенсора
            new_sensor: ID нового сенсора
            measurements: Список новых измерений
            context: Контекст последнего предсказания
            
        Returns:
            Tuple[float, float]: Новые параметры калибровки (param1, param2)
        """
        try:
            # Получение парных измерений для калибровки
            old_measurements, new_measurements = self._get_calibration_pairs(
                old_sensor, new_sensor, measurements, context
            )
            
            if len(old_measurements) < 2:
                raise ValueError(f"Insufficient calibration data: only {len(old_measurements)} device pairs")
                
            # Вычисление новых параметров
            param1, param2 = detect_measurement_shift(old_measurements, new_measurements)
            
            logger.info(f"Calibration completed: param1={param1}, param2={param2}")
            return param1, param2
            
        except Exception as e:
            logger.error(f"Calibration failed: {e}")
            raise
            
    def _get_calibration_pairs(
        self, 
        old_sensor: int, 
        new_sensor: int, 
        measurements: List[MeasurementData], 
        context: dict
    ) -> Tuple[List[MeasurementData], List[MeasurementData]]:
        """
        Получает парные измерения для калибровки со старого и нового сенсоров.
        """
        from services.data_fetcher import DataFetcher
        
        # Получение измерений с нового сенсора (по одному на устройство)
        new_measurements_by_device = self._group_measurements_by_device(measurements, new_sensor)
        
        if len(new_measurements_by_device) < 2:
            raise ValueError(f"Insufficient devices on new sensor: {len(new_measurements_by_device)}")
            
        # Получение соответствующих измерений со старого сенсора
        old_measurements_by_device = self._get_old_measurements_for_devices(
            old_sensor, new_measurements_by_device.keys(), context["prediction_time"]
        )
        
        # Определение общих устройств
        common_devices = set(old_measurements_by_device.keys()) & set(new_measurements_by_device.keys())
        
        if len(common_devices) < 2:
            raise ValueError(f"Insufficient common devices: {len(common_devices)}")
            
        logger.info(f"Calibration will use {len(common_devices)} device pairs")
        
        # Формирование согласованных списков
        sorted_devices = sorted(common_devices)
        old_measurements = [old_measurements_by_device[dev] for dev in sorted_devices]
        new_measurements = [new_measurements_by_device[dev] for dev in sorted_devices]
        
        return old_measurements, new_measurements
        
    def _group_measurements_by_device(self, measurements: List[MeasurementData], sensor_id: int) -> dict:
        """Группирует измерения по устройствам для указанного сенсора."""
        grouped = {}
        for measurement in measurements:
            if measurement.sensor_id == sensor_id and measurement.device_id not in grouped:
                grouped[measurement.device_id] = measurement
        return grouped
        
    def _get_old_measurements_for_devices(self, old_sensor: int, device_ids: set, timestamp) -> dict:
        """Получает последние измерения со старого сенсора для указанных устройств."""
        # Этот метод нужно будет интегрировать с DataFetcher
        # Пока заглушка
        return {}