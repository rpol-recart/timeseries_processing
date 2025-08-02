# services/sensor_change_detector.py
import logging
from typing import Tuple, List, Optional
from models.measurement_data import MeasurementData

logger = logging.getLogger(__name__)

class SensorChangeDetector:
    """Сервис для определения смены сенсора и разделения пакета измерений."""
    
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher
        
    def partition_by_sensor_change(
        self, current_sensor: int, measurements: List[MeasurementData]
    ) -> Tuple[List[MeasurementData], List[MeasurementData]]:
        """
        Находит первую точку смены сенсора и разделяет пакет измерений.

        Args:
            current_sensor: ID сенсора из текущего контекста.
            measurements: Отсортированный по времени список новых измерений.

        Returns:
            Кортеж из двух списков:
            - (измерения_для_текущего_сенсора, измерения_после_смены_сенсора)
        """
        if not measurements:
            return [], []

        split_index = -1
        for i, measurement in enumerate(measurements):
            if measurement.sensor_id != current_sensor:
                split_index = i
                break
        
        if split_index == -1:
            # Смены сенсора не произошло, все измерения относятся к текущему сенсору
            return measurements, []
        
        new_sensor = measurements[split_index].sensor_id
        logger.info(
            f"Sensor change detected at index {split_index}: "
            f"{current_sensor} -> {new_sensor}"
        )

        pre_change_measurements = measurements[:split_index]
        post_change_measurements = measurements[split_index:]

        return pre_change_measurements, post_change_measurements