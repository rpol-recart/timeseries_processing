# services/sensor_change_detector.py
import logging
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

class SensorChangeDetector:
    """Сервис для определения смены сенсора."""
    
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher
        
    def detect_change(self, current_sensor: int, measurements: list) -> Tuple[bool, Optional[int]]:
        """
        Определяет, произошла ли смена сенсора.
        
        Returns:
            (bool, int): (смена_произошла, новый_сенсор_id)
        """
        if not measurements:
            return False, None
            
        new_sensor = measurements[0].sensor_id
        sensor_changed = new_sensor != current_sensor
        
        if sensor_changed:
            logger.info(f"Detected sensor change: {current_sensor} -> {new_sensor}")
            
        return sensor_changed, new_sensor if sensor_changed else None