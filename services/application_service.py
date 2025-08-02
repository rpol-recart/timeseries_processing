# services/application_service.py
import logging
from typing import Optional
from db.db import DB
from services.data_fetcher import DataFetcher
from services.measurement_processor import MeasurementProcessor
from services.sensor_calibration_service import SensorCalibrationService
from services.sensor_change_detector import SensorChangeDetector

logger = logging.getLogger(__name__)

class ApplicationService:
    """
    Основной сервис приложения, координирующий весь workflow обработки измерений.
    """
    
    def __init__(self):
        self.db = DB()
        self.data_fetcher = DataFetcher(self.db)
        self.sensor_change_detector = SensorChangeDetector(self.data_fetcher)
        self.calibration_service = SensorCalibrationService()
        self.measurement_processor = MeasurementProcessor(self.db)
        
    def process_measurements(self) -> None:
        """
        Основной метод обработки измерений:
        1. Получение контекста последнего предсказания
        2. Получение новых измерений
        3. Обработка смены сенсора (если нужно)
        4. Обработка измерений с предсказаниями
        """
        # Шаг 1: Получение контекста
        context = self._get_processing_context()
        if not context:
            logger.error("Cannot establish processing context. Exiting.")
            return
            
        # Шаг 2: Получение новых данных
        measurements = self._get_new_measurements()
        if not measurements:
            logger.info("No new measurements to process.")
            return
            
        # Шаг 3: Обработка смены сенсора
        calibration_params = self._handle_sensor_change(context, measurements)
        
        # Шаг 4: Обработка измерений
        self._process_measurements_with_predictions(measurements, calibration_params)
        
    def _get_processing_context(self) -> Optional[dict]:
        """Получение контекста для обработки (последнее предсказание)."""
        last_prediction = self.data_fetcher.get_last_prediction()
        if not last_prediction:
            return None
            
        logger.info(f"Processing context: last prediction at {last_prediction['prediction_time']} "
                   f"for sensor {last_prediction['sensor_id']}")
        return last_prediction
        
    def _get_new_measurements(self) -> list:
        """Получение новых измерений для обработки."""
        measurements = self.data_fetcher.get_new_measurements()
        logger.info(f"Found {len(measurements)} new measurements")
        return measurements
        
    def _handle_sensor_change(self, context: dict, measurements: list) -> tuple[float, float]:
        """
        Обработка смены сенсора и определение параметров калибровки.
        """
        current_sensor = context["sensor_id"]
        current_params = (context["param1"], context["param2"])
        
        # Проверка смены сенсора
        sensor_changed, new_sensor = self.sensor_change_detector.detect_change(
            current_sensor, measurements
        )
        
        if sensor_changed:
            logger.info(f"Sensor changed from {current_sensor} to {new_sensor}")
            return self.calibration_service.recalibrate_for_sensor_change(
                old_sensor=current_sensor,
                new_sensor=new_sensor,
                measurements=measurements,
                context=context
            )
        
        return current_params
        
    def _process_measurements_with_predictions(self, measurements: list, params: tuple[float, float]) -> None:
        """Обработка измерений с применением ML-предсказаний."""
        processed_count = self.measurement_processor.process_batch(measurements, params)
        logger.info(f"Successfully processed {processed_count} measurements")
        
    def cleanup(self) -> None:
        """Очистка ресурсов."""
        try:
            if hasattr(self, 'db'):
                self.db.close_pool()
                logger.info("Database resources cleaned up")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")