# services/application_service.py
import logging
from typing import Optional
from db.db import DB
from services.data_fetcher import DataFetcher
from services.measurement_processor import MeasurementProcessor
from services.sensor_calibration_service import SensorCalibrationService
from services.sensor_change_detector import SensorChangeDetector
from models.measurement_data import MeasurementData

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
        1. Получение контекста последнего предсказания.
        2. Получение новых измерений.
        3. Разделение пакета измерений по точке смены сенсора.
        4. Последовательная обработка каждой части пакета с соответствующими параметрами.
        """
        # Шаг 1: Получение контекста
        context = self._get_processing_context()
        if not context:
            logger.error("Cannot establish processing context. Exiting.")
            return
            
        # Шаг 2: Получение новых данных
        all_new_measurements = self._get_new_measurements()
        if not all_new_measurements:
            logger.info("No new measurements to process.")
            return

        current_sensor_id = context["sensor_id"]
        current_params = (context["param1"], context["param2"])

        # Шаг 3: Разделение пакета по смене сенсора
        pre_change_batch, post_change_batch = self.sensor_change_detector.partition_by_sensor_change(
            current_sensor_id, all_new_measurements
        )

        # Шаг 4.1: Обработка измерений со старого сенсора (если они есть)
        if pre_change_batch:
            logger.info(f"Processing {len(pre_change_batch)} measurements for sensor {current_sensor_id} with existing params.")
            self._process_measurements_with_predictions(pre_change_batch, current_params)

        # Шаг 4.2: Обработка измерений с нового сенсора (если они есть)
        if post_change_batch:
            new_sensor_id = post_change_batch[0].sensor_id
            logger.info(f"Recalibrating for sensor change from {current_sensor_id} to {new_sensor_id}.")
            
            try:
                # Выполняем калибровку на данных нового сенсора
                new_params = self.calibration_service.recalibrate_for_sensor_change(
                    old_sensor=current_sensor_id,
                    new_sensor=new_sensor_id,
                    measurements=post_change_batch, # Калибруемся на данных после смены
                    context=context
                )
                logger.info(f"Processing {len(post_change_batch)} measurements for new sensor {new_sensor_id} with new params.")
                self._process_measurements_with_predictions(post_change_batch, new_params)
            except Exception as e:
                logger.error(f"Failed to recalibrate and process for new sensor {new_sensor_id}. "
                             f"Measurements will be skipped. Error: {e}")

    def _get_processing_context(self) -> Optional[dict]:
        """Получение контекста для обработки (последнее предсказание)."""
        last_prediction = self.data_fetcher.get_last_prediction()
        if not last_prediction:
            return None
            
        logger.info(f"Processing context: last prediction at {last_prediction['prediction_time']} "
                   f"for sensor {last_prediction['sensor_id']}")
        return last_prediction
        
    def _get_new_measurements(self) -> list[MeasurementData]:
        """Получение новых измерений для обработки."""
        measurements = self.data_fetcher.get_new_measurements()
        logger.info(f"Found {len(measurements)} new measurements")
        return measurements
        
    def _process_measurements_with_predictions(self, measurements: list[MeasurementData], params: tuple[float, float]) -> None:
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