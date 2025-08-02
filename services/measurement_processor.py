# services/measurement_processor.py
import logging
from typing import List, Tuple
from models.measurement_data import MeasurementData
from services.preprocessing import preprocess
from services.prediction import predict

logger = logging.getLogger(__name__)

class MeasurementProcessor:
    """Сервис для обработки измерений и выполнения предсказаний."""
    
    def __init__(self, db):
        self.db = db
        
    def process_batch(self, measurements: List[MeasurementData], params: Tuple[float, float]) -> int:
        """
        Обрабатывает батч измерений с применением ML-предсказаний.
        
        Args:
            measurements: Список измерений для обработки
            params: Параметры калибровки (param1, param2)
            
        Returns:
            int: Количество успешно обработанных измерений
        """
        param1, param2 = params
        processed_count = 0
        
        for measurement in measurements:
            try:
                self._process_single_measurement(measurement, param1, param2)
                processed_count += 1
            except Exception as e:
                logger.error(f"Failed to process measurement {measurement}: {e}")
                
        return processed_count
        
    def _process_single_measurement(self, measurement: MeasurementData, param1: float, param2: float) -> None:
        """Обрабатывает одно измерение."""
        # Добавление параметров калибровки
        measurement.add_params(param1, param2)
        
        # Препроцессинг
        preprocessed = preprocess(measurement)
        
        # Предсказание
        result = predict(preprocessed)
        
        # Сохранение результата
        self.db.insert_prediction(
            sensor_id=measurement.sensor_id,
            device_id=measurement.device_id,
            param1=param1,
            param2=param2,
            result=result
        )
        
        logger.debug(f"Processed measurement: sensor={measurement.sensor_id}, "
                    f"device={measurement.device_id}, result={result}")