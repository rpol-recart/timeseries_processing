# main.py
from db.db import DB
from utils.retry import retry_db_operation
from services.data_fetcher import DataFetcher
from services.preprocessing import preprocess
from services.prediction import predict
from services.sensor_shift_detector import detect_measurement_shift
from models.measurement_data import MeasurementData
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry_db_operation
def main():
    try:    
        db = DB()
        fetcher = DataFetcher(db)

        # Шаг 1: Получить последнее предсказание
        last_pred = fetcher.get_last_prediction()
        if not last_pred:
            logger.error("No previous prediction found. Exiting.")
            return

        logger.info(f"Last prediction: {last_pred['prediction_time']} for sensor {last_pred['sensor_id']}")

        # Шаг 2: Получить новые измерения
        measurements = fetcher.get_new_measurements()
        if not measurements:
            logger.info("No new measurements. Exiting.")
            return

        current_sensor = last_pred["sensor_id"]
        new_sensor_detected = measurements[0].sensor_id != current_sensor

        param1, param2 = last_pred["param1"], last_pred["param2"]

        if new_sensor_detected:
            logger.info(f"Sensor changed from {current_sensor} to {measurements[0].sensor_id}")

            # Проверяем, есть ли минимум по одному измерению от 2 разных устройств
            devices = {m.device_id for m in measurements if m.sensor_id == measurements[0].sensor_id}
            if len(devices) < 2:
                logger.warning("Not enough devices after sensor change. Exiting without prediction.")
                return

            # Получаем последние измерения со старым сенсором (по одному на устройство)
            old_measurements = []
            for device_id in devices:
                # Здесь нужно запросить последнее измерение по старому сенсору и устройству
                # Заглушка:
                old_m = MeasurementData(
                    sensor_id=current_sensor,
                    device_id=device_id,
                    measurement_time=last_pred["prediction_time"],
                    raw_data=[{"ts": list(range(5000)), "feat1": [0.1]*5000, "feat2": [0.2]*5000}]
                )
                old_measurements.append(old_m)

            # Определяем новые param1, param2
            param1, param2 = detect_measurement_shift(old_measurements)
            logger.info(f"New params after sensor change: param1={param1}, param2={param2}")

        # Обработка измерений
        for measurement in measurements:
            if not new_sensor_detected:
                measurement.add_params(param1, param2)
            else:
                if measurement.sensor_id == measurements[0].sensor_id:  # Только новые сенсоры
                    measurement.add_params(param1, param2)
                else:
                    continue  # Пропускаем старые

            # Препроцессинг
            preprocessed = preprocess(measurement)

            # Предсказание
            try:
                result = predict(preprocessed)
                logger.info(f"Prediction result: {result} for sensor {measurement.sensor_id}")

                # Запись в БД
                db.insert_prediction(
                    sensor_id=measurement.sensor_id,
                    device_id=measurement.device_id,
                    param1=measurement.param1,
                    param2=measurement.param2,
                    result=result
                )
            except Exception as e:
                logger.error(f"Prediction failed: {e}")
                
    except Exception as e:
        logger.error(f"Application failed after retries: {str(e)}")
        raise
    finally:
        if 'db' in locals():
            try:
                db.close_pool()
            except Exception as e:
                logger.error(f"Error closing DB pool: {str(e)}")
        

if __name__ == "__main__":
    main()