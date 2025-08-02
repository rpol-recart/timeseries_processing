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

            new_sensor_id = measurements[0].sensor_id

            # Шаг 1: Собираем первые (или любые) измерения на НОВОМ сенсоре по одному на устройство
            new_measurements_by_device = {}
            for m in measurements:
                if m.sensor_id == new_sensor_id:
                    # Берём первое встреченное измерение для устройства
                    if m.device_id not in new_measurements_by_device:
                        new_measurements_by_device[m.device_id] = m

            if len(new_measurements_by_device) < 2:
                logger.warning(f"Not enough devices on new sensor {new_sensor_id}: {len(new_measurements_by_device)} < 2")
                return

            logger.info(f"Found {len(new_measurements_by_device)} devices on new sensor: {sorted(new_measurements_by_device.keys())}")

            # Шаг 2: Для каждого устройства из new_measurements_by_device — ищем последнее измерение на СТАРОМ сенсоре
            old_measurements_by_device = {}
            for device_id in new_measurements_by_device:
                old_m = fetcher.get_last_measurement_for_sensor_device_before(
                    sensor_id=current_sensor,
                    device_id=device_id,
                    timestamp=last_pred["prediction_time"]
                )
                if old_m is not None:
                    old_measurements_by_device[device_id] = old_m
                else:
                    logger.warning(f"No old measurement found for device {device_id} on sensor {current_sensor}")

            # Шаг 3: Определяем общие устройства (те, у которых есть данные на обоих сенсорах)
            common_devices = set(old_measurements_by_device.keys()) & set(new_measurements_by_device.keys())
            if len(common_devices) < 2:
                logger.warning(
                    f"Insufficient devices with data on both sensors. "
                    f"Only {len(common_devices)} devices have data on both old and new sensor. Need at least 2."
                )
                return

            logger.info(f"Valid device pairs found on both sensors: {sorted(common_devices)}")

            # Шаг 4: Формируем согласованные списки: по одному измерению на устройство
            # Важно: порядок должен быть одинаковым в обоих списках
            sorted_devices = sorted(common_devices)
            old_measurements = [old_measurements_by_device[dev] for dev in sorted_devices]
            new_measurements = [new_measurements_by_device[dev] for dev in sorted_devices]

            # Шаг 5: Выполняем детектирование сдвига
            try:
                param1, param2 = detect_measurement_shift(old_measurements, new_measurements)
                logger.info(f"New params after sensor change: param1={param1}, param2={param2}")
            except Exception as e:
                logger.error(f"Failed to detect measurement shift: {e}")
                return

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