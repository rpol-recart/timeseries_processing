# services/data_fetcher.py
from db.db import DB
from models.measurement_data import MeasurementData
import json
from utils.validators import is_valid_measurement

class DataFetcher:
    def __init__(self, db: DB):
        self.db = db

    def get_last_prediction(self):
        return self.db.fetch_last_prediction()

    def get_last_measurement_for_sensor_device_before(self, sensor_id: int, device_id: int, timestamp) -> MeasurementData:
        """
        Получает последнее измерение по старому сенсору и устройству до указанного времени.
        Возвращает экземпляр MeasurementData или None.
        """
        row = self.db.fetch_last_measurement_for_sensor_device_before_time(
            sensor_id=sensor_id,
            device_id=device_id,
            timestamp=timestamp
        )
        if not row:
            return None

        try:
            raw_data = json.loads(row["data"].read() if hasattr(row["data"], 'read') else row["data"])
        except (json.JSONDecodeError, TypeError):
            return None

        measurement = MeasurementData(
            sensor_id=row["sensor_id"],
            device_id=row["device_id"],
            measurement_time=row["measurement_time"],
            raw_data=raw_data
        )

        return measurement if is_valid_measurement(measurement) else None
    
    def get_new_measurements(self):
        rows = self.db.fetch_unprocessed_measurements_last24h()
        measurements = []
        for row in rows:
            try:
                raw_data = json.loads(row["data"])  # Предполагаем JSON в поле data
            except:
                continue  # Пропускаем невалидные
            m = MeasurementData(
                sensor_id=row["sensor_id"],
                device_id=row["device_id"],
                measurement_time=row["measurement_time"],
                raw_data=raw_data
            )
            if is_valid_measurement(m):
                measurements.append(m)
        return sorted(measurements, key=lambda x: x.measurement_time)