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

    def get_new_measurements(self, last_time):
        rows = self.db.fetch_new_measurements(last_time)
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