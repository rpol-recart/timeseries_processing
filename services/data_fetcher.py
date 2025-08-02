# services/data_fetcher.py
from db.db import DB
from models.measurement_data import MeasurementData
import json
from utils.validators import is_valid_measurement
from collections import defaultdict
import logging
from typing import List

logger = logging.getLogger(__name__)

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
    
    def get_new_measurements(self) -> List[MeasurementData]:
        """Получает новые измерения, группируя временные ряды по measurement_time, sensor_id, device_id"""
        rows = self.db.fetch_unprocessed_measurements_last24h()
        
        # Группировка по уникальным измерениям
        measurements_map = defaultdict(list)
        for row in rows:
            key = (row["measurement_time"], row["sensor_id"], row["device_id"])
            measurements_map[key].append(row["data"])  # data - это CLOB с JSON

        measurements = []
        for (measurement_time, sensor_id, device_id), clob_data_list in measurements_map.items():
            measurement = MeasurementData(
                sensor_id=sensor_id,
                device_id=device_id,
                measurement_time=measurement_time,
                measurement_count=len(clob_data_list)
            )

            for clob_data in clob_data_list:
                try:
                    measurement.add_time_series(clob_data)
                except ValueError as e:
                    logger.warning(
                        f"Invalid time series data for sensor {sensor_id}, "
                        f"device {device_id} at {measurement_time}: {str(e)}"
                    )
                    continue

            if measurement.is_complete() and is_valid_measurement(measurement):
                measurements.append(measurement)
            else:
                logger.warning(
                    f"Incomplete or invalid measurement: sensor {sensor_id}, "
                    f"device {device_id} at {measurement_time} "
                    f"(has {len(measurement.raw_data)} of {measurement.measurement_count} series)"
                )

        return sorted(measurements, key=lambda x: x.measurement_time)