# models/measurement_data.py
from datetime import datetime
from typing import List, Dict

class MeasurementData:
    def __init__(self, sensor_id: int, device_id: int, measurement_time: datetime, raw_data: List[Dict]):
        self.sensor_id = sensor_id
        self.device_id = device_id
        self.measurement_time = measurement_time
        self.raw_data = raw_data  # Список временных рядов: [{"ts": [...], "feat1": [...], "feat2": [...]}]
        self.param1 = None
        self.param2 = None

    def add_params(self, param1, param2):
        self.param1 = param1
        self.param2 = param2

    def __repr__(self):
        return f"MeasurementData(sensor={self.sensor_id}, device={self.device_id}, time={self.measurement_time})"