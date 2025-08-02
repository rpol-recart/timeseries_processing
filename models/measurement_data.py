from datetime import datetime
from typing import List, Dict, Optional
import json

class MeasurementData:
    def __init__(
        self,
        sensor_id: int,
        device_id: int,
        measurement_time: datetime,
        measurement_count: int,
        time_series_data: Optional[List[Dict]] = None
    ):
        self.sensor_id = sensor_id
        self.device_id = device_id
        self.measurement_time = measurement_time
        self.measurement_count = measurement_count
        self.raw_data = time_series_data if time_series_data else []
        self.param1 = None
        self.param2 = None

    def add_time_series(self, json_str: str):
        """Добавляет временной ряд из JSON-строки CLOB"""
        try:
            time_series = json.loads(json_str)
            if not isinstance(time_series, dict):
                raise ValueError("JSON data must be a dictionary")
            if "ts" not in time_series:
                raise ValueError("Time series must contain 'ts' key")
            self.raw_data.append(time_series)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in CLOB data: {str(e)}")

    def add_params(self, param1: float, param2: float):
        self.param1 = param1
        self.param2 = param2

    def is_complete(self) -> bool:
        return len(self.raw_data) == self.measurement_count

    def __repr__(self):
        return (
            f"MeasurementData(sensor={self.sensor_id}, device={self.device_id}, "
            f"time={self.measurement_time.isoformat()}, "
            f"series={len(self.raw_data)}/{self.measurement_count})"
        )