# utils/validators.py
def is_valid_measurement(measurement_data) -> bool:
    data = measurement_data.raw_data
    if not (3 <= len(data) <= 10):
        return False
    for ts in data:
        if not (3000 <= len(ts.get("ts", [])) <= 8000):
            return False
        if len(ts.get("feat1", [])) != len(ts["ts"]) or len(ts.get("feat2", [])) != len(ts["ts"]):
            return False
    return True