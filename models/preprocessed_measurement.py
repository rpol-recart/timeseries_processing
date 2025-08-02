# models/preprocessed_measurement.py
import json
class PreprocessedMeasurement:
    def __init__(self, processed_data):
        self.data = processed_data  # Может быть numpy array, dict, etc.

    def to_json(self):
        
        return json.dumps({"data": "serialized_processed_data"})
    
