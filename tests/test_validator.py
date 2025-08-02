# tests/test_validators.py
import pytest
from models.measurement_data import MeasurementData
from utils.validators import is_valid_measurement

# tests/test_validator.py
def test_valid_measurement():
    """Test valid measurement."""
    raw_data = [
        {"ts": list(range(3000)), "feat1": list(range(3000)), "feat2": list(range(3000))},
        {"ts": list(range(4000)), "feat1": list(range(4000)), "feat2": list(range(4000))},
        {"ts": list(range(3500)), "feat1": list(range(3500)), "feat2": list(range(3500))}  # Added third series
    ]
    measurement = MeasurementData(1, 1, None, raw_data)
    
    assert is_valid_measurement(measurement)

def test_invalid_measurement_length():
    """Тест невалидного измерения (неправильная длина)."""
    raw_data = [{"ts": [1,2,3], "feat1": [1,2], "feat2": [1,2,3]}]  # Несоответствие длин
    measurement = MeasurementData(1, 1, None, raw_data)
    
    assert not is_valid_measurement(measurement)

def test_invalid_measurement_count():
    """Тест невалидного измерения (неправильное количество временных рядов)."""
    raw_data = [{"ts": [1,2,3]}] * 2  # Слишком мало рядов
    measurement = MeasurementData(1, 1, None, raw_data)
    
    assert not is_valid_measurement(measurement)