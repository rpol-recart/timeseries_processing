# config.py
import os
from datetime import timedelta
from dataclasses import dataclass
from typing import Tuple, Type
import oracledb

@dataclass
class DatabaseConfig:
    """Конфигурация базы данных."""
    user: str = os.getenv("DB_USER", "user")
    password: str = os.getenv("DB_PASSWORD", "pass") 
    dsn: str = os.getenv("DB_DSN", "localhost:1521/XE")
    pool_min: int = int(os.getenv("DB_POOL_MIN", "1"))
    pool_max: int = int(os.getenv("DB_POOL_MAX", "5"))
    pool_increment: int = int(os.getenv("DB_POOL_INCREMENT", "1"))

@dataclass 
class RetryConfig:
    """Конфигурация повторных попыток."""
    attempts: int = int(os.getenv("DB_RETRY_ATTEMPTS", "3"))
    delay: float = float(os.getenv("DB_RETRY_DELAY", "1.0"))
    backoff: float = float(os.getenv("DB_RETRY_BACKOFF", "2.0"))
    exceptions: Tuple[Type[Exception], ...] = (
        oracledb.DatabaseError,
        oracledb.OperationalError,
        TimeoutError,
    )

@dataclass
class ProcessingConfig:
    """Конфигурация обработки данных."""
    prediction_timeout: int = int(os.getenv("PREDICTION_TIMEOUT", "3"))
    late_data_tolerance: timedelta = timedelta(hours=int(os.getenv("LATE_DATA_TOLERANCE_HOURS", "24")))
    min_calibration_devices: int = int(os.getenv("MIN_CALIBRATION_DEVICES", "2"))
    
@dataclass
class DegradationConfig:
    "Конфигурация модели деградации"
    degradation_type:str = os.getenv("DEGRADATION_TYPE", "linear") # avaible values [linear,quadratic]
    linear_slope_e:float=float(os.getenv("LINEAR_DEGRADATION_SLOPE_E", "0.5"))
    linear_intercept_e:float=float(os.getenv("LINEAR_DEGRADATION_INTERCEPT_E", "0.5"))
    linear_slope_i:float=float(os.getenv("LINEAR_DEGRADATION_SLOPE_I", "0.5"))
    linear_intercept_i:float=float(os.getenv("LINEAR_DEGRADATION_INTERCEPT_I", "0.5"))

# Глобальные экземпляры конфигураций
DB_CONFIG = DatabaseConfig()
RETRY_CONFIG = RetryConfig()
PROCESSING_CONFIG = ProcessingConfig()
DEGRADATION_CONFIG = DegradationConfig()

# Обратная совместимость
DB_USER = DB_CONFIG.user
DB_PASSWORD = DB_CONFIG.password  
DB_DSN = DB_CONFIG.dsn
POOL_MIN = DB_CONFIG.pool_min
POOL_MAX = DB_CONFIG.pool_max
POOL_INCREMENT = DB_CONFIG.pool_increment
PREDICTION_TIMEOUT = PROCESSING_CONFIG.prediction_timeout
DB_RETRY_ATTEMPTS = RETRY_CONFIG.attempts
DB_RETRY_DELAY = RETRY_CONFIG.delay
DB_RETRY_BACKOFF = RETRY_CONFIG.backoff
DB_RETRY_EXCEPTIONS = RETRY_CONFIG.exceptions
LATE_DATA_TOLERANCE = PROCESSING_CONFIG.late_data_tolerance