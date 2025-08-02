# config.py
import os
from datetime import timedelta
import cx_Oracle

# Oracle DB
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pass")
DB_DSN = os.getenv("DB_DSN", "localhost:1521/XE")

# Пул соединений
POOL_MIN = 1
POOL_MAX = 5
POOL_INCREMENT = 1

# Таймауты
PREDICTION_TIMEOUT = 3  # секунды

# Retry настройки
DB_RETRY_ATTEMPTS = 3  # Количество попыток
DB_RETRY_DELAY = 1  # Задержка между попытками в секундах
DB_RETRY_BACKOFF = 2  # Множитель для экспоненциальной задержки
DB_RETRY_EXCEPTIONS = (
    cx_Oracle.DatabaseError,
    cx_Oracle.OperationalError,
    TimeoutError,
)  # Типы исключений для retry


LATE_DATA_TOLERANCE = timedelta(hours=24) # определяет насколько может запоздать поступление данных замера относительно фактического времени