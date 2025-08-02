# config.py
import os

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