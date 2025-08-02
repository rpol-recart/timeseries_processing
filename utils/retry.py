# utils/retry.py
import time
import logging
from functools import wraps
from config import (
    DB_RETRY_ATTEMPTS,
    DB_RETRY_DELAY,
    DB_RETRY_BACKOFF,
    DB_RETRY_EXCEPTIONS,
)

logger = logging.getLogger(__name__)

def retry_db_operation(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        last_exception = None
        delay = DB_RETRY_DELAY
        
        for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except DB_RETRY_EXCEPTIONS as e:
                last_exception = e
                logger.warning(
                    f"DB operation failed (attempt {attempt}/{DB_RETRY_ATTEMPTS}): {str(e)}"
                )
                if attempt < DB_RETRY_ATTEMPTS:
                    time.sleep(delay)
                    delay *= DB_RETRY_BACKOFF  # Экспоненциальная задержка
            except Exception as e:
                logger.error(f"Non-retryable error in DB operation: {str(e)}")
                raise

        logger.error(f"All retry attempts failed for DB operation")
        raise last_exception if last_exception else Exception("Unknown DB error")

    return wrapper