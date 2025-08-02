# utils/retry.py
import time
import logging
from functools import wraps
from config import RETRY_CONFIG

logger = logging.getLogger(__name__)

def retry_db_operation(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        last_exception = None
        delay = RETRY_CONFIG.delay
        
        for attempt in range(1, RETRY_CONFIG.attempts + 1):
            try:
                return func(*args, **kwargs)
            except RETRY_CONFIG.exceptions as e:
                last_exception = e
                logger.warning(
                    f"DB operation failed (attempt {attempt}/{RETRY_CONFIG.attempts}): {str(e)}"
                )
                if attempt < RETRY_CONFIG.attempts:
                    time.sleep(delay)
                    delay *= RETRY_CONFIG.backoff  # Экспоненциальная задержка
            except Exception as e:
                logger.error(f"Non-retryable error in DB operation: {str(e)}")
                raise

        logger.error(f"All retry attempts failed for DB operation")
        raise last_exception if last_exception else Exception("Unknown DB error")

    return wrapper