# main.py
import logging
from services.application_service import ApplicationService
from utils.retry import retry_db_operation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@retry_db_operation
def main():
    """
    Главная точка входа приложения.
    Координирует высокоуровневый workflow обработки измерений.
    """
    app_service = None
    try:
        app_service = ApplicationService()
        app_service.process_measurements()
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise
    finally:
        if app_service:
            app_service.cleanup()

if __name__ == "__main__":
    main()