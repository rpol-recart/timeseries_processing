# db/db.py
import oracledb
from config import DB_CONFIG, PROCESSING_CONFIG
from utils.retry import retry_db_operation
import logging

logger = logging.getLogger(__name__)


class DB:
    def __init__(self):
        self.pool = None
        self._init_pool()

    @retry_db_operation
    def _init_pool(self):
        """Initialize Oracle connection pool using oracledb."""
        self.pool = oracledb.create_pool(
            user=DB_CONFIG.user,
            password=DB_CONFIG.password,
            dsn=DB_CONFIG.dsn,
            min=DB_CONFIG.pool_min,
            max=DB_CONFIG.pool_max,
            increment=DB_CONFIG.pool_increment,
            getmode=oracledb.PoolGetMode.WAIT  # Wait if pool is busy
        )
        logger.info(
            f"Database connection pool initialized: "
            f"min={DB_CONFIG.pool_min}, max={DB_CONFIG.pool_max}, increment={DB_CONFIG.pool_increment}"
        )

    @retry_db_operation
    def get_connection(self):
        """Acquire a connection from the pool."""
        if not self.pool:
            raise RuntimeError("Connection pool is not initialized.")
        return self.pool.acquire()

    @retry_db_operation
    def close_pool(self):
        """Close the entire connection pool."""
        if self.pool:
            self.pool.close()
            logger.info("Database connection pool closed.")

    @retry_db_operation
    def fetch_last_prediction(self):
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    prediction_time, sensor_id, device_id, param1, param2, result
                FROM table1
                ORDER BY prediction_time DESC
                FETCH FIRST 1 ROW ONLY
            """)
            row = cursor.fetchone()
            if row:
                return {
                    "prediction_time": row[0],
                    "sensor_id": row[1],
                    "device_id": row[2],
                    "param1": row[3],
                    "param2": row[4],
                    "result": row[5]
                }
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()  # Return to pool

    @retry_db_operation
    def fetch_unprocessed_measurements_last24h(self):
        """Возвращает все необработанные временные ряды с указанием количества рядов в измерении"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
    
            # Получаем временной диапазон для выборки
            cursor.execute("SELECT MAX(prediction_time) FROM table1")
            max_pred_time_row = cursor.fetchone()
            if not max_pred_time_row or not max_pred_time_row[0]:
                logger.info("No predictions found in table1")
                return []
    
            max_pred_time = max_pred_time_row[0]
            min_time = max_pred_time - PROCESSING_CONFIG.late_data_tolerance
    
            # Запрос всех временных рядов с флагом обработки
            cursor.execute("""
                SELECT 
                    t2.sensor_id,
                    t2.device_id,
                    t2.measurement_time,
                    t2.data
                FROM table2 t2
                WHERE t2.measurement_time BETWEEN :min_time AND :max_time
                AND NOT EXISTS (
                    SELECT 1 FROM table1 t1
                    WHERE t1.sensor_id = t2.sensor_id
                    AND t1.device_id = t2.device_id
                    AND ABS(EXTRACT(EPOCH FROM (t1.prediction_time - t2.measurement_time))) < 1
                )
                ORDER BY t2.measurement_time, t2.sensor_id, t2.device_id
            """, min_time=min_time, max_time=max_pred_time)
    
            return [
                {
                    "sensor_id": row[0],
                    "device_id": row[1],
                    "measurement_time": row[2],
                    "data": row[3].read() if hasattr(row[3], 'read') else row[3]  # Чтение CLOB
                }
                for row in cursor
            ]
    
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @retry_db_operation
    def fetch_new_measurements(self, last_prediction_time):
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    sensor_id, device_id, measurement_time, data
                FROM table2
                WHERE measurement_time > :last_time
                ORDER BY measurement_time
            """, last_time=last_prediction_time)
            rows = cursor.fetchall()
            return [
                {
                    "sensor_id": r[0],
                    "device_id": r[1],
                    "measurement_time": r[2],
                    "data": r[3]
                }
                for r in rows
            ]
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()  # Return to pool

    @retry_db_operation
    def fetch_last_measurement_for_sensor_device_before_time(self, sensor_id: int, device_id: int, timestamp):
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT sensor_id, device_id, measurement_time, data
                FROM table2
                WHERE sensor_id = :sensor_id
                  AND device_id = :device_id
                  AND measurement_time < :timestamp
                ORDER BY measurement_time DESC
                FETCH FIRST 1 ROW ONLY
            """, sensor_id=sensor_id, device_id=device_id, timestamp=timestamp)

            row = cursor.fetchone()
            if row:
                return {
                    "sensor_id": row[0],
                    "device_id": row[1],
                    "measurement_time": row[2],
                    "data": row[3]
                }
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()  # Return to pool

    @retry_db_operation
    def insert_prediction(self, sensor_id, device_id, param1, param2, result):
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO table1 (prediction_time, sensor_id, device_id, param1, param2, result)
                VALUES (SYSTIMESTAMP, :sensor_id, :device_id, :param1, :param2, :result)
            """, sensor_id=sensor_id, device_id=device_id, param1=param1, param2=param2, result=result)
            conn.commit()
            logger.debug(f"Inserted prediction for sensor {sensor_id}, device {device_id}")
        except Exception as e:
            logger.error(f"Failed to insert prediction: {str(e)}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()  # Return to pool