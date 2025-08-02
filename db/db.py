# db/db.py
import cx_Oracle
from config import DB_USER, DB_PASSWORD, DB_DSN, POOL_MIN, POOL_MAX, POOL_INCREMENT,LATE_DATA_TOLERANCE
from utils.retry import retry_db_operation
import logging

logger = logging.getLogger(__name__)

class DB:
    def __init__(self):
        self._init_pool()

    @retry_db_operation
    def _init_pool(self):
        self.pool = cx_Oracle.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN,
            min=POOL_MIN,
            max=POOL_MAX,
            increment=POOL_INCREMENT,
            threaded=True
        )
        logger.info("Database connection pool initialized")

    @retry_db_operation
    def get_connection(self):
        return self.pool.acquire()

    @retry_db_operation
    def close_pool(self):
        if hasattr(self, 'pool'):
            self.pool.close()
            self.pool.wait()
            logger.info("Database connection pool closed")

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
                self.pool.release(conn)

    @retry_db_operation
    def fetch_unprocessed_measurements_last24h(self):
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Получаем MAX(prediction_time) из table1
            cursor.execute("""
                SELECT MAX(prediction_time) FROM table1
            """)
            max_pred_time_row = cursor.fetchone()
            if not max_pred_time_row or not max_pred_time_row[0]:
                logger.info("No prediction_time found in table1.")
                return []

            max_pred_time = max_pred_time_row[0]

            # Вычисляем min_time = max_pred_time - LATE_DATA_TOLERANCE
            from config import LATE_DATA_TOLERANCE
            min_time = max_pred_time - LATE_DATA_TOLERANCE

            # Выполняем основной запрос с параметрами
            cursor.execute("""
                SELECT t2.sensor_id, t2.device_id, t2.measurement_time, t2.data
                FROM table2 t2
                WHERE t2.measurement_time BETWEEN :min_time AND :max_time
                AND NOT EXISTS (
                    SELECT 1 
                    FROM table1 t1 
                    WHERE ABS(EXTRACT(EPOCH FROM (t1.prediction_time - t2.measurement_time))) < 1
                    AND t1.sensor_id = t2.sensor_id
                    AND t1.device_id = t2.device_id
                )
                ORDER BY t2.measurement_time
            """, min_time=min_time, max_time=max_pred_time)

            return [
                dict(zip(['sensor_id', 'device_id', 'measurement_time', 'data'], row))
                for row in cursor.fetchall()
            ]

        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release(conn)
                
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
                    "data": r[3]  # Предполагаем, что data — это CLOB или BLOB с JSON
                }
                for r in rows
            ]
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.pool.release(conn)

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
                self.pool.release(conn)