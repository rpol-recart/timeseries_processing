# db/db.py
import cx_Oracle
from config import DB_USER, DB_PASSWORD, DB_DSN, POOL_MIN, POOL_MAX, POOL_INCREMENT

class DB:
    def __init__(self):
        self.pool = cx_Oracle.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN,
            min=POOL_MIN,
            max=POOL_MAX,
            increment=POOL_INCREMENT,
            threaded=True
        )

    def get_connection(self):
        return self.pool.acquire()

    def close_pool(self):
        self.pool.close()
        self.pool.wait()

    def fetch_last_prediction(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
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
            cursor.close()
            self.pool.release(conn)

    def fetch_new_measurements(self, last_prediction_time):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
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
            cursor.close()
            self.pool.release(conn)

    def insert_prediction(self, sensor_id, device_id, param1, param2, result):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO table1 (prediction_time, sensor_id, device_id, param1, param2, result)
                VALUES (SYSTIMESTAMP, :sensor_id, :device_id, :param1, :param2, :result)
            """, sensor_id=sensor_id, device_id=device_id, param1=param1, param2=param2, result=result)
            conn.commit()
        finally:
            cursor.close()
            self.pool.release(conn)