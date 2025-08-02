# tests/test_db.py
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timedelta
from oracledb import  Connection, Cursor
from db.db import DB
from config import PROCESSING_CONFIG


# Sample data for testing
FAKE_PREDICTION_TIME = datetime(2023, 10, 1, 12, 0, 0)
FAKE_MEASUREMENT_TIME = FAKE_PREDICTION_TIME - timedelta(minutes=5)


@pytest.fixture
def mock_oracledb():
    """Mock oracledb module and config values."""
    with patch("db.db.oracledb") as mock_oracledb, \
         patch("db.db.DB_CONFIG") as mock_db_config:

        # Настройка mock_db_config
        mock_db_config.user = "mock_user"
        mock_db_config.password = "mock_pass"
        mock_db_config.dsn = "mock_dsn"
        mock_db_config.pool_min = 1
        mock_db_config.pool_max = 10
        mock_db_config.pool_increment = 1

        mock_pool = MagicMock()
        mock_conn = MagicMock(spec=Connection)
        mock_cursor = MagicMock(spec=Cursor)

        # Mock PoolGetMode.WAIT
        mock_wait_mode = MagicMock()
        type(mock_oracledb.PoolGetMode).WAIT = mock_wait_mode

        mock_oracledb.create_pool.return_value = mock_pool
        mock_pool.acquire.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Inject getmode for assertions
        mock_oracledb.PoolGetMode.WAIT = mock_wait_mode

        yield mock_oracledb


@pytest.fixture
def db_instance(mock_oracledb):
    """Create a DB instance with mocked oracledb."""
    db = DB()
    return db


def test_init_pool_success(db_instance, mock_oracledb):
    with patch("db.db.DB_CONFIG") as mock_db_config:
        mock_db_config.user = "mock_user"
        mock_db_config.password = "mock_pass"
        mock_db_config.dsn = "mock_dsn"
        mock_db_config.pool_min = 1
        mock_db_config.pool_max = 10
        mock_db_config.pool_increment = 1
        
        db_instance._init_pool()
        
        mock_oracledb.create_pool.assert_called_with(
            user="mock_user",
            password="mock_pass",
            dsn="mock_dsn",
            min=1,
            max=10,
            increment=1,
            getmode=mock_oracledb.PoolGetMode.WAIT,
        )


def test_get_connection_calls_acquire(db_instance):
    """Test that get_connection acquires from pool."""
    conn = db_instance.get_connection()
    assert conn is db_instance.pool.acquire.return_value


def test_get_connection_raises_if_no_pool(mock_oracledb):
    """Test that get_connection raises RuntimeError if pool not initialized."""
    db = DB()

    # Now simulate pool being None (e.g., failed init or closed)
    db.pool = None

    with pytest.raises(RuntimeError, match="Connection pool is not initialized."):
        db.get_connection()


def test_close_pool_calls_close(db_instance):
    """Test that close_pool closes the pool."""
    db_instance.close_pool()
    db_instance.pool.close.assert_called_once()


def test_fetch_last_prediction_success(db_instance):
    """Test successful fetch of last prediction."""
    cursor = db_instance.pool.acquire.return_value.cursor.return_value
    cursor.fetchone.return_value = (
        FAKE_PREDICTION_TIME,
        101,
        202,
        1.5,
        2.5,
        "OK",
    )

    result = db_instance.fetch_last_prediction()

    expected = {
        "prediction_time": FAKE_PREDICTION_TIME,
        "sensor_id": 101,
        "device_id": 202,
        "param1": 1.5,
        "param2": 2.5,
        "result": "OK",
    }
    assert result == expected

    cursor.execute.assert_called_once_with(
        """
                SELECT 
                    prediction_time, sensor_id, device_id, param1, param2, result
                FROM table1
                ORDER BY prediction_time DESC
                FETCH FIRST 1 ROW ONLY
            """
    )


def test_fetch_last_prediction_returns_none_if_no_data(db_instance):
    """Test that fetch_last_prediction returns None when no rows."""
    cursor = db_instance.pool.acquire.return_value.cursor.return_value
    cursor.fetchone.return_value = None

    result = db_instance.fetch_last_prediction()
    assert result is None


def test_fetch_unprocessed_measurements_last24h_no_max_time(db_instance, caplog):
    """Test that fetch_unprocessed_measurements returns [] if no max prediction_time."""
    cursor = db_instance.pool.acquire.return_value.cursor.return_value
    cursor.fetchone.return_value = (None,)  # No MAX(prediction_time)

    with caplog.at_level("INFO"):
        result = db_instance.fetch_unprocessed_measurements_last24h()

    assert result == []
    assert "No prediction_time found in table1." in caplog.text


def test_fetch_unprocessed_measurements_last24h_with_data(db_instance, caplog):
    """Test fetching unprocessed measurements with valid max time."""
    conn = db_instance.pool.acquire.return_value
    cursor = conn.cursor.return_value

    # Mock MAX(prediction_time)
    cursor.fetchone.return_value = (FAKE_PREDICTION_TIME,)

    # Mock measurement results
    raw_measurements = [
        (101, 202, FAKE_MEASUREMENT_TIME, {"val": 42}),
    ]
    cursor.fetchall.return_value = raw_measurements

    with caplog.at_level("INFO"):
        result = db_instance.fetch_unprocessed_measurements_last24h()

    assert len(result) == 1
    assert result[0]["sensor_id"] == 101
    assert result[0]["data"] == {"val": 42}

    # Check correct query and parameters
    min_time = FAKE_PREDICTION_TIME - PROCESSING_CONFIG.late_data_tolerance
    cursor.execute.assert_any_call("SELECT MAX(prediction_time) FROM table1")
    cursor.execute.assert_any_call(
        """
                SELECT t2.sensor_id, t2.device_id, t2.measurement_time, t2.data
                FROM table2 t2
                WHERE t2.measurement_time BETWEEN :min_time AND :max_time
                AND NOT EXISTS (
                    SELECT 1 
                    FROM table1 t1 
                    WHERE ABS(EXTRACT(EPOCH FROM (t1.prediction_time - t2.measurement_time))) < 1
                )
                ORDER BY t2.measurement_time
            """,
        min_time=min_time,
        max_time=FAKE_PREDICTION_TIME,
    )


def test_fetch_new_measurements(db_instance):
    """Test fetching new measurements after last prediction time."""
    cursor = db_instance.pool.acquire.return_value.cursor.return_value
    cursor.fetchall.return_value = [
        (101, 202, FAKE_MEASUREMENT_TIME, {"val": 100}),
    ]

    result = db_instance.fetch_new_measurements(FAKE_PREDICTION_TIME)

    assert len(result) == 1
    assert result[0]["measurement_time"] == FAKE_MEASUREMENT_TIME

    cursor.execute.assert_called_once_with(
        """
                SELECT 
                    sensor_id, device_id, measurement_time, data
                FROM table2
                WHERE measurement_time > :last_time
                ORDER BY measurement_time
            """,
        last_time=FAKE_PREDICTION_TIME,
    )


def test_fetch_last_measurement_before_time_found(db_instance):
    """Test fetching last measurement before a timestamp."""
    cursor = db_instance.pool.acquire.return_value.cursor.return_value
    cursor.fetchone.return_value = (101, 202, FAKE_MEASUREMENT_TIME, {"val": 50})

    result = db_instance.fetch_last_measurement_for_sensor_device_before_time(
        sensor_id=101, device_id=202, timestamp=FAKE_PREDICTION_TIME
    )

    expected = {
        "sensor_id": 101,
        "device_id": 202,
        "measurement_time": FAKE_MEASUREMENT_TIME,
        "data": {"val": 50},
    }
    assert result == expected

    cursor.execute.assert_called_once_with(
        """
                SELECT sensor_id, device_id, measurement_time, data
                FROM table2
                WHERE sensor_id = :sensor_id
                  AND device_id = :device_id
                  AND measurement_time < :timestamp
                ORDER BY measurement_time DESC
                FETCH FIRST 1 ROW ONLY
            """,
        sensor_id=101,
        device_id=202,
        timestamp=FAKE_PREDICTION_TIME,
    )


def test_fetch_last_measurement_before_time_not_found(db_instance):
    """Test returns None when no measurement is found."""
    cursor = db_instance.pool.acquire.return_value.cursor.return_value
    cursor.fetchone.return_value = None

    result = db_instance.fetch_last_measurement_for_sensor_device_before_time(
        sensor_id=999, device_id=888, timestamp=FAKE_PREDICTION_TIME
    )
    assert result is None


def test_insert_prediction_success(db_instance, caplog):
    """Test successful insertion of prediction."""
    conn = db_instance.pool.acquire.return_value
    cursor = conn.cursor.return_value

    with caplog.at_level("DEBUG"):
        db_instance.insert_prediction(
            sensor_id=101,
            device_id=202,
            param1=1.1,
            param2=2.2,
            result="GOOD"
        )

    cursor.execute.assert_called_once_with(
        """
                INSERT INTO table1 (prediction_time, sensor_id, device_id, param1, param2, result)
                VALUES (SYSTIMESTAMP, :sensor_id, :device_id, :param1, :param2, :result)
            """,
        sensor_id=101,
        device_id=202,
        param1=1.1,
        param2=2.2,
        result="GOOD"
    )
    conn.commit.assert_called_once()
    assert "Inserted prediction for sensor 101, device 202" in caplog.text


def test_insert_prediction_rollback_on_error(db_instance):
    """Test that insert rolls back on exception."""
    conn = db_instance.pool.acquire.return_value
    cursor = conn.cursor.return_value
    cursor.execute.side_effect = Exception("Insert failed")

    with pytest.raises(Exception, match="Insert failed"):
        db_instance.insert_prediction(101, 202, 1.1, 2.1, "BAD")

    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
    
def test_init_pool_failure(mock_oracledb):
    """Test that DB handles pool creation failure."""
    mock_oracledb.create_pool.side_effect = Exception("Network error")

    with pytest.raises(Exception, match="Network error"):
        DB()