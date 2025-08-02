# tests/test_retry.py
import pytest
from unittest.mock import MagicMock, patch, call
import time
from utils.retry import retry_db_operation


# Dummy function to decorate
def dummy_db_op(*args, **kwargs):
    return "success"


# Test that retry works with retryable exceptions
class TestRetryDBOperation:
    @patch("utils.retry.RETRY_CONFIG")
    @patch("utils.retry.time.sleep")
    def test_retries_on_retryable_exception(self, mock_sleep, mock_retry_config):
        """Test that retry_db_operation retries on retryable exceptions."""
        # Arrange
        mock_retry_config.attempts = 3
        mock_retry_config.delay = 0.01
        mock_retry_config.backoff = 2
        mock_retry_config.exceptions = (OSError,)
        
        mock_func = MagicMock(side_effect=[OSError("Fail 1"), OSError("Fail 2"), "success"])

        decorated = retry_db_operation(mock_func)

        # Act
        result = decorated("arg1", key="value")

        # Assert
        assert result == "success"
        assert mock_func.call_count == 3
        mock_func.assert_any_call("arg1", key="value")

        # Check sleep calls: delay=0.01, backoff=2 → [0.01, 0.02]
        assert mock_sleep.call_count == 2
        mock_sleep.assert_has_calls([call(0.01), call(0.02)])

    @patch("utils.retry.RETRY_CONFIG")
    @patch("utils.retry.time.sleep")
    def test_raises_last_exception_if_all_attempts_fail(self, mock_sleep, mock_retry_config, caplog):
        """Test that after all retries fail, the last exception is raised."""
        mock_retry_config.attempts = 3
        mock_retry_config.exceptions = (OSError,)
        
        mock_func = MagicMock(side_effect=OSError("Always fails"))

        decorated = retry_db_operation(mock_func)

        with caplog.at_level("ERROR"):
            with pytest.raises(OSError, match="Always fails"):
                decorated()

        assert mock_func.call_count == 3
        assert mock_sleep.call_count == 2  # Two delays between 3 attempts

        assert "All retry attempts failed for DB operation" in caplog.text

    @patch("utils.retry.RETRY_CONFIG")
    @patch("utils.retry.time.sleep")
    def test_does_not_retry_on_non_retryable_exception(self, mock_sleep, mock_retry_config, caplog):
        """Test that non-retryable exceptions (e.g. ValueError) are raised immediately."""
        mock_retry_config.exceptions = (OSError,)
        
        mock_func = MagicMock(side_effect=ValueError("Invalid data"))

        decorated = retry_db_operation(mock_func)

        with caplog.at_level("ERROR"):
            with pytest.raises(ValueError, match="Invalid data"):
                decorated()

        # Should fail on first try, no retries
        assert mock_func.call_count == 1
        mock_sleep.assert_not_called()

        # Check correct log message
        assert "Non-retryable error in DB operation" in caplog.text

    @patch("utils.retry.RETRY_CONFIG")
    @patch("utils.retry.time.sleep")
    def test_no_retry_if_first_call_succeeds(self, mock_sleep, mock_retry_config):
        """Test that no retry or sleep occurs if the first call succeeds."""
        mock_retry_config.exceptions = (OSError,)
        
        mock_func = MagicMock(return_value="ok")

        decorated = retry_db_operation(mock_func)

        result = decorated()

        assert result == "ok"
        mock_func.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("utils.retry.RETRY_CONFIG")
    @patch("utils.retry.time.sleep")
    def test_logs_warning_on_each_retry(self, mock_sleep, mock_retry_config, caplog):
        """Test that each retry logs a warning."""
        mock_retry_config.attempts = 3
        mock_retry_config.exceptions = (OSError,)
        
        mock_func = MagicMock(side_effect=[OSError("Boom"), "success"])

        decorated = retry_db_operation(mock_func)

        with caplog.at_level("WARNING"):
            result = decorated()

        assert result == "success"
        assert "DB operation failed (attempt 1/3): Boom" in caplog.text
        assert mock_sleep.call_count == 1

    @patch("utils.retry.RETRY_CONFIG")
    def test_preserves_function_metadata(self, mock_retry_config):
        """Test that @wraps preserves function name and doc."""
        mock_retry_config.exceptions = (OSError,)
        
        @retry_db_operation
        def example():
            """Example docstring."""
            pass

        assert example.__name__ == "example"
        assert example.__doc__ == "Example docstring."