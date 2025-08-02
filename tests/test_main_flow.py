# tests/test_main_flow.py
import pytest
from unittest.mock import MagicMock
from main_old import main

def test_main_no_last_prediction(monkeypatch):
    mock_db = MagicMock()
    mock_db.fetch_last_prediction.return_value = None
    monkeypatch.setattr("main.DB", lambda: mock_db)

    with pytest.raises(SystemExit):
        main()