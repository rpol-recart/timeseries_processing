# tests/test_config.py
import os
import pytest
import importlib
from config import DB_USER, DB_PASSWORD, DB_DSN, LATE_DATA_TOLERANCE

def test_default_config_values():
    assert DB_USER == "user"
    assert DB_PASSWORD == "pass"
    assert DB_DSN == "localhost:1521/XE"
    assert LATE_DATA_TOLERANCE.total_seconds() == 86400  # 24 часа

def test_env_override_config(monkeypatch):
    monkeypatch.setenv("DB_USER", "test_user")
    monkeypatch.setenv("DB_PASSWORD", "test_pass")
    monkeypatch.setenv("DB_DSN", "remote:1521/ORCL")

    # Force reload config module
    import config
    importlib.reload(config)

    assert config.DB_USER == "test_user"
    assert config.DB_PASSWORD == "test_pass"
    assert config.DB_DSN == "remote:1521/ORCL"