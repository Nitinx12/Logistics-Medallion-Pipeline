"""tests/unit/test_engine.py

Tests for src/utils/engine.py env parsing without touching real .env.
We import via importlib to control env vars per test.
"""

import importlib.util
import os
from pathlib import Path
from unittest.mock import patch


def load_engine_with_env(env: dict):
    # reload engine module with patched env, isolated from OS env and real .env
    spec = importlib.util.spec_from_file_location(
        "engine_under_test", Path(__file__).resolve().parents[2] / "src" / "utils" / "engine.py"
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    # clear=True ensures no leftover OS env interferes; do not read real .env
    with patch.dict(os.environ, env, clear=True):
        with patch("dotenv.load_dotenv", return_value=False):
            import warnings

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                spec.loader.exec_module(mod)  # type: ignore
                return mod, w
    return None, []


def test_engine_requires_postgres_and_mongo():
    base = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DATABASE": "freight_lake",
        "POSTGRES_USERNAME": "postgres",
        "POSTGRES_PASSWORD": "pw",
        "MONGO_URI": "mongodb://localhost:27017",
        "MONGO_DB": "freight_lake",
    }
    mod, w = load_engine_with_env(base)
    assert mod.POSTGRES_HOST == "localhost"
    assert mod.POSTGRES_PORT == 5432
    assert isinstance(mod.POSTGRES_PORT, int)
    assert mod.MONGO_URI == "mongodb://localhost:27017"


def test_engine_missing_required_raises():
    env = {
        "POSTGRES_HOST": None,
        "POSTGRES_PORT": None,
        "MONGO_URI": None,
    }
    # Need to remove keys entirely to trigger missing
    clean = {}
    try:
        mod, w = load_engine_with_env(clean)
        assert False, "should have raised"
    except OSError as e:
        assert "Missing required environment variables" in str(e)


def test_engine_invalid_port_raises():
    env = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "not_an_int",
        "POSTGRES_DATABASE": "db",
        "POSTGRES_USERNAME": "u",
        "POSTGRES_PASSWORD": "p",
        "MONGO_URI": "mongodb://localhost",
        "MONGO_DB": "db",
    }
    try:
        mod, w = load_engine_with_env(env)
        assert False
    except OSError as e:
        assert "POSTGRES_PORT must be an integer" in str(e)


def test_engine_bronze_write_mode_defaults():
    env = {
        "POSTGRES_HOST": "h",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DATABASE": "db",
        "POSTGRES_USERNAME": "u",
        "POSTGRES_PASSWORD": "p",
        "MONGO_URI": "mongodb://localhost",
        "MONGO_DB": "db",
    }
    mod, w = load_engine_with_env(env)
    assert mod.BRONZE_WRITE_MODE == "warehouse"
    assert mod.BRONZE_LOCAL_PATH == "./spark-warehouse/bronze"


def test_engine_warns_on_missing_optional():
    env = {
        "POSTGRES_HOST": "h",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DATABASE": "db",
        "POSTGRES_USERNAME": "u",
        "POSTGRES_PASSWORD": "p",
        "MONGO_URI": "mongodb://localhost",
        "MONGO_DB": "db",
    }
    mod, w = load_engine_with_env(env)
    # should warn about missing optional schemas and databricks
    assert any("bronze/silver/gold" in str(x.message) for x in w) or any(
        "Databricks" in str(x.message) for x in w
    )
