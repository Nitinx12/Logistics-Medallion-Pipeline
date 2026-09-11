"""tests/unit/test_mongo_extract_incremental.py

Unit tests for mongo_extract_incremental parity with pg logic.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.jobs.mongo_extract_incremental as mg


def test_build_windows_parity_with_pg():
    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 2)
    assert mg.build_windows(start, end, 1.0, True) == [(start, end, True)]
    assert mg.build_windows(end, start, 1.0, True) == []
    # same timestamp incremental empty
    assert mg.build_windows(start, start, 1.0, False) == []


def test_resolve_write_mode_parity():
    with patch.object(mg.config, "BRONZE_WRITE_MODE", "local"):
        assert mg.resolve_write_mode("warehouse") == "warehouse"
        assert mg.resolve_write_mode("auto") == "local"
    with patch.object(mg.config, "BRONZE_WRITE_MODE", "invalid"):
        assert mg.resolve_write_mode("auto") == "warehouse"


def test_build_job_config_validation():
    args = argparse.Namespace(
        target_catalog=None,
        target_schema="bronze",
        mode="append",
        key_column=None,
        updated_at_column="updated_at",
        chunk_days=1.0,
        fetch_size=10000,
        num_partitions=1,
        since=None,
        full=False,
        dry_run=False,
        write_mode="auto",
    )
    with pytest.raises(SystemExit):
        mg.build_job_config(args, "delivery_events")
    args.target_catalog = "main"
    args.mode = "merge"
    with pytest.raises(SystemExit, match="--mode merge requires"):
        mg.build_job_config(args, "delivery_events")
    args.mode = "append"
    cfg = mg.build_job_config(args, "delivery_events")
    assert cfg.source_collection == "delivery_events"
    assert cfg.target_fqtn == "main.bronze.delivery_events"


def test_is_uc_managed_blocked_mongo():
    assert mg._is_uc_managed_blocked(Exception("ErrorCode: 5108 something")) is True
    assert mg._is_uc_managed_blocked(Exception("ok")) is False


def test_coerce_datetime_mongo():
    dt = datetime(2026, 5, 1)
    assert mg._coerce_datetime(dt, "ctx") == dt
    assert mg._coerce_datetime("2026-05-01T00:00:00", "ctx") == dt
    with pytest.raises(ValueError):
        mg._coerce_datetime("bad", "ctx")


def test_discover_collections_filters_system():
    mock_db = MagicMock()
    mock_db.list_collection_names.return_value = ["delivery_events", "system.indexes", "orders"]
    cols = mg.discover_collections(mock_db, set())
    assert "system.indexes" not in cols
    assert "delivery_events" in cols
    assert cols == sorted(cols)

    # exclude set
    cols2 = mg.discover_collections(mock_db, {"orders"})
    assert "orders" not in cols2


def test_collection_has_field():
    mock_db = MagicMock()
    mock_db.__getitem__.return_value.find_one.return_value = {"x": 1}
    assert mg.collection_has_field(mock_db, "coll", "updated_at") is True
    mock_db.__getitem__.return_value.find_one.return_value = None
    assert mg.collection_has_field(mock_db, "coll", "missing") is False


def test_short_error_mongo():
    assert "404" in mg.short_error(Exception("line1\n404 Not Found detail\nline3"))


def test_target_table_exists_local_and_warehouse():
    mock_spark = MagicMock()
    with patch("src.jobs.mongo_extract_incremental._local_table_exists", return_value=True):
        assert mg.target_table_exists(mock_spark, "a.b.c", "local") is True
    with patch("src.jobs.mongo_extract_incremental._warehouse_table_exists", return_value=True):
        assert mg.target_table_exists(mock_spark, "a.b.c", "warehouse") is True
