"""tests/unit/test_pg_extract_incremental.py

Unit tests for pg_extract_incremental watermark, chunking and merge helpers.
Covers AGENTS.md quality gate: watermark filtering and upsert merge condition.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure repo root on path for src imports when running via pytest from repo root
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.jobs.pg_extract_incremental as pg


def test_build_windows_start_greater_than_end():
    s = datetime(2026, 1, 10)
    e = datetime(2026, 1, 9)
    assert pg.build_windows(s, e, 1.0, full_load=True) == []
    assert pg.build_windows(s, e, 1.0, full_load=False) == []


def test_build_windows_start_eq_end_full_vs_incremental():
    s = datetime(2026, 1, 10, 12, 0, 0)
    # Full load with same timestamp must return one inclusive window (bulk seeded data)
    assert pg.build_windows(s, s, 1.0, full_load=True) == [(s, s, True)]
    # Incremental with same watermark means up to date, no windows
    assert pg.build_windows(s, s, 1.0, full_load=False) == []


def test_build_windows_incremental_exclusive_first():
    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 3)
    windows = pg.build_windows(start, end, 1.0, full_load=False)
    # first window should be exclusive (False) for incremental
    assert windows[0] == (start, datetime(2026, 1, 2), False)
    assert windows[1] == (datetime(2026, 1, 2), datetime(2026, 1, 3), False)
    assert len(windows) == 2


def test_build_windows_full_inclusive_first():
    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 3)
    windows = pg.build_windows(start, end, 1.0, full_load=True)
    # first window inclusive for full
    assert windows[0] == (start, datetime(2026, 1, 2), True)
    # second window not first, inclusive False
    assert windows[1] == (datetime(2026, 1, 2), datetime(2026, 1, 3), False)


def test_build_windows_chunk_days_fractional():
    start = datetime(2026, 1, 1, 0, 0, 0)
    end = datetime(2026, 1, 2, 12, 0, 0)  # 1.5 days
    windows = pg.build_windows(start, end, 1.0, full_load=True)
    assert len(windows) == 2
    assert windows[0][1] == datetime(2026, 1, 2, 0, 0, 0)
    assert windows[1][1] == end


def test_build_windows_chunk_larger_than_range():
    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 2)
    windows = pg.build_windows(start, end, 10.0, full_load=True)
    assert windows == [(start, end, True)]


def test_resolve_write_mode_cli_overrides_env():
    with patch.object(pg.config, "BRONZE_WRITE_MODE", "local"):
        assert pg.resolve_write_mode("warehouse") == "warehouse"
        assert pg.resolve_write_mode("LOCAL") == "local"
        assert pg.resolve_write_mode("auto") == "local"
        # Auto is case sensitive in current impl, so "Auto" returns "auto" not env
        assert pg.resolve_write_mode("Auto") == "auto"


def test_resolve_write_mode_env_fallback():
    with patch.object(pg.config, "BRONZE_WRITE_MODE", "warehouse"):
        assert pg.resolve_write_mode("auto") == "warehouse"
    with patch.object(pg.config, "BRONZE_WRITE_MODE", "bad_value"):
        assert pg.resolve_write_mode("auto") == "warehouse"


def test_build_job_config_merge_requires_key():
    args = argparse.Namespace(
        source_schema="public",
        target_catalog="main",
        target_schema="bronze",
        mode="merge",
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
    with pytest.raises(SystemExit, match="--mode merge requires --key-column"):
        pg.build_job_config(args, "orders")


def test_build_job_config_missing_catalog():
    args = argparse.Namespace(
        source_schema="public",
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
    with pytest.raises(SystemExit, match="Target catalog/schema not set"):
        pg.build_job_config(args, "orders")


def test_build_job_config_since_parsing():
    args = argparse.Namespace(
        source_schema="public",
        target_catalog="main",
        target_schema="bronze",
        mode="append",
        key_column=None,
        updated_at_column="updated_at",
        chunk_days=1.0,
        fetch_size=10000,
        num_partitions=1,
        since="2026-01-15T08:30:00",
        full=False,
        dry_run=False,
        write_mode="auto",
    )
    cfg = pg.build_job_config(args, "public.orders", target_table_override=None)
    assert cfg.since_override == datetime(2026, 1, 15, 8, 30, 0)
    assert cfg.source_fqtn == "public.orders"
    assert cfg.target_fqtn == "main.bronze.orders"


def test_is_uc_managed_blocked_markers():
    for marker in [
        "ErrorCode: 5108",
        "ErrorCode: 5105",
        "createStagingTable",
        "UNITY_CATALOG_EXTERNAL_CREATE_TABLE_REQUEST_FOR_NON_EXTERNAL_TABLE_DENIED",
    ]:
        assert pg._is_uc_managed_blocked(Exception(marker)) is True
    assert pg._is_uc_managed_blocked(Exception("some random error")) is False


def test_coerce_datetime_variants():
    dt = datetime(2026, 3, 1, 12, 0, 0)
    assert pg._coerce_datetime(dt, "ctx") == dt
    assert pg._coerce_datetime(None, "ctx") is None
    assert pg._coerce_datetime("2026-03-01T12:00:00", "ctx") == dt
    with pytest.raises(ValueError, match="Could not parse"):
        pg._coerce_datetime("not-a-date", "ctx")
    with pytest.raises(TypeError):
        pg._coerce_datetime(12345, "ctx")


def test_short_error_truncation_and_markers():
    long_msg = "x" * 500
    assert len(pg.short_error(Exception(long_msg))) <= 220
    # prefers ApiException line
    err = Exception("header\nApiException: 403 Forbidden line\nfooter")
    assert "ApiException" in pg.short_error(err)


def test_local_delta_path():
    with patch.object(pg.config, "BRONZE_LOCAL_PATH", "./spark-warehouse/bronze"):
        p = pg._local_delta_path("freightlake.bronze.drivers")
        assert p.name == "drivers"
        assert "bronze" in str(p)


def test_write_chunk_routes_to_warehouse_and_local(monkeypatch):
    # warehouse path mocked
    mock_df = MagicMock()
    mock_schema = MagicMock()
    mock_schema.fields = []
    mock_df.schema = mock_schema
    job_w = pg.JobConfig(
        source_fqtn="public.orders",
        target_fqtn="main.bronze.orders",
        updated_at_col="updated_at",
        mode="append",
        key_column=None,
        chunk_days=1.0,
        fetch_size=10000,
        num_partitions=1,
        since_override=None,
        force_full=False,
        dry_run=False,
        write_mode="warehouse",
    )
    with patch("src.jobs.pg_extract_incremental._write_via_warehouse") as mock_wh:
        pg.write_chunk(MagicMock(), mock_df, job_w, table_exists=True)
        mock_wh.assert_called_once()

    job_l = pg.JobConfig(
        source_fqtn="public.orders",
        target_fqtn="main.bronze.orders",
        updated_at_col="updated_at",
        mode="append",
        key_column=None,
        chunk_days=1.0,
        fetch_size=10000,
        num_partitions=1,
        since_override=None,
        force_full=False,
        dry_run=False,
        write_mode="local",
    )
    with patch("src.jobs.pg_extract_incremental._write_local_delta") as mock_local:
        pg.write_chunk(MagicMock(), mock_df, job_l, table_exists=False)
        mock_local.assert_called_once()


def test_target_table_exists_delegates(monkeypatch):
    mock_spark = MagicMock()
    # local mode
    with patch("src.jobs.pg_extract_incremental._local_table_exists", return_value=True) as m:
        assert pg.target_table_exists(mock_spark, "main.bronze.orders", "local") is True
        m.assert_called_once()
    # warehouse mode success
    with patch("src.jobs.pg_extract_incremental._warehouse_table_exists", return_value=True) as m:
        assert pg.target_table_exists(mock_spark, "main.bronze.orders", "warehouse") is True
