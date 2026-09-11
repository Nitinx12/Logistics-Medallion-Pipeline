"""tests/unit/test_publish_gold_to_postgres.py"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import src.jobs.publish_gold_to_postgres as pub


def test_default_gold_tables_cover_star_schema():
    assert "dim_customers" in pub.DEFAULT_GOLD_TABLES
    assert "fact_loads" in pub.DEFAULT_GOLD_TABLES
    assert "fact_operations" in pub.DEFAULT_GOLD_TABLES
    assert len(pub.DEFAULT_GOLD_TABLES) == 14


def test_resolve_write_mode():
    with patch.object(pub.config, "BRONZE_WRITE_MODE", "local"):
        assert pub.resolve_write_mode("auto") == "local"
    with patch.object(pub.config, "BRONZE_WRITE_MODE", "warehouse"):
        assert pub.resolve_write_mode("auto") == "uc"
    assert pub.resolve_write_mode("local") == "local"
    assert pub.resolve_write_mode("uc") == "uc"


def test_postgres_jdbc_url():
    with patch.object(pub.config, "POSTGRES_HOST", "localhost"), \
         patch.object(pub.config, "POSTGRES_PORT", 5432), \
         patch.object(pub.config, "POSTGRES_DATABASE", "freight_lake"), \
         patch.object(pub.config, "POSTGRES_MART_DATABASE", None), \
         patch.object(pub.config, "POSTGRES_SSLMODE", None):
        # fallback to legacy when MART not set
        assert pub.postgres_jdbc_url() == "jdbc:postgresql://localhost:5432/freight_lake"
    with patch.object(pub.config, "POSTGRES_HOST", "localhost"), \
         patch.object(pub.config, "POSTGRES_PORT", 5432), \
         patch.object(pub.config, "POSTGRES_DATABASE", "db"), \
         patch.object(pub.config, "POSTGRES_MART_DATABASE", "mart_db"), \
         patch.object(pub.config, "POSTGRES_SSLMODE", "require"):
        url = pub.postgres_jdbc_url()
        assert "mart_db" in url
        assert "?sslmode=require" in url


def test_publish_table_dry_run():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.count.return_value = 123
    with patch.object(pub, "read_gold_table", return_value=mock_df) as mock_read:
        res = pub.publish_table(mock_spark, "dim_customers", "freightlake", "gold", "gold", "local", "./spark-warehouse/gold", dry_run=True)
        mock_read.assert_called_once()
        assert res["rows"] == 123
        assert res["status"] == "dry-run"
        assert res["table"] == "dim_customers"


def test_publish_table_write_overwrite():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.count.return_value = 10
    # Mock MART engine for schema creation
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    # _get_mart_engine is now used, mock it
    with patch.object(pub, "read_gold_table", return_value=mock_df), \
         patch("src.jobs.publish_gold_to_postgres.postgres_jdbc_url", return_value="jdbc:postgresql://localhost:5432/db"), \
         patch("src.jobs.publish_gold_to_postgres.postgres_props", return_value={"user": "u"}), \
         patch("src.jobs.publish_gold_to_postgres._get_mart_engine", return_value=mock_engine):
        res = pub.publish_table(mock_spark, "fact_loads", "freightlake", "gold", "gold", "uc", "./spark-warehouse/gold", dry_run=False)
        # should have called jdbc write
        mock_df.write.jdbc.assert_called_once()
        assert res["rows"] == 10
        assert res["status"] == "published"


def test_publish_table_error_handling():
    mock_spark = MagicMock()
    with patch.object(pub, "read_gold_table", side_effect=Exception("read failed")):
        res = pub.publish_table(mock_spark, "bad_table", "freightlake", "gold", "gold", "local", "./tmp", dry_run=False)
        assert "error" in res["status"]
        assert res["rows"] == 0
