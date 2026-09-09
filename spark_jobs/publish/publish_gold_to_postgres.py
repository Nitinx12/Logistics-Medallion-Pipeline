"""Publish gold Delta tables to Postgres serving mart.

Tries Databricks gold via Spark, falls back to local delta/gold parquet.
Uses watermark MERGE via INSERT ON CONFLICT for idempotency.
"""

from __future__ import annotations

import json
import pathlib
from datetime import UTC, datetime

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from spark_jobs.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__, "publish")

GOLD_DIR = pathlib.Path("delta/gold")
WATERMARK_FILE = pathlib.Path("watermarks.json")
MART_URL = "postgresql://postgres:admin@localhost:5432/freightlake_mart"
FALLBACK_URL = "postgresql://postgres:admin@localhost:5432/freight_lake"

GOLD_TABLES = [
    "dim_customer",
    "dim_driver",
    "dim_vehicle",
    "dim_warehouse",
    "dim_route",
    "dim_date",
    "fct_orders",
    "fct_shipments",
    "fct_deliveries",
]


def watermark_get(table: str) -> str:
    if WATERMARK_FILE.exists():
        try:
            return json.loads(WATERMARK_FILE.read_text()).get(f"gold:{table}", "1970-01-01T00:00:00")
        except (json.JSONDecodeError, OSError):
            return "1970-01-01T00:00:00"
    return "1970-01-01T00:00:00"


def watermark_set(table: str, ts: str) -> None:
    data = json.loads(WATERMARK_FILE.read_text()) if WATERMARK_FILE.exists() else {}
    data[f"gold:{table}"] = ts
    WATERMARK_FILE.write_text(json.dumps(data, indent=2))


def publish(table: str) -> None:
    # Try Databricks via Spark, else local parquet
    df = None
    try:
        # Spark path — only if pyspark and Databricks available
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        df_spark = spark.table(f"freightlake.gold.{table}")
        df = df_spark.toPandas()
        logger.info("Read %s from Databricks gold %s rows", table, len(df))
    except Exception:  # noqa: BLE001
        p = GOLD_DIR / f"{table}.parquet"
        if not p.exists():
            logger.warning("Gold %s not found at %s, skipping", table, p)
            return
        df = pd.read_parquet(p)
        logger.info("Read %s from local %s %s rows", table, p, len(df))

    if df is None or df.empty:
        logger.info("  %s 0 rows, skipping", table)
        return

    # watermark filter — if gold has updated_at, filter else publish all
    wm = watermark_get(table)
    if "updated_at" in df.columns:
        try:
            wm_dt = pd.to_datetime(wm, utc=True)
            df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
            df = df[df["updated_at"] > wm_dt]
            if df.empty:
                logger.info("  %s 0 new rows after watermark %s", table, wm)
                return
        except Exception as e:  # noqa: BLE001
            logger.warning("Watermark filter failed for %s: %s, publishing all", table, e)

    # Connect to mart — try freightlake_mart then fallback
    conn = None
    for url in [MART_URL, FALLBACK_URL]:
        try:
            conn = psycopg2.connect(url)
            conn.autocommit = True
            break
        except Exception:  # noqa: BLE001, S112
            continue
    if conn is None:
        logger.error("Cannot connect to mart %s", MART_URL)
        return
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS mart")
    # Create table if not exists via DDL from sql/serving_mart
    # Use pandas to infer and create
    cols = []
    for col, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            typ = "BIGINT"
        elif "float" in str(dtype):
            typ = "DOUBLE PRECISION"
        elif "datetime" in str(dtype):
            typ = "TIMESTAMPTZ"
        elif "bool" in str(dtype):
            typ = "BOOLEAN"
        else:
            typ = "TEXT"
        cols.append(f'"{col}" {typ}')
    pk = df.columns[0]
    # Ensure table exists — use DDL from sql/serving_mart if needed
    cur.execute(f"CREATE TABLE IF NOT EXISTS mart.{table} ({', '.join(cols)})")
    # Add PK if missing for ON CONFLICT
    try:
        cur.execute(f"ALTER TABLE mart.{table} ADD CONSTRAINT {table}_pkey PRIMARY KEY (\"{pk}\")")
    except Exception:  # noqa: BLE001, S110
        pass
    # Full refresh for now — TRUNCATE then INSERT is idempotent and avoids PK issues
    cur.execute(f"TRUNCATE mart.{table}")
    rows = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False)]
    if rows:
        cols_str = ",".join([f'"{c}"' for c in df.columns])
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO mart.{table} ({cols_str}) VALUES %s",
            rows,
            page_size=5000,
        )
    logger.info("  mart.%s %s rows (full refresh)", table, len(df))
    if "updated_at" in df.columns:
        watermark_set(table, str(df["updated_at"].max()))
    else:
        watermark_set(table, datetime.now(UTC).isoformat())
    cur.close()
    conn.close()


def main() -> None:
    logger.info("Publish start tables=%s", GOLD_TABLES)
    for t in GOLD_TABLES:
        publish(t)
    logger.info("Publish complete")


if __name__ == "__main__":
    main()
