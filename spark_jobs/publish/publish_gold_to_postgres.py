"""Publish gold Delta tables to Postgres serving mart via upsert."""

from __future__ import annotations

import json
import pathlib
from datetime import UTC, datetime

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from spark_jobs.utils.connection import pg_mart_url
from spark_jobs.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__, "publish")

GOLD_DIR = pathlib.Path("delta/gold")
WATERMARK_FILE = pathlib.Path("watermarks.json")

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

GOLD_PK: dict[str, str] = {
    "dim_customer": "customer_id",
    "dim_driver": "driver_sk",
    "dim_vehicle": "vehicle_sk",
    "dim_warehouse": "warehouse_id",
    "dim_route": "route_id",
    "dim_date": "date_id",
    "fct_orders": "order_id",
    "fct_shipments": "shipment_id",
    "fct_deliveries": "delivery_id",
}


def watermark_get(table: str) -> str:
    if WATERMARK_FILE.exists():
        try:
            return json.loads(WATERMARK_FILE.read_text()).get(
                f"gold:{table}", "1970-01-01T00:00:00"
            )
        except (json.JSONDecodeError, OSError):
            return "1970-01-01T00:00:00"
    return "1970-01-01T00:00:00"


def watermark_set(table: str, ts: str) -> None:
    data = json.loads(WATERMARK_FILE.read_text()) if WATERMARK_FILE.exists() else {}
    data[f"gold:{table}"] = ts
    WATERMARK_FILE.write_text(json.dumps(data, indent=2))


def upsert_to_mart(
    cur: psycopg2.extensions.cursor, table: str, df: pd.DataFrame, pk: str
) -> None:
    cols = list(df.columns)
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    update_sql = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk)
    rows = [
        tuple(None if pd.isna(x) else x for x in row)
        for row in df.itertuples(index=False)
    ]
    if not rows:
        return
    if update_sql:
        sql = f'INSERT INTO mart.{table} ({cols_sql}) VALUES %s ON CONFLICT ("{pk}") DO UPDATE SET {update_sql}'
    else:
        sql = f'INSERT INTO mart.{table} ({cols_sql}) VALUES %s ON CONFLICT ("{pk}") DO NOTHING'
    psycopg2.extras.execute_values(cur, sql, rows, page_size=5000)


def publish(table: str) -> None:
    df = None
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        df_spark = spark.table(f"freightlake.gold.{table}")
        df = df_spark.toPandas()
        logger.info("Read %s from Databricks gold %s rows", table, len(df))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Spark unavailable (%s), falling back to local parquet", exc)
        p = GOLD_DIR / f"{table}.parquet"
        if not p.exists():
            logger.warning("Gold %s not found at %s, skipping", table, p)
            return
        df = pd.read_parquet(p)
        logger.info("Read %s from local %s %s rows", table, p, len(df))

    if df is None or df.empty:
        logger.info("  %s 0 rows, skipping", table)
        return

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
            logger.warning(
                "Watermark filter failed for %s: %s, publishing all", table, e
            )

    try:
        conn = psycopg2.connect(pg_mart_url())
        conn.autocommit = True
    except Exception as exc:  # noqa: BLE001
        logger.error("Cannot connect to mart %s: %s", pg_mart_url(), exc)
        return
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS mart")
    ddl_path = pathlib.Path(__file__).parents[2] / "sql/serving_mart/01_mart_schema.sql"
    if ddl_path.exists():
        try:
            cur.execute(ddl_path.read_text())
        except Exception as exc:  # noqa: BLE001
            logger.warning("DDL execution warning: %s", exc)

    pk = GOLD_PK.get(table, str(df.columns[0]))
    # Ensure PK exists for ON CONFLICT — DDL already defines it, but add if missing
    try:
        cur.execute(
            f'ALTER TABLE mart.{table} ADD CONSTRAINT {table}_pkey PRIMARY KEY ("{pk}")'
        )
    except Exception:  # noqa: BLE001, S110
        pass

    # Idempotent upsert — never TRUNCATE
    cols_before = len(df)
    upsert_to_mart(cur, table, df, pk)
    logger.info("  mart.%s %s rows (upsert on %s)", table, cols_before, pk)
    if "updated_at" in df.columns:
        try:
            watermark_set(table, str(df["updated_at"].max()))
        except Exception:  # noqa: BLE001, S110
            pass
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
