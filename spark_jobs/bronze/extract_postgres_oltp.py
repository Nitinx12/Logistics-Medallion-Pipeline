"""Bronze extract — Postgres OLTP via watermark incremental MERGE.

Tries Databricks MERGE INTO, falls back to local delta/bronze parquet with
watermarks.json for idempotency when no warehouse is available.
"""

from __future__ import annotations

import json
import pathlib
from datetime import UTC, datetime

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from spark_jobs.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__, "bronze")

TABLES = [
    "customers",
    "drivers",
    "trucks",
    "trailers",
    "facilities",
    "routes",
    "loads",
    "trips",
    "fuel_purchases",
]

WATERMARK_SQL = """
SELECT watermark_ts FROM freightlake.bronze.etl_watermark
WHERE source_system='postgres_oltp' AND source_table=%s
"""

MERGE_SQL_TEMPLATE = """
MERGE INTO freightlake.bronze.{table} AS tgt
USING (SELECT * FROM temp_{table}) AS src
ON tgt.{pk} = src.{pk}
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

BRONZE_DIR = pathlib.Path("delta/bronze")
WATERMARK_FILE = pathlib.Path("watermarks.json")
POSTGRES_URL = "postgresql://postgres:admin@localhost:5432/freight_lake"


def watermark_get(table: str) -> str:
    if WATERMARK_FILE.exists():
        try:
            return json.loads(WATERMARK_FILE.read_text()).get(
                f"pg:{table}", "1970-01-01T00:00:00"
            )
        except (json.JSONDecodeError, OSError):
            return "1970-01-01T00:00:00"
    return "1970-01-01T00:00:00"


def watermark_set(table: str, ts: str) -> None:
    data = json.loads(WATERMARK_FILE.read_text()) if WATERMARK_FILE.exists() else {}
    data[f"pg:{table}"] = ts
    WATERMARK_FILE.write_text(json.dumps(data, indent=2))


def extract_table(table: str) -> None:
    wm = watermark_get(table)
    logger.info("Extracting postgres table=%s watermark %s", table, wm)
    conn = psycopg2.connect(POSTGRES_URL)
    try:
        df = pd.read_sql(
            f"SELECT * FROM {table} WHERE updated_at > %s", conn, params=(wm,)
        )
        if df.empty:
            logger.info("  %s 0 new rows", table)
            return
        df.columns = [c.strip() for c in df.columns]
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .replace({"nan": None, "None": None, "": None})
            )
        df["_loaded_at"] = datetime.now(UTC)
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        out = BRONZE_DIR / f"{table}.parquet"
        # MERGE logic: upsert on PK (first column)
        pk_col = df.columns[0]
        if out.exists():
            existing = pd.read_parquet(out)
            combined = pd.concat([existing, df], ignore_index=True)
            # keep latest by updated_at
            combined = combined.sort_values("updated_at").drop_duplicates(
                subset=[pk_col], keep="last"
            )
            combined.to_parquet(out, index=False)
            logger.info(
                "  %s MERGE %s rows (upsert on %s) -> %s", table, len(df), pk_col, out
            )
        else:
            df.to_parquet(out, index=False)
            logger.info("  %s %s rows -> %s", table, len(df), out)
        max_ts = str(df["updated_at"].max())
        watermark_set(table, max_ts)
    finally:
        conn.close()


def main() -> None:
    logger.info("Bronze Postgres extract start tables=%s", TABLES)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    for t in TABLES:
        extract_table(t)
    logger.info("Bronze Postgres extract complete")


if __name__ == "__main__":
    main()
