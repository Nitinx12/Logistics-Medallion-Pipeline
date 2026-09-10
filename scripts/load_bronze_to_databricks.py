"""Load local delta/bronze parquet files into Databricks freightlake.bronze.* tables.

Reads each parquet from delta/bronze/, creates the table if it does not exist
(schema inferred from the parquet), then inserts rows in batches using the
Databricks SQL connector — the same connector dbt uses, so no extra auth is
needed beyond what dbt debug already verified.

Usage:
    uv run python scripts/load_bronze_to_databricks.py
    uv run python scripts/load_bronze_to_databricks.py --table customers
    uv run python scripts/load_bronze_to_databricks.py --dry-run
"""

from __future__ import annotations

import argparse
import math
import os
import pathlib
from typing import Any

import logging

import pandas as pd
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

CATALOG = os.getenv("DATABRICKS_CATALOG", "freightlake")
BRONZE_SCHEMA = os.getenv("DATABRICKS_SCHEMA_BRONZE", "bronze")
BRONZE_DIR = pathlib.Path("delta/bronze")
BATCH_SIZE = 500  # rows per INSERT statement

# Pandas dtype -> Databricks SQL type mapping
_DTYPE_MAP: dict[str, str] = {
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "DOUBLE",
    "float32": "FLOAT",
    "bool": "BOOLEAN",
    "object": "STRING",
    "datetime64[ns]": "TIMESTAMP",
    "datetime64[us]": "TIMESTAMP",
    "datetime64[ns, UTC]": "TIMESTAMP",
    "datetime64[us, UTC]": "TIMESTAMP",
    "date": "DATE",
}


def _dtype_to_sql(dtype: Any) -> str:
    name = str(dtype)
    for k, v in _DTYPE_MAP.items():
        if name.startswith(k):
            return v
    return "STRING"


def _py_to_sql_literal(val: Any) -> str:
    """Convert a Python value to a SQL literal string."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    # strings, timestamps, dates — escape single quotes
    return "'" + str(val).replace("'", "''") + "'"


def get_connection() -> sql.client.Connection:
    host = os.getenv("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN", "")
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    if not host or not token or not http_path:
        raise RuntimeError(
            "Missing DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_HTTP_PATH in .env"
        )
    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )


def ensure_schema(cur: Any) -> None:
    logger.info("Ensuring catalog and schema exist")
    cur.execute(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")


def create_table_from_df(cur: Any, table: str, df: pd.DataFrame, dry_run: bool) -> None:
    """CREATE TABLE IF NOT EXISTS inferred from the DataFrame schema."""
    cols = []
    for col, dtype in zip(df.columns, df.dtypes):
        sql_type = _dtype_to_sql(dtype)
        cols.append(f"  {col} {sql_type}")
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{table}\n"
        f"(\n"
        + ",\n".join(cols)
        + "\n) USING DELTA"
    )
    logger.info("DDL for %s:\n%s", table, ddl)
    if not dry_run:
        cur.execute(ddl)


def load_table(cur: Any, table: str, df: pd.DataFrame, dry_run: bool) -> int:
    """Insert DataFrame into Databricks table in batches; return row count."""
    total = len(df)
    if total == 0:
        logger.info("  %s: 0 rows — skipping", table)
        return 0

    fqn = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
    col_list = ", ".join(df.columns)
    inserted = 0

    for start in range(0, total, BATCH_SIZE):
        chunk = df.iloc[start : start + BATCH_SIZE]
        row_literals = []
        for row in chunk.itertuples(index=False, name=None):
            vals = ", ".join(_py_to_sql_literal(v) for v in row)
            row_literals.append(f"({vals})")
        stmt = (
            f"INSERT INTO {fqn} ({col_list}) VALUES\n" + ",\n".join(row_literals)
        )
        if dry_run:
            logger.info(
                "  [dry-run] %s: would insert rows %d..%d",
                table,
                start,
                min(start + BATCH_SIZE, total) - 1,
            )
        else:
            cur.execute(stmt)
        inserted += len(chunk)

    return inserted


def process_table(
    cur: Any,
    table: str,
    parquet_path: pathlib.Path,
    dry_run: bool,
    truncate: bool,
) -> None:
    logger.info("Processing table=%s from %s", table, parquet_path)
    df = pd.read_parquet(parquet_path)
    logger.info("  Loaded %d rows, %d columns", len(df), len(df.columns))

    # Flatten any object/dict columns to string so they survive SQL INSERT
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().head(5)
            if any(isinstance(v, (dict, list)) for v in sample):
                df[col] = df[col].astype(str)

    create_table_from_df(cur, table, df, dry_run)

    if truncate and not dry_run:
        logger.info(
            "  Truncating %s.%s.%s before reload", CATALOG, BRONZE_SCHEMA, table
        )
        cur.execute(f"DELETE FROM {CATALOG}.{BRONZE_SCHEMA}.{table}")

    n = load_table(cur, table, df, dry_run)
    logger.info("  %s: %d rows inserted (dry_run=%s)", table, n, dry_run)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load local delta/bronze parquet files into Databricks"
    )
    parser.add_argument(
        "--table",
        help="Load only this table (default: all parquet files in delta/bronze/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and log everything but do not execute any SQL",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="DELETE existing rows before inserting (full reload instead of append)",
    )
    args = parser.parse_args()

    if not BRONZE_DIR.exists():
        logger.error("delta/bronze/ not found — run bronze extract first")
        return

    parquet_files = sorted(BRONZE_DIR.glob("*.parquet"))
    if args.table:
        parquet_files = [p for p in parquet_files if p.stem == args.table]
        if not parquet_files:
            logger.error(
                "No parquet file for table=%s in %s", args.table, BRONZE_DIR
            )
            return

    logger.info(
        "Starting Databricks bronze load: %d tables, dry_run=%s, truncate=%s",
        len(parquet_files),
        args.dry_run,
        args.truncate,
    )

    conn = get_connection()
    cur = conn.cursor()

    try:
        if not args.dry_run:
            ensure_schema(cur)

        for pf in parquet_files:
            process_table(cur, pf.stem, pf, args.dry_run, args.truncate)

        logger.info("Bronze load complete for %d tables", len(parquet_files))
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
