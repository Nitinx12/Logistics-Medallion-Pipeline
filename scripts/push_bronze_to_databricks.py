"""Push local delta/bronze/*.parquet to Databricks bronze tables.

Creates catalog/schema/tables if missing (via sql/databricks/*.sql)
then bulk inserts from local parquet using databricks-sql-connector.

Requires DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH in .env
"""

import os
import pathlib
import traceback
from datetime import UTC, datetime

import databricks.sql as dbsql
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BRONZE_DIR = pathlib.Path("delta/bronze")

# Map parquet file -> Databricks table
TABLES = {
    "customers": "freightlake.bronze.customers",
    "drivers": "freightlake.bronze.drivers",
    "trucks": "freightlake.bronze.trucks",
    "trailers": "freightlake.bronze.trailers",
    "facilities": "freightlake.bronze.facilities",
    "routes": "freightlake.bronze.routes",
    "loads": "freightlake.bronze.loads",
    "trips": "freightlake.bronze.trips",
    "fuel_purchases": "freightlake.bronze.fuel_purchases",
    "delivery_events": "freightlake.bronze.delivery_events",
    "safety_incidents": "freightlake.bronze.safety_incidents",
    "maintenance_records": "freightlake.bronze.maintenance_records",
}


def get_conn():
    host = os.getenv("DATABRICKS_HOST", "")
    # databricks-sql-connector expects host without https://
    # .env.example includes https://, strip it for connector
    host = host.removeprefix("https://")
    host = host.rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    if not host or not token or not http_path:
        raise RuntimeError("Missing DATABRICKS_HOST/TOKEN/HTTP_PATH")
    # databricks-sql-connector uses server_hostname, not host
    return dbsql.connect(server_hostname=host, http_path=http_path, access_token=token)


def run_sql_file(cursor, path: pathlib.Path):
    # Use sqlparse-free logic: strip -- comments per line, then split on ;
    # This avoids treating comment fragments like "license_number/license_state..."
    # as standalone statements.
    raw = path.read_text()
    # Remove -- comments (keep newlines)
    cleaned_lines = []
    for line in raw.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("--"):
            continue
        # also strip inline -- comments (simple, safe for these DDL files)
        if "--" in line:
            line = line.split("--", 1)[0]
        cleaned_lines.append(line)
    cleaned_sql = "\n".join(cleaned_lines)
    for stmt in cleaned_sql.split(";"):
        stmt = stmt.strip()
        if not stmt:
            continue
        try:
            cursor.execute(stmt)
            print(f"  OK: {stmt[:80]}...")
        except Exception as e:  # noqa: BLE001
            # Ignore already-exists / show-schema errors that are idempotent
            msg = str(e).lower()
            if (
                "already exists" in msg
                or "show schemas" in stmt.lower()
                or "show tables" in stmt.lower()
            ):
                print(f"  OK (ignored): {stmt[:80]}...")
                continue
            print(f"  ERR: {e}\n  stmt: {stmt[:200]}")


def push_table(cursor, parquet_name: str, fq_table: str):
    p = BRONZE_DIR / f"{parquet_name}.parquet"
    if not p.exists():
        print(f"  skip {parquet_name} no parquet")
        return
    df = pd.read_parquet(p)
    # Ensure _loaded_at exists for bronze tables that require it
    # Local parquet has _loaded_at already from extract (UTC)
    # If missing, add now
    if "_loaded_at" not in df.columns:
        df["_loaded_at"] = datetime.now(UTC)
    # For partitioned tables, _loaded_date is generated always as, don't insert
    if "_loaded_date" in df.columns:
        df = df.drop(columns=["_loaded_date"])
    # Truncate before load (idempotent)
    cursor.execute(f"DELETE FROM {fq_table}")
    print(f"  push {parquet_name}: {len(df)} rows -> {fq_table}")
    if df.empty:
        return
    cols = df.columns.tolist()
    # Build placeholders
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join([f"`{c}`" for c in cols])
    sql = f"INSERT INTO {fq_table} ({col_list}) VALUES ({placeholders})"
    # Convert df to list of tuples, handling NaN -> None and Timestamp
    rows = []
    for row in df.itertuples(index=False):
        vals = []
        for v in row:
            if pd.isna(v):
                vals.append(None)
            elif isinstance(v, pd.Timestamp):
                # databricks expects string for STRING columns that were DATE? keep as iso
                vals.append(v.isoformat() if pd.isna(v) else str(v))
            else:
                vals.append(v)
        rows.append(tuple(vals))
    # Batch insert 1000 at a time
    batch = 2000
    for i in range(0, len(rows), batch):
        chunk = rows[i : i + batch]
        cursor.executemany(sql, chunk)
        print(f"    batch {i // batch + 1} {len(chunk)} rows")
    print(f"  done {parquet_name}")


def main():
    conn = get_conn()
    cursor = conn.cursor()
    print("Connected to Databricks")
    # 1. schemas
    print("=== creating schemas ===")
    run_sql_file(cursor, pathlib.Path("sql/databricks/schemas.sql"))
    print("=== creating bronze tables ===")
    run_sql_file(cursor, pathlib.Path("sql/databricks/bronze_tables.sql"))
    # Fix for stg_customers column mapping - ensure silver table exists with mapping
    # Run ALTER on existing silver table if present
    try:
        cursor.execute(
            "ALTER TABLE freightlake.silver.stg_customers SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')"
        )
        print("  patched silver.stg_customers column mapping")
    except Exception as e:  # noqa: BLE001
        print(f"  patch stg_customers skipped: {e}")
    # 2. push data
    for parquet_name, fq_table in TABLES.items():
        try:
            push_table(cursor, parquet_name, fq_table)
        except Exception as e:  # noqa: BLE001
            print(f"ERR push {parquet_name}: {e}")
            traceback.print_exc()
    cursor.close()
    conn.close()
    print("Done")


if __name__ == "__main__":
    main()
