"""Initialize Databricks catalog and schemas for FreightLake."""

from __future__ import annotations

import os
import pathlib

from dotenv import load_dotenv

load_dotenv()

CATALOG = os.getenv("DATABRICKS_CATALOG", "freightlake")
BRONZE = os.getenv("DATABRICKS_SCHEMA_BRONZE", "bronze")
SILVER = os.getenv("DATABRICKS_SCHEMA_SILVER", "silver")
GOLD = os.getenv("DATABRICKS_SCHEMA_GOLD", "gold")


def via_sql() -> None:
    from databricks import sql

    host = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
    token = os.getenv("DATABRICKS_TOKEN", "")
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    if not host or not token or not http_path:
        print("Missing DATABRICKS_HOST/TOKEN/HTTP_PATH — see .env.example")
        return
    conn = sql.connect(server_hostname=host, http_path=http_path, access_token=token)
    cur = conn.cursor()
    for q in [
        f"CREATE CATALOG IF NOT EXISTS {CATALOG}",
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE}",
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER}",
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}",
    ]:
        print(f">> {q}")
        cur.execute(q)
        print("OK", cur.fetchall()[:1])
    cur.close()
    conn.close()
    print("Databricks schemas ready via SQL warehouse")


def via_files() -> None:
    print(
        "SQL scope missing — using file fallback. Run sql/databricks/schemas.sql and "
        "sql/databricks/watermark.sql manually in Databricks SQL Warehouse."
    )
    for name in ["sql/databricks/schemas.sql", "sql/databricks/watermark.sql", "sql/databricks/bronze_tables.sql"]:
        p = pathlib.Path(name)
        if p.exists():
            print(f"\n-- {name} --")
            print(p.read_text()[:800])


if __name__ == "__main__":
    try:
        via_sql()
    except Exception as e:  # noqa: BLE001
        print(f"SQL connect failed: {e}")
        via_files()
