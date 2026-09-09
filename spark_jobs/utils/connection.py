"""Connection helpers for Postgres, Mongo, Databricks."""

from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv

load_dotenv()


def pg_oltp_url() -> str:
    return os.getenv("POSTGRES_OLTP_URL", "postgresql://freightlake_oltp_user:changeme@localhost:5432/freightlake_oltp")


def pg_mart_url() -> str:
    return os.getenv("POSTGRES_MART_URL", "postgresql://freightlake_mart_user:changeme@localhost:5432/freightlake_mart")


def mongo_uri() -> str:
    return os.getenv("MONGO_URI", "mongodb://root:changeme@localhost:27017/freightlake_tracking?authSource=admin")


def databricks_cfg() -> dict[str, Any]:
    return {
        "host": os.getenv("DATABRICKS_HOST", ""),
        "token": os.getenv("DATABRICKS_TOKEN", ""),
        "http_path": os.getenv("DATABRICKS_HTTP_PATH", ""),
        "catalog": os.getenv("DATABRICKS_CATALOG", "freightlake"),
        "bronze": os.getenv("DATABRICKS_SCHEMA_BRONZE", "bronze"),
        "silver": os.getenv("DATABRICKS_SCHEMA_SILVER", "silver"),
        "gold": os.getenv("DATABRICKS_SCHEMA_GOLD", "gold"),
    }
