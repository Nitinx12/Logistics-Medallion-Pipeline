"""Ensure required env vars exist before any src import during test collection.

src/utils/engine.py validates required env at import time and raises OSError
if they are missing. On CI there is no .env file, so collection would fail
before any test runs. This conftest sets dummy values early so imports succeed;
individual tests can still override via patch.dict as needed.
"""

import os

# Dummy values sufficient for import time validation; real values come from
# .env locally or from patched env in specific tests.
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DATABASE", "freight_lake")
os.environ.setdefault("POSTGRES_USERNAME", "postgres")
os.environ.setdefault("POSTGRES_PASSWORD", "postgres")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("MONGO_DB", "freight_lake")
# Optional but keeps warnings quiet during collection
os.environ.setdefault("POSTGRES_SCHEMA_BRONZE", "bronze")
os.environ.setdefault("POSTGRES_SCHEMA_SILVER", "silver")
os.environ.setdefault("POSTGRES_SCHEMA_GOLD", "gold")
os.environ.setdefault("DATABRICKS_HOST", "dummy.host")
os.environ.setdefault("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/dummy")
os.environ.setdefault("DATABRICKS_TOKEN", "dapi_dummy")
os.environ.setdefault("DATABRICKS_CATALOG", "freightlake")
os.environ.setdefault("DATABRICKS_SCHEMA", "bronze")
