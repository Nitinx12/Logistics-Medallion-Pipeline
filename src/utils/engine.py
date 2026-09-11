import os
import warnings

from dotenv import load_dotenv

if not load_dotenv():
    print("Warning: no .env file found, relying on system environment variables")

# =========================================================
# POSTGRES
# =========================================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
# Optional: required by managed providers like Neon, unset/None for plain
# local Postgres so this never breaks the Docker dev setup.
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE")
POSTGRES_CHANNEL_BINDING = os.getenv("POSTGRES_CHANNEL_BINDING")

# Split databases per diagram PG_OLTP vs PG_MART, with fallback to legacy single DB
# docker/init/01_create_database.sql creates freightlake_oltp, freightlake_mart, airflow
POSTGRES_SUPERUSER = os.getenv("POSTGRES_SUPERUSER", POSTGRES_USERNAME)
POSTGRES_SUPERUSER_PASSWORD = os.getenv("POSTGRES_SUPERUSER_PASSWORD", POSTGRES_PASSWORD)
POSTGRES_OLTP_DATABASE = os.getenv("POSTGRES_OLTP_DATABASE", POSTGRES_DATABASE)
POSTGRES_OLTP_USER = os.getenv("POSTGRES_OLTP_USER", POSTGRES_USERNAME)
POSTGRES_OLTP_PASSWORD = os.getenv("POSTGRES_OLTP_PASSWORD", POSTGRES_PASSWORD)
POSTGRES_MART_DATABASE = os.getenv("POSTGRES_MART_DATABASE", POSTGRES_DATABASE)
POSTGRES_MART_USER = os.getenv("POSTGRES_MART_USER", POSTGRES_USERNAME)
POSTGRES_MART_PASSWORD = os.getenv("POSTGRES_MART_PASSWORD", POSTGRES_PASSWORD)
POSTGRES_AIRFLOW_DATABASE = os.getenv("POSTGRES_AIRFLOW_DATABASE", "airflow")
POSTGRES_AIRFLOW_USER = os.getenv("POSTGRES_AIRFLOW_USER", "airflow")
POSTGRES_AIRFLOW_PASSWORD = os.getenv("POSTGRES_AIRFLOW_PASSWORD", "admin")

# Cast port to int now, fail loudly later if it's garbage instead of silently
# passing a string into a driver that expects int.
if POSTGRES_PORT is not None:
    try:
        POSTGRES_PORT = int(POSTGRES_PORT)
    except ValueError:
        raise OSError(f"POSTGRES_PORT must be an integer, got: {POSTGRES_PORT!r}")

# Postgres schemas (medallion architecture). Optional: only needed by
# scripts that actually build a bronze/silver/gold layout. mongo_exp.py
# writes straight into the `public` schema and does not touch these.
POSTGRES_SCHEMA_BRONZE = os.getenv("POSTGRES_SCHEMA_BRONZE")
POSTGRES_SCHEMA_SILVER = os.getenv("POSTGRES_SCHEMA_SILVER")
POSTGRES_SCHEMA_GOLD = os.getenv("POSTGRES_SCHEMA_GOLD")

# =========================================================
# PYSPARK
# =========================================================
# Read here mainly so a missing value fails fast with a clear message;
# Spark itself picks these up from the process environment once load_dotenv()
# has run, so this doesn't need to be passed anywhere manually.
PYSPARK_PYTHON = os.getenv("PYSPARK_PYTHON")
PYSPARK_DRIVER_PYTHON = os.getenv("PYSPARK_DRIVER_PYTHON")

# =========================================================
# MONGODB
# =========================================================
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

# =========================================================
# DATABRICKS
# =========================================================
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
# Optional: scopes queries to a specific catalog/schema instead of the SQL
# warehouse's default. Only needed by scripts that actually target Databricks.
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG")
DATABRICKS_SCHEMA = os.getenv("DATABRICKS_SCHEMA")

# Bronze write strategy for pg_extract_incremental.
# Databricks managed tables (CATALOG_DB_STORAGE) cannot be created or written
# from outside Databricks compute (ErrorCode 5108 createStagingTable and 5105
# getTableCredentials, plus UNITY_CATALOG_EXTERNAL_CREATE_TABLE_REQUEST_FOR_NON_EXTERNAL_TABLE_DENIED).
# Options:
#   warehouse   -> use Databricks SQL warehouse via databricks-sql-connector (default, works outside)
#   local       -> write Delta to local filesystem under BRONZE_LOCAL_PATH (no UC, for offline dev)
#   uc_managed  -> direct Unity Catalog staged write via UCSingleCatalog (only inside Databricks)
#   uc_external -> Unity Catalog external tables via external location (requires DATABRICKS_EXTERNAL_LOCATION)
BRONZE_WRITE_MODE = os.getenv("BRONZE_WRITE_MODE", "warehouse").strip().lower()
BRONZE_LOCAL_PATH = os.getenv("BRONZE_LOCAL_PATH", "./spark-warehouse/bronze")
DATABRICKS_EXTERNAL_LOCATION = os.getenv("DATABRICKS_EXTERNAL_LOCATION")
# Optional explicit external table base location, e.g. s3://bucket/freightlake/bronze
DATABRICKS_EXTERNAL_BASE_PATH = os.getenv("DATABRICKS_EXTERNAL_BASE_PATH", DATABRICKS_EXTERNAL_LOCATION or "")


# =========================================================
# VALIDATION
# =========================================================
# Hard-required: every script in this project touches Postgres and/or
# Mongo, so these must always be present or nothing can run.
_required = {
    "POSTGRES_HOST": POSTGRES_HOST,
    "POSTGRES_PORT": POSTGRES_PORT,
    "POSTGRES_DATABASE": POSTGRES_DATABASE,
    "POSTGRES_USERNAME": POSTGRES_USERNAME,
    "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    "MONGO_URI": MONGO_URI,
    "MONGO_DB": MONGO_DB,
}

_missing = [k for k, v in _required.items() if not v]

if _missing:
    raise OSError(f"Missing required environment variables: {', '.join(_missing)}")

# Split DB optional vars, only used when init scripts create separate OLTP/MART DBs
_optional_split = {
    "POSTGRES_OLTP_DATABASE": POSTGRES_OLTP_DATABASE,
    "POSTGRES_MART_DATABASE": POSTGRES_MART_DATABASE,
}

# Soft-required: only needed by scripts that build a bronze/silver/gold
# schema layout. Missing values here don't stop the Mongo -> Postgres
# pipeline from running, but scripts that DO use them will fail with a
# clear error the moment they're actually touched -- not silently.
_optional = {
    "POSTGRES_SCHEMA_BRONZE": POSTGRES_SCHEMA_BRONZE,
    "POSTGRES_SCHEMA_SILVER": POSTGRES_SCHEMA_SILVER,
    "POSTGRES_SCHEMA_GOLD": POSTGRES_SCHEMA_GOLD,
}

_missing_optional = [k for k, v in _optional.items() if not v]

if _missing_optional:
    warnings.warn(
        "Not set (only needed if you use the bronze/silver/gold schemas): "
        f"{', '.join(_missing_optional)}",
        stacklevel=2,
    )

# Soft-required: only needed by scripts that actually connect to Databricks.
# Missing values here don't stop the Postgres/Mongo pipeline from running,
# but get_databricks_connection() in connection.py will fail with a clear
# error the moment it's actually called without them.
_optional_databricks = {
    "DATABRICKS_HOST": DATABRICKS_HOST,
    "DATABRICKS_HTTP_PATH": DATABRICKS_HTTP_PATH,
    "DATABRICKS_TOKEN": DATABRICKS_TOKEN,
    "DATABRICKS_CATALOG": DATABRICKS_CATALOG,
    "DATABRICKS_SCHEMA": DATABRICKS_SCHEMA,
}

_missing_databricks = [k for k, v in _optional_databricks.items() if not v]

if _missing_databricks:
    warnings.warn(
        "Not set (only needed if you connect to Databricks): "
        f"{', '.join(_missing_databricks)}",
        stacklevel=2,
    )