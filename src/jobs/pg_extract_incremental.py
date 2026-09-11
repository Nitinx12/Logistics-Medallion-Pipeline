"""
pg_extract_incremental.py
==========================
Incremental Postgres -> Databricks (Unity Catalog) extraction job.

Reads Postgres table(s) via Spark JDBC (jars/postgresql-42.7.3.jar) and writes
them into Delta tables. Supports multiple write strategies controlled by
BRONZE_WRITE_MODE in .env:

  uc_managed  -> direct Unity Catalog managed writes via UCSingleCatalog
                (only works inside Databricks compute; outside it fails with
                403 ErrorCode 5108/5105 and UNITY_CATALOG_EXTERNAL_CREATE_TABLE_REQUEST_FOR_NON_EXTERNAL_TABLE_DENIED)
  warehouse   -> Databricks SQL warehouse via databricks-sql-connector (default,
                works from outside; verified with CREATE TABLE / INSERT on
                freightlake.bronze)
  local       -> local filesystem Delta under BRONZE_LOCAL_PATH (no UC, for
                offline dev, writes to ./spark-warehouse/bronze)
  uc_external -> Unity Catalog external tables via DATABRICKS_EXTERNAL_LOCATION

The original design assumed uc_managed from any host, but Databricks now
blocks managed-table creation and getTableCredentials from outside compute for
security. This module keeps uc_managed for in-Databricks runs but defaults to
warehouse for local runs and auto-falls back on 403.

Single table vs. all tables
----------------------------
    --table orders       -> extracts just `orders`
    (no --table)          -> discovers every base table in --source-schema
                              (default: public) and extracts all of them into
                              --target-schema (default: bronze)

Incrementality
--------------
Each table is watermarked independently on an "updated at" column
(default: updated_at):

    1. If the target Delta table already exists, the watermark is
       MAX(updated_at) read straight out of that table -- no separate
       state file to maintain or lose.
    2. If it doesn't exist yet (or --full is passed), it's a full load
       starting from MIN(updated_at) in the source.
    3. The upper bound for the run is MAX(updated_at) in the source,
       captured once up front, so every chunk reads a single consistent
       snapshot instead of a moving target.

Tables that don't have the watermark column at all (lookup/reference
tables, etc.) are handled per --no-updated-at-mode: 'overwrite' (default)
does a full snapshot replace every run, 'skip' leaves them alone.

The [start, end] range for a table is split into fixed-size time windows
(--chunk-days) and pulled + written one window at a time ("chunking and
batch processing"). Within a window, --num-partitions > 1 additionally asks
Spark's JDBC reader to split that window across parallel connections via
partitionColumn/lowerBound/upperBound, and --fetch-size controls how many
rows the JDBC driver pulls per network round trip.

Usage
-----
    # Everything in `public` -> bronze, one call
    python -m src.jobs.pg_extract_incremental

    # Just one table, append-only incremental, auto-detecting watermark
    python -m src.jobs.pg_extract_incremental --table orders

    # Explicit source schema + target location + upsert on primary key
    python -m src.jobs.pg_extract_incremental \
        --table public.orders \
        --target-catalog main --target-schema bronze --target-table orders \
        --mode merge --key-column order_id

    # All tables, but skip a couple and force a full reload
    python -m src.jobs.pg_extract_incremental --exclude-tables alembic_version,schema_migrations --full

    # See what would happen without writing anything
    python -m src.jobs.pg_extract_incremental --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Quiet the terminal down BEFORE pyspark is imported: py4j/log4j start
# chattering the moment the JVM gateway spins up.
# ---------------------------------------------------------------------------
warnings.filterwarnings("ignore")
os.environ.setdefault("PYTHONWARNINGS", "ignore")
os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.progress import (  # noqa: E402
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table  # noqa: E402

# ---------------------------------------------------------------------------
# Project imports. Supports being run either as part of the `src` package
# (python -m src.jobs.pg_extract_incremental) or as a standalone script
# (python src/jobs/pg_extract_incremental.py) from anywhere in the repo.
# ---------------------------------------------------------------------------
try:
    from ..utils import engine as config
    from ..utils.logger import get_logger
except ImportError:  # pragma: no cover - fallback for direct script execution
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from src.utils import engine as config
    from src.utils.logger import get_logger

REPO_ROOT = Path(__file__).resolve().parents[2]
JARS_DIR = REPO_ROOT / "jars"
LOG4J_CONFIG = REPO_ROOT / "config" / "log4j2.properties"

console = Console()
log = get_logger("pg_extract_incremental", console_level=logging.WARNING)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Incrementally extract Postgres table(s) into Delta table(s)."
    )
    p.add_argument("--table", default=None, help="Source table, e.g. 'orders' or 'public.orders'. Omit to process every table in --source-schema.")
    p.add_argument("--source-schema", default="public", help="Postgres schema to read from / discover tables in (default: public)")
    p.add_argument("--exclude-tables", default="", help="Comma-separated table names to skip when running against all tables")

    p.add_argument("--target-catalog", default=getattr(config, "DATABRICKS_CATALOG", None), help="Unity Catalog catalog name (default: DATABRICKS_CATALOG env)")
    p.add_argument("--target-schema", default="bronze", help="Target schema (default: bronze)")
    p.add_argument("--target-table", default=None, help="Target table name, single-table mode only (default: same as source table)")
    p.add_argument("--write-mode", choices=["auto", "warehouse", "local", "uc_managed", "uc_external"], default="auto", help="Bronze write strategy: auto picks BRONZE_WRITE_MODE env (warehouse default) and falls back from uc_managed on 403; warehouse uses SQL warehouse, local writes to BRONZE_LOCAL_PATH, uc_managed/uc_external use UCSingleCatalog")

    p.add_argument("--updated-at-column", default="updated_at", help="Watermark column (default: updated_at)")
    p.add_argument("--no-updated-at-mode", choices=["skip", "overwrite"], default="overwrite", help="What to do with tables lacking --updated-at-column when running against all tables: full snapshot replace, or skip (default: overwrite)")
    p.add_argument("--mode", choices=["append", "merge"], default="append", help="append = bronze-style insert log; merge = upsert on --key-column (single-table mode only)")
    p.add_argument("--key-column", default=None, help="Primary key column, required when --mode merge")

    p.add_argument("--chunk-days", type=float, default=1.0, help="Size of each incremental time window in days (default: 1)")
    p.add_argument("--fetch-size", type=int, default=10_000, help="JDBC fetchsize per round trip (default: 10000)")
    p.add_argument("--num-partitions", type=int, default=1, help="Parallel JDBC connections per chunk, partitioned on the watermark column (default: 1)")

    p.add_argument("--since", default=None, help="Override the watermark, ISO format e.g. 2026-01-01T00:00:00")
    p.add_argument("--full", action="store_true", help="Force a full reload, ignoring the existing watermark")
    p.add_argument("--dry-run", action="store_true", help="Compute chunks and row counts but write nothing")

    return p.parse_args()


@dataclass
class JobConfig:
    source_fqtn: str          # schema.table in Postgres
    target_fqtn: str          # catalog.schema.table in Unity Catalog
    updated_at_col: str
    mode: str
    key_column: Optional[str]
    chunk_days: float
    fetch_size: int
    num_partitions: int
    since_override: Optional[datetime]
    force_full: bool
    dry_run: bool
    write_mode: str = "warehouse"


def build_job_config(args: argparse.Namespace, table_name: str, target_table_override: Optional[str] = None) -> JobConfig:
    source_fqtn = table_name if "." in table_name else f"{args.source_schema}.{table_name}"

    target_table = target_table_override or table_name.split(".")[-1]
    if not args.target_catalog or not args.target_schema:
        raise SystemExit(
            "Target catalog/schema not set. Pass --target-catalog/--target-schema "
            "or set DATABRICKS_CATALOG in .env"
        )
    target_fqtn = f"{args.target_catalog}.{args.target_schema}.{target_table}"

    if args.mode == "merge" and not args.key_column:
        raise SystemExit("--mode merge requires --key-column")

    since_override = datetime.fromisoformat(args.since) if args.since else None

    return JobConfig(
        source_fqtn=source_fqtn,
        target_fqtn=target_fqtn,
        updated_at_col=args.updated_at_column,
        mode=args.mode,
        key_column=args.key_column,
        chunk_days=args.chunk_days,
        fetch_size=args.fetch_size,
        num_partitions=max(1, args.num_partitions),
        since_override=since_override,
        force_full=args.full,
        dry_run=args.dry_run,
        write_mode=resolve_write_mode(getattr(args, "write_mode", "auto")),
    )


# ---------------------------------------------------------------------------
# Spark session: JDBC (Postgres) + Delta + Unity Catalog REST catalog
# ---------------------------------------------------------------------------
def resolve_write_mode(cli_mode: str) -> str:
    """Resolve CLI --write-mode with BRONZE_WRITE_MODE env. 'auto' picks env."""
    if cli_mode and cli_mode != "auto":
        return cli_mode.lower()
    env_mode = getattr(config, "BRONZE_WRITE_MODE", "warehouse").strip().lower()
    if env_mode not in {"warehouse", "local", "uc_managed", "uc_external", "auto"}:
        return "warehouse"
    return env_mode if env_mode != "auto" else "warehouse"


def _is_uc_managed_blocked(e: Exception) -> bool:
    text = str(e)
    blocked_markers = [
        "ErrorCode: 5108",
        "ErrorCode: 5105",
        "Permission denied on table",
        "createStagingTable",
        "getTableCredentials",
        "UNITY_CATALOG_EXTERNAL_CREATE_TABLE_REQUEST_FOR_NON_EXTERNAL_TABLE_DENIED",
        "from outside of Databricks Unity Catalog enabled compute environment",
    ]
    return any(m in text for m in blocked_markers)


def build_spark_session(catalog_name: str, write_mode: str = "warehouse") -> SparkSession:
    jar_paths = sorted(str(p) for p in JARS_DIR.glob("*.jar"))
    if not jar_paths:
        raise SystemExit(f"No jars found in {JARS_DIR}. Expected the Postgres/Delta/Unity Catalog jars there.")

    # local mode does not need Unity Catalog at all
    if write_mode == "local":
        builder = (
            SparkSession.builder.appName("pg_extract_incremental")
            .config("spark.jars", ",".join(jar_paths))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.session.timeZone", "UTC")
        )
        if LOG4J_CONFIG.exists():
            builder = builder.config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile={LOG4J_CONFIG.as_uri()}")
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    host = config.DATABRICKS_HOST or ""
    if not host:
        raise SystemExit("DATABRICKS_HOST is not set - required to reach the Unity Catalog REST API.")
    host = host.strip().rstrip("/")
    # For Databricks workspaces (dbc-*.cloud.databricks.com) the UC client's base URI is the workspace host itself
    # (e.g. https://dbc-xxx.cloud.databricks.com). The client appends /api/2.1/unity-catalog internally.
    # For OSS UC (e.g. http://localhost:8080/api/2.1/unity-catalog) the caller passes the full suffix already.
    if "cloud.databricks.com" in host:
        uc_uri = host if host.startswith("http") else f"https://{host}"
    else:
        # OSS Unity Catalog: ensure the suffix is present if caller gave only host:port
        uc_uri = host if host.startswith("http") else f"https://{host}"
        if "/api/2.1/unity-catalog" not in uc_uri:
            uc_uri = uc_uri.rstrip("/") + "/api/2.1/unity-catalog"

    builder = (
        SparkSession.builder.appName("pg_extract_incremental")
        .config("spark.jars", ",".join(jar_paths))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(f"spark.sql.catalog.{catalog_name}", "io.unitycatalog.spark.UCSingleCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.uri", uc_uri)
        .config(f"spark.sql.catalog.{catalog_name}.token", config.DATABRICKS_TOKEN or "")
        .config("spark.sql.defaultCatalog", catalog_name)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )

    if LOG4J_CONFIG.exists():
        log4j_opt = f"-Dlog4j.configurationFile={LOG4J_CONFIG.as_uri()}"
        builder = builder.config("spark.driver.extraJavaOptions", log4j_opt)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def verify_uc_catalog(spark: SparkSession, catalog_name: str, write_mode: str = "warehouse") -> None:
    """Fail fast, once, with a short readable message instead of a giant Java
    stack trace repeated for every table if the catalog plugin can't load."""
    if write_mode in {"local", "warehouse"}:
        # warehouse/local do not require UC catalog access from Spark
        return
    try:
        spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
    except Exception as e:
        msg = str(e)
        if "ClassNotFoundException" in msg:
            raise SystemExit(
                "Could not load the Unity Catalog Spark plugin "
                "(io.unitycatalog.spark.UCSingleCatalog).\n"
                "That class ships in its own jar, 'unitycatalog-spark_4.2_2.13-0.6.0.jar' "
                f"-- it is NOT part of unitycatalog-client or unitycatalog-hadoop, and it isn't in {JARS_DIR}.\n"
                "Download a version matching your other unitycatalog-*-0.6.0 jars from:\n"
                "  https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-spark_4.2_2.13/\n"
                f"and drop the jar into {JARS_DIR}, then rerun."
            )
        raise SystemExit(f"Could not reach Unity Catalog catalog '{catalog_name}': {short_error(e)}")


def short_error(e: Exception, limit: int = 220) -> str:
    """Most informative line of an exception, truncated -- keeps the rich summary table readable
    instead of dumping a full JVM stack trace into a table cell. Full details still
    go to the log file via log.exception()."""
    text = str(e).strip()
    if not text:
        return type(e).__name__
    # Prefer the line with the actual API error (403, listSchemas, ApiException) over the generic Py4J header
    for line in text.splitlines():
        if "ApiException" in line or "listSchemas" in line or "403" in line or "401" in line or "404" in line:
            return line.strip()[:limit] + ("…" if len(line.strip()) > limit else "")
    first_line = text.splitlines()[0].strip()
    return first_line if len(first_line) <= limit else first_line[: limit - 1] + "…"


def postgres_jdbc_options() -> tuple[str, dict]:
    # Prefer OLTP split DB when defined per docker/init/01_create_database.sql
    database = getattr(config, "POSTGRES_OLTP_DATABASE", None) or config.POSTGRES_DATABASE
    user = getattr(config, "POSTGRES_OLTP_USER", None) or config.POSTGRES_USERNAME
    password = getattr(config, "POSTGRES_OLTP_PASSWORD", None) or config.POSTGRES_PASSWORD
    url = f"jdbc:postgresql://{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{database}"
    if getattr(config, "POSTGRES_SSLMODE", None):
        url += f"?sslmode={config.POSTGRES_SSLMODE}"
    props = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }
    return url, props


# ---------------------------------------------------------------------------
# Table discovery
# ---------------------------------------------------------------------------
def discover_tables(spark: SparkSession, jdbc_url: str, props: dict, schema: str, exclude: set[str]) -> list[str]:
    query = (
        "(SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' "
        "ORDER BY table_name) AS t"
    )
    rows = spark.read.jdbc(jdbc_url, query, properties=props).collect()
    return [r["table_name"] for r in rows if r["table_name"] not in exclude]


def table_has_column(spark: SparkSession, jdbc_url: str, props: dict, schema: str, table: str, column: str) -> bool:
    query = (
        "(SELECT 1 AS present FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name = '{column}') AS c"
    )
    return spark.read.jdbc(jdbc_url, query, properties=props).count() > 0


# ---------------------------------------------------------------------------
# Helpers for alternative write modes (warehouse / local)
# ---------------------------------------------------------------------------
def _local_delta_path(table_fqtn: str) -> Path:
    # freightlake.bronze.drivers -> <BRONZE_LOCAL_PATH>/drivers
    table = table_fqtn.split(".")[-1]
    base = Path(getattr(config, "BRONZE_LOCAL_PATH", "./spark-warehouse/bronze"))
    return base / table


def _warehouse_table_exists(target_fqtn: str) -> bool:
    try:
        from ..utils.connections import get_databricks_connection
    except ImportError:
        from src.utils.connections import get_databricks_connection  # type: ignore
    conn = get_databricks_connection()
    catalog, schema, table = target_fqtn.split(".")
    with conn.cursor() as cur:
        cur.execute(f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'")
        return len(cur.fetchall()) > 0


def _warehouse_max_watermark(target_fqtn: str, col: str):
    try:
        from ..utils.connections import get_databricks_connection
    except ImportError:
        from src.utils.connections import get_databricks_connection  # type: ignore
    conn = get_databricks_connection()
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX({col}) AS wm FROM {target_fqtn}")
        row = cur.fetchone()
        if row is None:
            return None
        # databricks-sql returns Row or tuple
        try:
            return row[0] if isinstance(row, (list, tuple)) else row["wm"]  # type: ignore
        except Exception:
            return getattr(row, "wm", None)


def _local_table_exists(target_fqtn: str) -> bool:
    return (_local_delta_path(target_fqtn) / "_delta_log").exists()


def _local_max_watermark(spark: SparkSession, target_fqtn: str, col: str):
    path = str(_local_delta_path(target_fqtn))
    try:
        df = spark.read.format("delta").load(path)
        row = df.selectExpr(f"MAX({col}) AS wm").collect()[0]
        return row["wm"]
    except Exception:
        return None


def _spark_type_to_sql(t) -> str:
    from pyspark.sql.types import (
        StringType, IntegerType, LongType, DoubleType, FloatType,
        BooleanType, TimestampType, DateType, DecimalType, BinaryType,
    )
    if isinstance(t, StringType):
        return "STRING"
    if isinstance(t, IntegerType):
        return "INT"
    if isinstance(t, LongType):
        return "BIGINT"
    if isinstance(t, (DoubleType, FloatType)):
        return "DOUBLE"
    if isinstance(t, BooleanType):
        return "BOOLEAN"
    if isinstance(t, TimestampType):
        return "TIMESTAMP"
    if isinstance(t, DateType):
        return "DATE"
    if isinstance(t, DecimalType):
        return f"DECIMAL({t.precision},{t.scale})"
    if isinstance(t, BinaryType):
        return "BINARY"
    return "STRING"


def _write_via_warehouse(df: DataFrame, target_fqtn: str, mode: str, key_col: Optional[str] = None) -> None:
    """Create managed table via warehouse and insert rows. Works from outside."""
    import pandas as pd
    import math
    try:
        from ..utils.connections import get_databricks_connection
    except ImportError:
        from src.utils.connections import get_databricks_connection  # type: ignore

    def _sql_literal(v):
        # Handle pandas NA / numpy nan / NaT
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return "NULL"
        try:
            if pd.isna(v):
                return "NULL"
        except Exception:
            pass
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        # pandas Timestamp, datetime, date
        if isinstance(v, (datetime, pd.Timestamp)):
            # pandas Timestamp to isoformat
            try:
                return f"'{pd.Timestamp(v).isoformat()}'"
            except Exception:
                return f"'{str(v)}'"
        # bytes
        if isinstance(v, (bytes, bytearray)):
            return "'" + v.hex() + "'"
        # bool must be before int
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        return str(v)

    # Collect to pandas for type-stable inserts
    pdf = df.toPandas()
    if pdf.empty:
        return
    conn = get_databricks_connection()
    # Build CREATE TABLE IF NOT EXISTS from Spark schema
    cols_ddl = ", ".join(f"`{f.name}` {_spark_type_to_sql(f.dataType)}" for f in df.schema.fields)
    with conn.cursor() as cur:
        # Use USING DELTA without LOCATION -> managed table in UC (allowed via warehouse)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_fqtn} ({cols_ddl}) USING DELTA")
        # For merge, we need to use SQL MERGE via temp view simulated as VALUES
        # Simpler: pandas iteration with executemany. For append, bulk insert.
        if mode == "merge" and key_col and key_col in pdf.columns:
            # Create temp view as VALUES and MERGE
            # Build VALUES clause in batches to avoid huge SQL
            for start in range(0, len(pdf), 2000):
                batch = pdf.iloc[start:start+2000]
                # Build column list
                cols = ", ".join(f"`{c}`" for c in pdf.columns)
                # Build rows VALUES
                rows_sql = []
                for _, r in batch.iterrows():
                    vals = [_sql_literal(v) for v in r]
                    rows_sql.append("(" + ", ".join(vals) + ")")
                values_clause = ", ".join(rows_sql)
                # Use MERGE with inline table
                merge_sql = f"""
                MERGE INTO {target_fqtn} AS t
                USING (SELECT * FROM VALUES {values_clause} AS s({cols})) AS s
                ON t.`{key_col}` = s.`{key_col}`
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
                cur.execute(merge_sql)
        else:
            # Append: executemany via single INSERT VALUES batch (2000 rows per round trip)
            cols = ", ".join(f"`{c}`" for c in pdf.columns)
            for start in range(0, len(pdf), 2000):
                batch = pdf.iloc[start:start+2000]
                rows_sql = []
                for _, r in batch.iterrows():
                    vals = [_sql_literal(v) for v in r]
                    rows_sql.append("(" + ", ".join(vals) + ")")
                values_clause = ", ".join(rows_sql)
                cur.execute(f"INSERT INTO {target_fqtn} ({cols}) VALUES {values_clause}")


def _write_local_delta(spark: SparkSession, df: DataFrame, target_fqtn: str, mode: str, table_exists: bool) -> None:
    path = str(_local_delta_path(target_fqtn))
    if not table_exists:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        return
    if mode == "append":
        df.write.format("delta").mode("append").save(path)
        return
    # merge not implemented for local; fallback to append


# ---------------------------------------------------------------------------
# Watermark + chunk planning
# ---------------------------------------------------------------------------
def target_table_exists(spark: SparkSession, target_fqtn: str, write_mode: str = "warehouse") -> bool:
    if write_mode == "local":
        return _local_table_exists(target_fqtn)
    if write_mode == "warehouse":
        try:
            return _warehouse_table_exists(target_fqtn)
        except Exception:
            # fallback to Spark UC check
            pass
    # Use catalog API instead of DESCRIBE to avoid [TABLE_OR_VIEW_NOT_FOUND] ERROR logs
    # Spark logs DESCRIBE failures at ERROR via SQLQueryContextLogger even when we catch the exception
    try:
        parts = target_fqtn.split(".")
        if len(parts) == 3:
            catalog, schema, table = parts
            # If default catalog is set, tableExists can check schema.table; otherwise query UC directly
            try:
                return spark.catalog.tableExists(f"{schema}.{table}")
            except Exception:
                # Fallback: UC catalog show tables (no ERROR log on miss)
                return spark.sql(f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'").count() > 0
        return spark.catalog.tableExists(target_fqtn)
    except Exception:
        return False


def _coerce_datetime(value, context: str) -> Optional[datetime]:
    """Bounds and watermarks should come back from Spark as real datetimes,
    but if the underlying column is actually text/varchar rather than a
    native timestamp/timestamptz type, Spark infers StringType and hands
    back a plain Python str instead -- which then blows up later with a
    cryptic 'str has no attribute isoformat' deep inside a query builder.
    Normalize either shape here, with a clear error if it's neither."""
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as e:
            raise ValueError(f"Could not parse {context} value {value!r} as an ISO timestamp: {e}") from e
    raise TypeError(f"Unexpected type {type(value).__name__} for {context}: {value!r}")


def get_source_bounds(spark: SparkSession, jdbc_url: str, props: dict, source_fqtn: str, updated_at_col: str):
    query = f"(SELECT MIN({updated_at_col}) AS min_ts, MAX({updated_at_col}) AS max_ts FROM {source_fqtn}) AS bounds"
    row = spark.read.jdbc(jdbc_url, query, properties=props).collect()[0]
    min_ts = _coerce_datetime(row["min_ts"], f"{source_fqtn}.{updated_at_col} MIN")
    max_ts = _coerce_datetime(row["max_ts"], f"{source_fqtn}.{updated_at_col} MAX")
    return min_ts, max_ts


def resolve_watermark(spark: SparkSession, job: JobConfig) -> Optional[datetime]:
    if job.force_full:
        return None
    if job.since_override:
        return job.since_override
    if not target_table_exists(spark, job.target_fqtn, job.write_mode):
        return None
    try:
        if job.write_mode == "local":
            wm = _local_max_watermark(spark, job.target_fqtn, job.updated_at_col)
            return _coerce_datetime(wm, f"{job.target_fqtn}.{job.updated_at_col} watermark")
        if job.write_mode == "warehouse":
            wm = _warehouse_max_watermark(job.target_fqtn, job.updated_at_col)
            return _coerce_datetime(wm, f"{job.target_fqtn}.{job.updated_at_col} watermark")
        row = spark.sql(f"SELECT MAX({job.updated_at_col}) AS wm FROM {job.target_fqtn}").collect()[0]
        return _coerce_datetime(row["wm"], f"{job.target_fqtn}.{job.updated_at_col} watermark")
    except Exception:
        # If watermark query fails (e.g. table not yet created via warehouse), treat as full load
        return None


def build_windows(
    start: datetime, end: datetime, chunk_days: float, full_load: bool
) -> list[tuple[datetime, datetime, bool]]:
    """Returns (window_start, window_end, is_first) triples covering [start, end].

    full_load distinguishes two different meanings of `start`:
      - FULL load (watermark is None): `start` is MIN(updated_at) from the
        source -- a real boundary value that has never been captured, so it
        must be included.
      - INCREMENTAL run (watermark is an existing MAX(updated_at) already
        sitting in the target table): `start` is a value that was already
        captured *inclusively* on a previous run, so it must be excluded
        this time to avoid re-appending the same rows.

    start == end is a real, non-empty case on a FULL load: it means every
    row in the (unloaded) table shares the exact same timestamp -- common
    with batch-seeded/bulk-inserted data where updated_at is one literal
    value rather than set per-row. Without this branch, build_windows used
    to return [] here and the table would be reported "up to date" with 0
    rows despite never having been loaded at all.

    start == end on an INCREMENTAL run, by contrast, genuinely means no new
    rows have landed since the last run -- correctly returns [] so the
    already-loaded tied-timestamp rows aren't re-appended every run.
    """
    if start > end:
        return []
    if start == end:
        return [(start, end, True)] if full_load else []

    windows = []
    cur = start
    step = timedelta(days=chunk_days)
    first = True
    while cur < end:
        window_end = min(cur + step, end)
        # Only the very first window of a FULL load gets an inclusive lower
        # bound; the first window of an INCREMENTAL run must stay exclusive
        # since `start` (the watermark) was already captured last run.
        windows.append((cur, window_end, first and full_load))
        cur = window_end
        first = False
    return windows


# ---------------------------------------------------------------------------
# Extraction + load
# ---------------------------------------------------------------------------
def read_window(
    spark: SparkSession,
    jdbc_url: str,
    props: dict,
    job: JobConfig,
    window_start: datetime,
    window_end: datetime,
    inclusive_start: bool,
) -> DataFrame:
    op = ">=" if inclusive_start else ">"
    predicate = (
        f"{job.updated_at_col} {op} '{window_start.isoformat()}' "
        f"AND {job.updated_at_col} <= '{window_end.isoformat()}'"
    )
    query = f"(SELECT * FROM {job.source_fqtn} WHERE {predicate}) AS chunk"

    read_props = dict(props)
    read_props["fetchsize"] = str(job.fetch_size)

    if job.num_partitions > 1:
        return spark.read.jdbc(
            jdbc_url,
            query,
            column=job.updated_at_col,
            lowerBound=window_start.isoformat(),
            upperBound=window_end.isoformat(),
            numPartitions=job.num_partitions,
            properties=read_props,
        )
    return spark.read.jdbc(jdbc_url, query, properties=read_props)


def write_chunk(spark: SparkSession, df: DataFrame, job: JobConfig, table_exists: bool) -> None:
    # Route to alternative writers first when configured
    if job.write_mode == "local":
        _write_local_delta(spark, df, job.target_fqtn, job.mode, table_exists)
        return
    if job.write_mode == "warehouse":
        _write_via_warehouse(df, job.target_fqtn, job.mode, job.key_column)
        return
    # uc_managed / uc_external: try native UC path, fall back on 403
    try:
        if not table_exists:
            df.write.format("delta").mode("append").saveAsTable(job.target_fqtn)
            return
        if job.mode == "append":
            df.write.format("delta").mode("append").saveAsTable(job.target_fqtn)
            return
        # merge / upsert
        tmp_view = "_pg_extract_incremental_chunk"
        df.createOrReplaceTempView(tmp_view)
        spark.sql(
            f"""
            MERGE INTO {job.target_fqtn} AS t
            USING {tmp_view} AS s
            ON t.{job.key_column} = s.{job.key_column}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
        spark.catalog.dropTempView(tmp_view)
    except Exception as e:
        if job.write_mode in {"uc_managed", "uc_external"} or _is_uc_managed_blocked(e):
            log.warning(f"UC managed write blocked for {job.target_fqtn} ({short_error(e)}). Falling back to warehouse writes. Set BRONZE_WRITE_MODE=warehouse or local to avoid this.")
            try:
                _write_via_warehouse(df, job.target_fqtn, job.mode, job.key_column)
                return
            except Exception as we:
                log.exception(f"Warehouse fallback also failed for {job.target_fqtn}")
                raise we from e
        raise


def run_incremental_table(spark: SparkSession, jdbc_url: str, props: dict, job: JobConfig, progress: Progress) -> dict:
    """Watermarked, chunked extract + load for one table. Used for both single-table and bulk runs."""
    t0 = datetime.now()
    table_exists = target_table_exists(spark, job.target_fqtn, job.write_mode)
    watermark = resolve_watermark(spark, job)
    min_ts, max_ts = get_source_bounds(spark, jdbc_url, props, job.source_fqtn, job.updated_at_col)

    if max_ts is None:
        return {"table": job.source_fqtn, "kind": "EMPTY", "rows": 0, "chunks": 0, "status": "no rows in source", "elapsed": datetime.now() - t0}

    start = watermark if watermark is not None else min_ts
    end = max_ts
    load_kind = "FULL" if watermark is None else "INCREMENTAL"

    windows = build_windows(start, end, job.chunk_days, full_load=(load_kind == "FULL"))
    if not windows:
        return {"table": job.source_fqtn, "kind": load_kind, "rows": 0, "chunks": 0, "status": "up to date", "elapsed": datetime.now() - t0}

    # For warehouse/local full loads, truncate first to make --full idempotent (otherwise append duplicates)
    if load_kind == "FULL" and table_exists and not job.dry_run:
        if job.write_mode == "warehouse":
            try:
                from ..utils.connections import get_databricks_connection
            except ImportError:
                from src.utils.connections import get_databricks_connection  # type: ignore
            try:
                conn = get_databricks_connection()
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {job.target_fqtn}")
                    log.info(f"Truncated {job.target_fqtn} for FULL reload (warehouse)")
            except Exception as e:
                log.warning(f"Could not truncate {job.target_fqtn} before FULL reload: {e}")
                # fallback: drop and recreate will be handled by writer
        elif job.write_mode == "local":
            try:
                import shutil
                p = _local_delta_path(job.target_fqtn)
                if p.exists():
                    shutil.rmtree(p)
                    log.info(f"Removed local Delta path {p} for FULL reload")
            except Exception as e:
                log.warning(f"Could not clear local path for {job.target_fqtn}: {e}")
            table_exists = False

    task = progress.add_task(f"{job.source_fqtn} ({load_kind.lower()})", total=len(windows))
    total_rows = 0
    for window_start, window_end, is_first in windows:
        df = read_window(spark, jdbc_url, props, job, window_start, window_end, is_first)
        if job.dry_run:
            total_rows += df.count()
        else:
            df = df.persist()
            row_count = df.count()
            if row_count > 0:
                write_chunk(spark, df, job, table_exists)
                table_exists = True
            df.unpersist()
            total_rows += row_count
        progress.advance(task)
    progress.remove_task(task)

    return {
        "table": job.source_fqtn,
        "kind": load_kind,
        "rows": total_rows,
        "chunks": len(windows),
        "status": "dry-run" if job.dry_run else "written",
        "elapsed": datetime.now() - t0,
    }


def run_full_snapshot_table(spark: SparkSession, jdbc_url: str, props: dict, job: JobConfig, progress: Progress) -> dict:
    """Simple full-replace load for tables with no watermark column (e.g. lookup tables)."""
    t0 = datetime.now()
    task = progress.add_task(f"{job.source_fqtn} (snapshot, no {job.updated_at_col})", total=1)

    read_props = dict(props)
    read_props["fetchsize"] = str(job.fetch_size)
    query = f"(SELECT * FROM {job.source_fqtn}) AS full_snapshot"
    df = spark.read.jdbc(jdbc_url, query, properties=read_props)

    if job.dry_run:
        row_count = df.count()
        status = "dry-run"
    else:
        df = df.persist()
        row_count = df.count()
        if job.write_mode == "local":
            path = str(_local_delta_path(job.target_fqtn))
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        elif job.write_mode == "warehouse":
            try:
                from ..utils.connections import get_databricks_connection
            except ImportError:
                from src.utils.connections import get_databricks_connection  # type: ignore
            conn = get_databricks_connection()
            # For snapshot overwrite: ensure table exists, truncate, then insert
            # Create table first if needed by helper, then truncate
            catalog, schema, table = job.target_fqtn.split(".")
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {job.target_fqtn}")
            except Exception:
                pass  # table may not exist yet
            _write_via_warehouse(df, job.target_fqtn, "append")
        else:
            try:
                df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(job.target_fqtn)
            except Exception as e:
                if _is_uc_managed_blocked(e):
                    _write_via_warehouse(df, job.target_fqtn, "append")
                else:
                    raise
        df.unpersist()
        status = "overwritten"

    progress.advance(task)
    progress.remove_task(task)
    return {"table": job.source_fqtn, "kind": "SNAPSHOT", "rows": row_count, "chunks": 1, "status": status, "elapsed": datetime.now() - t0}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()

    if args.table is None and args.mode == "merge":
        raise SystemExit(
            "--mode merge isn't supported when running against all tables (primary keys differ per table). "
            "Pass --table to target a single table for merge/upsert."
        )

    write_mode = resolve_write_mode(getattr(args, "write_mode", "auto"))
    # Validate warehouse mode needs warehouse credentials
    if write_mode == "warehouse" and not getattr(config, "DATABRICKS_TOKEN", None):
        console.print("[yellow]BRONZE_WRITE_MODE=warehouse but DATABRICKS_TOKEN is not set. Falling back to local.[/yellow]")
        write_mode = "local"

    with console.status(f"[cyan]Starting Spark session (write_mode={write_mode})...", spinner="dots"):
        spark = build_spark_session(args.target_catalog, write_mode=write_mode)
    verify_uc_catalog(spark, args.target_catalog, write_mode=write_mode)
    log.info(f"Spark session ready (write_mode={write_mode})")

    jdbc_url, props = postgres_jdbc_options()
    exclude = {t.strip() for t in args.exclude_tables.split(",") if t.strip()}

    bulk_mode = args.table is None
    if bulk_mode:
        with console.status(f"[cyan]Discovering tables in '{args.source_schema}'...", spinner="dots"):
            table_names = discover_tables(spark, jdbc_url, props, args.source_schema, exclude)
        if not table_names:
            console.print(f"[yellow]No tables found in schema '{args.source_schema}'.[/yellow]")
            spark.stop()
            return
        if args.target_table:
            console.print("[yellow]--target-table is ignored when running against all tables.[/yellow]")
    else:
        table_names = [args.table]

    header = (
        (f"[bold]tables[/bold]  {len(table_names)} discovered in '{args.source_schema}'\n" if bulk_mode
         else f"[bold]source[/bold]  {table_names[0] if '.' in table_names[0] else f'{args.source_schema}.{table_names[0]}'}\n")
        + f"[bold]target[/bold]  {args.target_catalog}.{args.target_schema}  (write_mode={write_mode})\n"
        + f"[bold]mode[/bold]    {args.mode}" + (f"  (key: {args.key_column})" if args.mode == "merge" else "") + "\n"
        + f"[bold]window[/bold]  {args.chunk_days}d chunks, fetchsize={args.fetch_size}, partitions={args.num_partitions}"
    )
    if write_mode == "warehouse":
        header += "\n[dim]warehouse writes via Databricks SQL (managed tables from outside) – avoids 403 ErrorCode 5108/5105[/dim]"
    elif write_mode == "local":
        header += f"\n[dim]local Delta at {getattr(config, 'BRONZE_LOCAL_PATH', './spark-warehouse/bronze')} – no UC, no 403[/dim]"
    elif write_mode == "uc_managed":
        header += "\n[dim]UC managed via UCSingleCatalog – only works inside Databricks compute[/dim]"
    console.print(Panel.fit(header, title="pg_extract_incremental" + (" - all tables" if bulk_mode else ""), border_style="cyan"))
    if bulk_mode:
        console.print(f"[dim]{', '.join(table_names)}[/dim]")

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    )

    results = []
    with progress:
        overall = progress.add_task("[bold]overall[/bold]", total=len(table_names)) if bulk_mode else None
        for name in table_names:
            job = build_job_config(args, name, target_table_override=args.target_table if not bulk_mode else None)
            try:
                has_watermark_col = table_has_column(spark, jdbc_url, props, args.source_schema, name.split(".")[-1], job.updated_at_col)
                if has_watermark_col:
                    result = run_incremental_table(spark, jdbc_url, props, job, progress)
                elif args.no_updated_at_mode == "overwrite":
                    result = run_full_snapshot_table(spark, jdbc_url, props, job, progress)
                else:
                    result = {"table": job.source_fqtn, "kind": "SKIPPED", "rows": 0, "chunks": 0, "status": f"no {job.updated_at_col} column", "elapsed": timedelta(0)}
            except Exception as e:
                log.exception(f"Failed extracting {job.source_fqtn}")  # full traceback goes to the log file
                result = {"table": job.source_fqtn, "kind": "ERROR", "rows": 0, "chunks": 0, "status": f"error: {short_error(e)}", "elapsed": timedelta(0)}
            results.append(result)
            if overall is not None:
                progress.advance(overall)

    summary = Table(title="Run summary")
    summary.add_column("table")
    summary.add_column("type")
    summary.add_column("rows", justify="right")
    summary.add_column("chunks", justify="right")
    summary.add_column("status")
    summary.add_column("elapsed", justify="right")

    total_rows = 0
    for r in results:
        total_rows += r["rows"]
        summary.add_row(r["table"], r["kind"], f"{r['rows']:,}", str(r["chunks"]), r["status"], f"{r['elapsed'].total_seconds():.1f}s")

    console.print(summary)
    console.print(f"[bold green]Done.[/bold green] {total_rows:,} rows across {len(results)} table(s).")

    log.info(f"pg_extract_incremental complete: {len(results)} table(s), {total_rows} rows, dry_run={args.dry_run}")
    spark.stop()


if __name__ == "__main__":
    main()