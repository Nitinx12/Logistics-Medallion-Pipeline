"""
mongo_extract_incremental.py
=============================
Incremental MongoDB -> Databricks (Delta) extraction job.

Reads MongoDB collection(s) via Spark Mongo connector (jars/mongo-spark-connector_*.jar,
mongodb-driver-sync) and writes them into Delta tables. Supports multiple write
strategies controlled by BRONZE_WRITE_MODE in .env (same as pg_extract_incremental):

  warehouse   -> Databricks SQL warehouse via databricks-sql-connector (default, works outside)
  local       -> local filesystem Delta under BRONZE_LOCAL_PATH
  uc_managed  -> direct Unity Catalog managed writes via UCSingleCatalog (only inside Databricks)
  uc_external -> Unity Catalog external tables via DATABRICKS_EXTERNAL_LOCATION

Collections discovered from MONGO_DB via MongoClient.list_collection_names().

Watermarking mirrors pg_extract_incremental: each collection is watermarked on
an "updated_at" field (default: updated_at):

    1. If target Delta table exists, watermark is MAX(updated_at) from that table.
    2. If not exists or --full, full load from MIN(updated_at) in source.
    3. Upper bound is MAX(updated_at) in source captured once.

Collections lacking the watermark column are handled per --no-updated-at-mode.

Chunking splits [start, end] into --chunk-days windows. Within a window
Spark reads via mongo connector with $match on updated_at range, then writes
via warehouse/local/uc path.

Usage
-----
    python -m src.jobs.mongo_extract_incremental
    python -m src.jobs.mongo_extract_incremental --collection delivery_events
    python -m src.jobs.mongo_extract_incremental --write-mode warehouse --full
    python -m src.jobs.mongo_extract_incremental --dry-run
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

try:
    from ..utils import engine as config
    from ..utils.logger import get_logger
except ImportError:  # pragma: no cover
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from src.utils import engine as config
    from src.utils.logger import get_logger

REPO_ROOT = Path(__file__).resolve().parents[2]
JARS_DIR = REPO_ROOT / "jars"
LOG4J_CONFIG = REPO_ROOT / "config" / "log4j2.properties"

console = Console()
log = get_logger("mongo_extract_incremental", console_level=logging.WARNING)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Incrementally extract MongoDB collection(s) into Delta table(s).")
    p.add_argument("--collection", default=None, help="Source collection, e.g. 'delivery_events'. Omit to process every collection in MONGO_DB.")
    p.add_argument("--database", default=getattr(config, "MONGO_DB", None), help="MongoDB database (default: MONGO_DB env)")
    p.add_argument("--exclude-collections", default="", help="Comma-separated collection names to skip when running against all collections")
    p.add_argument("--target-catalog", default=getattr(config, "DATABRICKS_CATALOG", None), help="Unity Catalog catalog name (default: DATABRICKS_CATALOG env)")
    p.add_argument("--target-schema", default="bronze", help="Target schema (default: bronze)")
    p.add_argument("--target-table", default=None, help="Target table name, single-collection mode only (default: same as source collection)")
    p.add_argument("--write-mode", choices=["auto", "warehouse", "local", "uc_managed", "uc_external"], default="auto", help="Bronze write strategy: auto picks BRONZE_WRITE_MODE env (warehouse default)")
    p.add_argument("--updated-at-column", default="updated_at", help="Watermark field (default: updated_at)")
    p.add_argument("--no-updated-at-mode", choices=["skip", "overwrite"], default="overwrite", help="What to do with collections lacking watermark field (default: overwrite)")
    p.add_argument("--mode", choices=["append", "merge"], default="append", help="append = bronze insert log; merge = upsert on --key-column (single-collection only)")
    p.add_argument("--key-column", default=None, help="Primary key field, required when --mode merge")
    p.add_argument("--chunk-days", type=float, default=1.0, help="Size of each incremental time window in days (default: 1)")
    p.add_argument("--fetch-size", type=int, default=10000, help="Not used for Mongo but kept for parity")
    p.add_argument("--num-partitions", type=int, default=1, help="Not used for Mongo but kept for parity")
    p.add_argument("--since", default=None, help="Override watermark, ISO format e.g. 2026-01-01T00:00:00")
    p.add_argument("--full", action="store_true", help="Force full reload, ignoring watermark")
    p.add_argument("--dry-run", action="store_true", help="Compute chunks and row counts but write nothing")
    return p.parse_args()


@dataclass
class JobConfig:
    source_collection: str
    target_fqtn: str
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


def build_job_config(args: argparse.Namespace, coll_name: str, target_table_override: Optional[str] = None) -> JobConfig:
    source_fqtn = coll_name
    target_table = target_table_override or coll_name
    if not args.target_catalog or not args.target_schema:
        raise SystemExit("Target catalog/schema not set. Pass --target-catalog/--target-schema or set DATABRICKS_CATALOG in .env")
    target_fqtn = f"{args.target_catalog}.{args.target_schema}.{target_table}"
    if args.mode == "merge" and not args.key_column:
        raise SystemExit("--mode merge requires --key-column")
    since_override = datetime.fromisoformat(args.since) if args.since else None
    return JobConfig(
        source_collection=source_fqtn,
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
# Spark session + helpers (mirrors pg_extract_incremental)
# ---------------------------------------------------------------------------
def resolve_write_mode(cli_mode: str) -> str:
    if cli_mode and cli_mode != "auto":
        return cli_mode.lower()
    env_mode = getattr(config, "BRONZE_WRITE_MODE", "warehouse").strip().lower()
    if env_mode not in {"warehouse", "local", "uc_managed", "uc_external", "auto"}:
        return "warehouse"
    return env_mode if env_mode != "auto" else "warehouse"


def _is_uc_managed_blocked(e: Exception) -> bool:
    text = str(e)
    blocked = [
        "ErrorCode: 5108",
        "ErrorCode: 5105",
        "Permission denied on table",
        "createStagingTable",
        "getTableCredentials",
        "UNITY_CATALOG_EXTERNAL_CREATE_TABLE_REQUEST_FOR_NON_EXTERNAL_TABLE_DENIED",
        "from outside of Databricks Unity Catalog enabled compute environment",
    ]
    return any(m in text for m in blocked)


def build_spark_session(catalog_name: str, write_mode: str = "warehouse") -> SparkSession:
    jar_paths = sorted(str(p) for p in JARS_DIR.glob("*.jar"))
    if not jar_paths:
        raise SystemExit(f"No jars found in {JARS_DIR}.")
    if write_mode == "local":
        builder = (
            SparkSession.builder.appName("mongo_extract_incremental")
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
        raise SystemExit("DATABRICKS_HOST is not set.")
    host = host.strip().rstrip("/")
    if "cloud.databricks.com" in host:
        uc_uri = host if host.startswith("http") else f"https://{host}"
    else:
        uc_uri = host if host.startswith("http") else f"https://{host}"
        if "/api/2.1/unity-catalog" not in uc_uri:
            uc_uri = uc_uri.rstrip("/") + "/api/2.1/unity-catalog"
    builder = (
        SparkSession.builder.appName("mongo_extract_incremental")
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
        builder = builder.config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile={LOG4J_CONFIG.as_uri()}")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def verify_uc_catalog(spark: SparkSession, catalog_name: str, write_mode: str = "warehouse") -> None:
    if write_mode in {"local", "warehouse"}:
        return
    try:
        spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
    except Exception as e:
        msg = str(e)
        if "ClassNotFoundException" in msg:
            raise SystemExit(f"Could not load UCSingleCatalog jar. Check {JARS_DIR}")
        raise SystemExit(f"Could not reach catalog '{catalog_name}': {short_error(e)}")


def short_error(e: Exception, limit: int = 220) -> str:
    text = str(e).strip()
    if not text:
        return type(e).__name__
    for line in text.splitlines():
        if "ApiException" in line or "403" in line or "401" in line or "404" in line:
            return line.strip()[:limit] + ("…" if len(line.strip()) > limit else "")
    first = text.splitlines()[0].strip()
    return first if len(first) <= limit else first[: limit - 1] + "…"


# ---------------------------------------------------------------------------
# Mongo helpers
# ---------------------------------------------------------------------------
def discover_collections(mongo_db, exclude: set[str]) -> list[str]:
    cols = mongo_db.list_collection_names()
    return sorted([c for c in cols if c not in exclude and not c.startswith("system.")])

def collection_has_field(mongo_db, coll: str, field: str) -> bool:
    return mongo_db[coll].find_one({field: {"$exists": True}}) is not None

def _local_delta_path(table_fqtn: str) -> Path:
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
    from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, TimestampType, DateType, DecimalType, BinaryType
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
    import pandas as pd, math
    try:
        from ..utils.connections import get_databricks_connection
    except ImportError:
        from src.utils.connections import get_databricks_connection  # type: ignore
    def _sql_literal(v):
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return "NULL"
        try:
            if pd.isna(v):
                return "NULL"
        except Exception:
            pass
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        if isinstance(v, (datetime, pd.Timestamp)):
            try:
                return f"'{pd.Timestamp(v).isoformat()}'"
            except Exception:
                return f"'{str(v)}'"
        if isinstance(v, (bytes, bytearray)):
            return "'" + v.hex() + "'"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        return str(v)
    pdf = df.toPandas()
    if pdf.empty:
        return
    conn = get_databricks_connection()
    cols_ddl = ", ".join(f"`{f.name}` {_spark_type_to_sql(f.dataType)}" for f in df.schema.fields)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_fqtn} ({cols_ddl}) USING DELTA")
        if mode == "merge" and key_col and key_col in pdf.columns:
            for start in range(0, len(pdf), 2000):
                batch = pdf.iloc[start:start+2000]
                cols = ", ".join(f"`{c}`" for c in pdf.columns)
                rows_sql = []
                for _, r in batch.iterrows():
                    vals = [_sql_literal(v) for v in r]
                    rows_sql.append("(" + ", ".join(vals) + ")")
                values_clause = ", ".join(rows_sql)
                merge_sql = f"""
                MERGE INTO {target_fqtn} AS t
                USING (SELECT * FROM VALUES {values_clause} AS s({cols})) AS s
                ON t.`{key_col}` = s.`{key_col}`
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
                cur.execute(merge_sql)
        else:
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

# ---------------------------------------------------------------------------
# Watermark + chunk planning (reused)
# ---------------------------------------------------------------------------
def target_table_exists(spark: SparkSession, target_fqtn: str, write_mode: str = "warehouse") -> bool:
    if write_mode == "local":
        return _local_table_exists(target_fqtn)
    if write_mode == "warehouse":
        try:
            return _warehouse_table_exists(target_fqtn)
        except Exception:
            pass
    try:
        parts = target_fqtn.split(".")
        if len(parts) == 3:
            catalog, schema, table = parts
            try:
                return spark.catalog.tableExists(f"{schema}.{table}")
            except Exception:
                return spark.sql(f"SHOW TABLES IN {catalog}.{schema} LIKE '{table}'").count() > 0
        return spark.catalog.tableExists(target_fqtn)
    except Exception:
        return False

def _coerce_datetime(value, context: str) -> Optional[datetime]:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as e:
            raise ValueError(f"Could not parse {context} value {value!r}: {e}") from e
    raise TypeError(f"Unexpected type {type(value).__name__} for {context}: {value!r}")

def get_source_bounds_mongo(spark: SparkSession, mongo_uri: str, mongo_db: str, coll: str, updated_at_col: str):
    # Use pymongo directly – avoids mongo-spark-connector 10.4.0 incompatibility with Spark 4.2
    # (NoSuchMethodError: ExpressionEncoder.resolveAndBind)
    from pymongo import MongoClient
    db = MongoClient(mongo_uri)[mongo_db]
    pipeline = [{"$group": {"_id": None, "min_ts": {"$min": f"${updated_at_col}"}, "max_ts": {"$max": f"${updated_at_col}"}}}]
    docs = list(db[coll].aggregate(pipeline))
    if not docs:
        return None, None
    min_ts = _coerce_datetime(docs[0].get("min_ts"), f"{coll}.{updated_at_col} MIN")
    max_ts = _coerce_datetime(docs[0].get("max_ts"), f"{coll}.{updated_at_col} MAX")
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
        return None

def build_windows(start: datetime, end: datetime, chunk_days: float, full_load: bool) -> list[tuple[datetime, datetime, bool]]:
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
        windows.append((cur, window_end, first and full_load))
        cur = window_end
        first = False
    return windows

# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
def read_window_mongo(spark: SparkSession, mongo_uri: str, mongo_db: str, job: JobConfig, window_start: datetime, window_end: datetime, inclusive_start: bool) -> DataFrame:
    import pandas as pd
    from pymongo import MongoClient
    from bson import ObjectId
    op = "$gte" if inclusive_start else "$gt"
    client = MongoClient(mongo_uri)
    coll = client[mongo_db][job.source_collection]
    cursor = coll.find(
        {job.updated_at_col: {op: window_start.isoformat(), "$lte": window_end.isoformat()}},
        {"_id": 0}  # exclude _id to avoid ObjectId struct issues; keep as string if needed
    )
    docs = list(cursor)
    # Convert _id to string if present when we included it – here we excluded, so add if needed elsewhere
    # If collection uses _id as business key, keep it; otherwise no _id needed in bronze
    if not docs:
        # Return empty DF with inferred schema from one sample doc via pymongo
        sample = coll.find_one(projection={"_id": 0})
        if sample is None:
            sample = {}
        # Create empty pandas with same columns
        pdf = pd.DataFrame([sample]).iloc[0:0]
        return spark.createDataFrame(pdf)
    pdf = pd.DataFrame(docs)
    # Ensure updated_at stays string for watermark
    return spark.createDataFrame(pdf)

def write_chunk(spark: SparkSession, df: DataFrame, job: JobConfig, table_exists: bool) -> None:
    if job.write_mode == "local":
        _write_local_delta(spark, df, job.target_fqtn, job.mode, table_exists)
        return
    if job.write_mode == "warehouse":
        _write_via_warehouse(df, job.target_fqtn, job.mode, job.key_column)
        return
    try:
        if not table_exists:
            df.write.format("delta").mode("append").saveAsTable(job.target_fqtn)
            return
        if job.mode == "append":
            df.write.format("delta").mode("append").saveAsTable(job.target_fqtn)
            return
        tmp_view = "_mongo_extract_chunk"
        df.createOrReplaceTempView(tmp_view)
        spark.sql(f"""
        MERGE INTO {job.target_fqtn} AS t
        USING {tmp_view} AS s
        ON t.{job.key_column} = s.{job.key_column}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """)
        spark.catalog.dropTempView(tmp_view)
    except Exception as e:
        if job.write_mode in {"uc_managed", "uc_external"} or _is_uc_managed_blocked(e):
            log.warning(f"UC managed write blocked for {job.target_fqtn} ({short_error(e)}). Falling back to warehouse.")
            try:
                _write_via_warehouse(df, job.target_fqtn, job.mode, job.key_column)
                return
            except Exception as we:
                log.exception(f"Warehouse fallback also failed for {job.target_fqtn}")
                raise we from e
        raise

def run_incremental_collection(spark: SparkSession, mongo_uri: str, mongo_db: str, job: JobConfig, progress: Progress) -> dict:
    t0 = datetime.now()
    table_exists = target_table_exists(spark, job.target_fqtn, job.write_mode)
    watermark = resolve_watermark(spark, job)
    min_ts, max_ts = get_source_bounds_mongo(spark, mongo_uri, mongo_db, job.source_collection, job.updated_at_col)
    if max_ts is None:
        return {"table": job.source_collection, "kind": "EMPTY", "rows": 0, "chunks": 0, "status": "no rows in source", "elapsed": datetime.now() - t0}
    start = watermark if watermark is not None else min_ts
    end = max_ts
    load_kind = "FULL" if watermark is None else "INCREMENTAL"
    windows = build_windows(start, end, job.chunk_days, full_load=(load_kind == "FULL"))
    if not windows:
        return {"table": job.source_collection, "kind": load_kind, "rows": 0, "chunks": 0, "status": "up to date", "elapsed": datetime.now() - t0}
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
            except Exception as e:
                log.warning(f"Could not truncate {job.target_fqtn} before FULL: {e}")
        elif job.write_mode == "local":
            import shutil
            p = _local_delta_path(job.target_fqtn)
            if p.exists():
                shutil.rmtree(p)
            table_exists = False
    task = progress.add_task(f"{job.source_collection} ({load_kind.lower()})", total=len(windows))
    total_rows = 0
    for window_start, window_end, is_first in windows:
        df = read_window_mongo(spark, mongo_uri, mongo_db, job, window_start, window_end, is_first)
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
    return {"table": job.source_collection, "kind": load_kind, "rows": total_rows, "chunks": len(windows), "status": "dry-run" if job.dry_run else "written", "elapsed": datetime.now() - t0}

def run_full_snapshot_collection(spark: SparkSession, mongo_uri: str, mongo_db: str, job: JobConfig, progress: Progress) -> dict:
    t0 = datetime.now()
    task = progress.add_task(f"{job.source_collection} (snapshot, no {job.updated_at_col})", total=1)
    import pandas as pd
    from pymongo import MongoClient
    docs = list(MongoClient(mongo_uri)[mongo_db][job.source_collection].find({}, {"_id": 0}))
    pdf = pd.DataFrame(docs) if docs else pd.DataFrame()
    df = spark.createDataFrame(pdf) if not pdf.empty else spark.createDataFrame([], schema="dummy STRING")
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
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {job.target_fqtn}")
            except Exception:
                pass
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
    return {"table": job.source_collection, "kind": "SNAPSHOT", "rows": row_count, "chunks": 1, "status": status, "elapsed": datetime.now() - t0}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    if args.collection is None and args.mode == "merge":
        raise SystemExit("--mode merge isn't supported when running against all collections (primary keys differ). Pass --collection.")
    write_mode = resolve_write_mode(getattr(args, "write_mode", "auto"))
    if write_mode == "warehouse" and not getattr(config, "DATABRICKS_TOKEN", None):
        console.print("[yellow]BRONZE_WRITE_MODE=warehouse but DATABRICKS_TOKEN not set. Falling back to local.[/yellow]")
        write_mode = "local"
    with console.status(f"[cyan]Starting Spark session (write_mode={write_mode})...", spinner="dots"):
        spark = build_spark_session(args.target_catalog, write_mode=write_mode)
    verify_uc_catalog(spark, args.target_catalog, write_mode=write_mode)
    log.info(f"Spark session ready (write_mode={write_mode})")
    mongo_uri = config.MONGO_URI or ""
    mongo_db_name = args.database or config.MONGO_DB
    if not mongo_uri or not mongo_db_name:
        raise SystemExit("MONGO_URI / MONGO_DB not set. Check .env")
    from pymongo import MongoClient
    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]
    exclude = {t.strip() for t in args.exclude_collections.split(",") if t.strip()}
    bulk_mode = args.collection is None
    if bulk_mode:
        with console.status(f"[cyan]Discovering collections in '{mongo_db_name}'...", spinner="dots"):
            coll_names = discover_collections(db, exclude)
        if not coll_names:
            console.print(f"[yellow]No collections found in '{mongo_db_name}'.[/yellow]")
            spark.stop()
            return
        if args.target_table:
            console.print("[yellow]--target-table is ignored when running against all collections.[/yellow]")
    else:
        coll_names = [args.collection]
    header = (
        (f"[bold]collections[/bold]  {len(coll_names)} discovered in '{mongo_db_name}'\n" if bulk_mode else f"[bold]source[/bold]  {coll_names[0]}\n")
        + f"[bold]target[/bold]  {args.target_catalog}.{args.target_schema}  (write_mode={write_mode})\n"
        + f"[bold]mode[/bold]    {args.mode}" + (f"  (key: {args.key_column})" if args.mode == "merge" else "") + "\n"
        + f"[bold]window[/bold]  {args.chunk_days}d chunks"
    )
    if write_mode == "warehouse":
        header += "\n[dim]warehouse writes via Databricks SQL – avoids 403[/dim]"
    elif write_mode == "local":
        header += f"\n[dim]local Delta at {getattr(config, 'BRONZE_LOCAL_PATH', './spark-warehouse/bronze')}[/dim]"
    console.print(Panel.fit(header, title="mongo_extract_incremental" + (" - all collections" if bulk_mode else ""), border_style="cyan"))
    if bulk_mode:
        console.print(f"[dim]{', '.join(coll_names)}[/dim]")
    progress = Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn(), console=console)
    results = []
    with progress:
        overall = progress.add_task("[bold]overall[/bold]", total=len(coll_names)) if bulk_mode else None
        for name in coll_names:
            job = build_job_config(args, name, target_table_override=args.target_table if not bulk_mode else None)
            try:
                has_col = collection_has_field(db, name, job.updated_at_col)
                if has_col:
                    result = run_incremental_collection(spark, mongo_uri, mongo_db_name, job, progress)
                elif args.no_updated_at_mode == "overwrite":
                    result = run_full_snapshot_collection(spark, mongo_uri, mongo_db_name, job, progress)
                else:
                    result = {"table": name, "kind": "SKIPPED", "rows": 0, "chunks": 0, "status": f"no {job.updated_at_col} column", "elapsed": timedelta(0)}
            except Exception as e:
                log.exception(f"Failed extracting {name}")
                result = {"table": name, "kind": "ERROR", "rows": 0, "chunks": 0, "status": f"error: {short_error(e)}", "elapsed": timedelta(0)}
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
    console.print(f"[bold green]Done.[/bold green] {total_rows:,} rows across {len(results)} collection(s).")
    log.info(f"mongo_extract_incremental complete: {len(results)} collection(s), {total_rows} rows, dry_run={args.dry_run}")
    spark.stop()

if __name__ == "__main__":
    main()
