"""
pg_extract_incremental.py
==========================
Incremental Postgres -> Databricks (Unity Catalog) extraction job.

Reads Postgres table(s) via Spark JDBC (jars/postgresql-42.7.3.jar) and writes
them straight into Unity Catalog-managed Delta tables using Spark's native UC
catalog plugin (jars/unitycatalog-client, unitycatalog-hadoop,
delta-kernel-unitycatalog, delta-spark, delta-storage). No Databricks SQL
warehouse or cluster is involved -- this writes directly to the storage that
Unity Catalog manages, and Databricks sees the tables immediately because UC
is the source of truth.

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
        description="Incrementally extract Postgres table(s) into Unity Catalog Delta table(s)."
    )
    p.add_argument("--table", default=None, help="Source table, e.g. 'orders' or 'public.orders'. Omit to process every table in --source-schema.")
    p.add_argument("--source-schema", default="public", help="Postgres schema to read from / discover tables in (default: public)")
    p.add_argument("--exclude-tables", default="", help="Comma-separated table names to skip when running against all tables")

    p.add_argument("--target-catalog", default=getattr(config, "DATABRICKS_CATALOG", None), help="Unity Catalog catalog name (default: DATABRICKS_CATALOG env)")
    p.add_argument("--target-schema", default="bronze", help="Target schema (default: bronze)")
    p.add_argument("--target-table", default=None, help="Target table name, single-table mode only (default: same as source table)")

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
    )


# ---------------------------------------------------------------------------
# Spark session: JDBC (Postgres) + Delta + Unity Catalog REST catalog
# ---------------------------------------------------------------------------
def build_spark_session(catalog_name: str) -> SparkSession:
    jar_paths = sorted(str(p) for p in JARS_DIR.glob("*.jar"))
    if not jar_paths:
        raise SystemExit(f"No jars found in {JARS_DIR}. Expected the Postgres/Delta/Unity Catalog jars there.")

    host = config.DATABRICKS_HOST or ""
    if not host:
        raise SystemExit("DATABRICKS_HOST is not set - required to reach the Unity Catalog REST API.")
    uc_uri = host if host.startswith("http") else f"https://{host}"
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


def verify_uc_catalog(spark: SparkSession, catalog_name: str) -> None:
    """Fail fast, once, with a short readable message instead of a giant Java
    stack trace repeated for every table if the catalog plugin can't load."""
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
    url = f"jdbc:postgresql://{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DATABASE}"
    if getattr(config, "POSTGRES_SSLMODE", None):
        url += f"?sslmode={config.POSTGRES_SSLMODE}"
    props = {
        "user": config.POSTGRES_USERNAME,
        "password": config.POSTGRES_PASSWORD,
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
# Watermark + chunk planning
# ---------------------------------------------------------------------------
def target_table_exists(spark: SparkSession, target_fqtn: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {target_fqtn}")
        return True
    except Exception:
        return False


def get_source_bounds(spark: SparkSession, jdbc_url: str, props: dict, source_fqtn: str, updated_at_col: str):
    query = f"(SELECT MIN({updated_at_col}) AS min_ts, MAX({updated_at_col}) AS max_ts FROM {source_fqtn}) AS bounds"
    row = spark.read.jdbc(jdbc_url, query, properties=props).collect()[0]
    return row["min_ts"], row["max_ts"]


def resolve_watermark(spark: SparkSession, job: JobConfig) -> Optional[datetime]:
    if job.force_full:
        return None
    if job.since_override:
        return job.since_override
    if not target_table_exists(spark, job.target_fqtn):
        return None
    row = spark.sql(f"SELECT MAX({job.updated_at_col}) AS wm FROM {job.target_fqtn}").collect()[0]
    return row["wm"]


def build_windows(start: datetime, end: datetime, chunk_days: float) -> list[tuple[datetime, datetime, bool]]:
    """Returns (window_start, window_end, is_first) triples covering [start, end]."""
    if start >= end:
        return []

    windows = []
    cur = start
    step = timedelta(days=chunk_days)
    first = True
    while cur < end:
        window_end = min(cur + step, end)
        windows.append((cur, window_end, first))
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


def run_incremental_table(spark: SparkSession, jdbc_url: str, props: dict, job: JobConfig, progress: Progress) -> dict:
    """Watermarked, chunked extract + load for one table. Used for both single-table and bulk runs."""
    t0 = datetime.now()
    table_exists = target_table_exists(spark, job.target_fqtn)
    watermark = resolve_watermark(spark, job)
    min_ts, max_ts = get_source_bounds(spark, jdbc_url, props, job.source_fqtn, job.updated_at_col)

    if max_ts is None:
        return {"table": job.source_fqtn, "kind": "EMPTY", "rows": 0, "chunks": 0, "status": "no rows in source", "elapsed": datetime.now() - t0}

    start = watermark if watermark is not None else min_ts
    end = max_ts
    load_kind = "FULL" if watermark is None else "INCREMENTAL"

    windows = build_windows(start, end, job.chunk_days)
    if not windows:
        return {"table": job.source_fqtn, "kind": load_kind, "rows": 0, "chunks": 0, "status": "up to date", "elapsed": datetime.now() - t0}

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
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(job.target_fqtn)
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

    with console.status("[cyan]Starting Spark session...", spinner="dots"):
        spark = build_spark_session(args.target_catalog)
    verify_uc_catalog(spark, args.target_catalog)
    log.info("Spark session ready")

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
        + f"[bold]target[/bold]  {args.target_catalog}.{args.target_schema}  (unity catalog)\n"
        + f"[bold]mode[/bold]    {args.mode}" + (f"  (key: {args.key_column})" if args.mode == "merge" else "") + "\n"
        + f"[bold]window[/bold]  {args.chunk_days}d chunks, fetchsize={args.fetch_size}, partitions={args.num_partitions}"
    )
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