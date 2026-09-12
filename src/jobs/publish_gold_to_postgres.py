"""
publish_gold_to_postgres.py
===========================
Gold Delta -> Postgres serving mart publisher.

Reads Gold star schema from Databricks Unity Catalog (or local Delta via
BRONZE_WRITE_MODE=local fallback) via Spark and writes to Postgres
serving mart (PG_MART in the diagram) via JDBC (jars/postgresql-42.7.3.jar).

Diagram edge: GOLD -- PySpark publish --> PG_MART --> BI

Gold tables are materialized as tables (full refresh) in dbt, so publish
is also full overwrite per table (TRUNCATE + INSERT or overwrite).

Usage:
    python -m src.jobs.publish_gold_to_postgres --catalog freightlake --gold-schema gold --mart-schema gold
    python -m src.jobs.publish_gold_to_postgres --tables dim_customers,fact_loads --dry-run
    python -m src.jobs.publish_gold_to_postgres --write-mode local --local-gold-path ./spark-warehouse/bronze
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")
os.environ.setdefault("PYTHONWARNINGS", "ignore")
os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

from pyspark.sql import SparkSession  # noqa: E402
from rich.console import Console  # noqa: E402
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
log = get_logger("publish_gold_to_postgres")

# Gold tables as defined in dbt/models/gold/
DEFAULT_GOLD_TABLES = [
    "dim_customers",
    "dim_drivers",
    "dim_facilities",
    "dim_routes",
    "dim_trucks",
    "dim_trailers",
    "dim_date",
    "fact_loads",
    "fact_trips",
    "fact_fuel_purchases",
    "fact_delivery_events",
    "fact_maintenance_records",
    "fact_safety_incidents",
    "fact_operations",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Publish Gold Delta to Postgres serving mart.")
    p.add_argument(
        "--catalog",
        default=getattr(config, "DATABRICKS_CATALOG", "freightlake"),
        help="Source catalog for Gold (default: DATABRICKS_CATALOG)",
    )
    p.add_argument("--gold-schema", default="gold", help="Source gold schema (default: gold)")
    p.add_argument(
        "--mart-schema",
        default=getattr(config, "POSTGRES_SCHEMA_GOLD", "gold") or "gold",
        help="Target Postgres mart schema (default: POSTGRES_SCHEMA_GOLD or gold)",
    )
    p.add_argument(
        "--tables", default=None, help="Comma separated gold tables to publish, default all"
    )
    p.add_argument(
        "--write-mode",
        choices=["auto", "uc", "local"],
        default="auto",
        help="Read gold from UC or local Delta",
    )
    p.add_argument(
        "--local-gold-path",
        default=getattr(config, "BRONZE_LOCAL_PATH", "./spark-warehouse/bronze").replace(
            "bronze", "gold"
        ),
        help="Local gold path when write-mode=local",
    )
    p.add_argument(
        "--jdbc-batch-size",
        type=int,
        default=10000,
        help="JDBC batch size for fast publish (10000 for 10k+ rows)",
    )
    p.add_argument(
        "--repartition",
        type=int,
        default=4,
        help="Repartition gold DF before JDBC write for fast publish",
    )
    p.add_argument("--dry-run", action="store_true", help="Count rows and log, write nothing")
    p.add_argument(
        "--demo",
        action="store_true",
        help="Use synthetic gold data, no Delta or UC required, for local demo and CI",
    )
    return p.parse_args()


def build_spark_session(catalog_name: str, write_mode: str) -> SparkSession:
    jar_paths = sorted(str(p) for p in JARS_DIR.glob("*.jar"))
    if not jar_paths:
        raise SystemExit(f"No jars found in {JARS_DIR}")

    if write_mode == "local":
        builder = (
            SparkSession.builder.appName("publish_gold_to_postgres")
            .config("spark.jars", ",".join(jar_paths))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.session.timeZone", "UTC")
        )
    else:
        host = (config.DATABRICKS_HOST or "").strip().rstrip("/")
        if not host:
            raise SystemExit("DATABRICKS_HOST not set for UC read")
        if "cloud.databricks.com" in host:
            uc_uri = host if host.startswith("http") else f"https://{host}"
        else:
            uc_uri = host if host.startswith("http") else f"https://{host}"
            if "/api/2.1/unity-catalog" not in uc_uri:
                uc_uri = uc_uri.rstrip("/") + "/api/2.1/unity-catalog"
        builder = (
            SparkSession.builder.appName("publish_gold_to_postgres")
            .config("spark.jars", ",".join(jar_paths))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config(f"spark.sql.catalog.{catalog_name}", "io.unitycatalog.spark.UCSingleCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.uri", uc_uri)
            .config(f"spark.sql.catalog.{catalog_name}.token", config.DATABRICKS_TOKEN or "")
            .config("spark.sql.defaultCatalog", catalog_name)
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.session.timeZone", "UTC")
        )
    if LOG4J_CONFIG.exists():
        builder = builder.config(
            "spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile={LOG4J_CONFIG.as_uri()}"
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def postgres_jdbc_url() -> str:
    # Prefer MART split DB when defined, fallback to legacy single DB
    database = getattr(config, "POSTGRES_MART_DATABASE", None) or config.POSTGRES_DATABASE
    url = f"jdbc:postgresql://{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{database}"
    if getattr(config, "POSTGRES_SSLMODE", None):
        url += f"?sslmode={config.POSTGRES_SSLMODE}"
    return url


def postgres_props() -> dict:
    user = getattr(config, "POSTGRES_MART_USER", None) or config.POSTGRES_USERNAME
    password = getattr(config, "POSTGRES_MART_PASSWORD", None) or config.POSTGRES_PASSWORD
    return {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def _get_mart_engine():
    """SQLAlchemy engine for MART DB, prefers POSTGRES_MART_* split vars."""
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    host = config.POSTGRES_HOST
    port = config.POSTGRES_PORT
    database = getattr(config, "POSTGRES_MART_DATABASE", None) or config.POSTGRES_DATABASE
    user = getattr(config, "POSTGRES_MART_USER", None) or config.POSTGRES_USERNAME
    password = getattr(config, "POSTGRES_MART_PASSWORD", None) or config.POSTGRES_PASSWORD
    query = {}
    if getattr(config, "POSTGRES_SSLMODE", None):
        query["sslmode"] = config.POSTGRES_SSLMODE
    url = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
        query=query,
    )
    return create_engine(url)


def resolve_write_mode(cli_mode: str) -> str:
    if cli_mode != "auto":
        return "local" if cli_mode == "local" else "uc"
    # auto: local if BRONZE_WRITE_MODE is local
    env = getattr(config, "BRONZE_WRITE_MODE", "warehouse").strip().lower()
    return "local" if env == "local" else "uc"


def _demo_gold_df(spark: SparkSession, table: str):
    """Synthetic gold DataFrame for --demo, mirrors gx/demo_dataframe."""
    import pandas as pd

    if table == "dim_date":
        dates = pd.date_range("2020-01-01", "2020-01-05", freq="D")
        pdf = pd.DataFrame(
            {
                "date_key": dates.strftime("%Y%m%d").astype(int),
                "full_date": dates,
                "year": dates.year,
            }
        )
        return spark.createDataFrame(pdf)
    if table == "dim_customers":
        pdf = pd.DataFrame(
            {
                "customer_sk": ["sk1"],
                "customer_id": ["CUST_001"],
                "customer_name": ["Acme"],
                "is_current": [True],
            }
        )
        return spark.createDataFrame(pdf)
    if table.startswith("dim_"):
        pdf = pd.DataFrame({f"{table}_sk": ["sk1"], f"{table.replace('dim_', '')}_id": ["ID_001"]})
        return spark.createDataFrame(pdf)
    pdf = pd.DataFrame({"id": [1, 2], "table": [table, table]})
    return spark.createDataFrame(pdf)


def read_gold_table(
    spark: SparkSession,
    catalog: str,
    gold_schema: str,
    table: str,
    write_mode: str,
    local_path: str,
    demo: bool = False,
):
    if demo:
        return _demo_gold_df(spark, table)
    if write_mode == "local":
        path = Path(local_path) / table
        if not (path / "_delta_log").exists() and not path.exists():
            raise FileNotFoundError(
                f"Local gold Delta not found at {path}. Run dbt with --write-mode local or use --demo."
            )
        return spark.read.format("delta").load(str(path))
    # uc mode: check catalog availability gracefully
    try:
        return spark.table(f"{catalog}.{gold_schema}.{table}")
    except Exception as e:
        # bubble as FileNotFound-like for caller to handle as skipped
        raise FileNotFoundError(f"UC table not found {catalog}.{gold_schema}.{table}: {e}") from e


def publish_table(
    spark: SparkSession,
    table: str,
    catalog: str,
    gold_schema: str,
    mart_schema: str,
    write_mode: str,
    local_path: str,
    dry_run: bool,
    demo: bool = False,
    jdbc_batch_size: int = 10000,
    repartition: int = 4,
) -> dict:
    t0 = datetime.now()
    try:
        df = read_gold_table(spark, catalog, gold_schema, table, write_mode, local_path, demo=demo)
        # Fast publish: repartition for 10k+ rows to parallel JDBC
        if repartition and df.rdd.getNumPartitions() != repartition:
            try:
                df = df.repartition(repartition)
            except Exception:
                pass
        count = df.count()
        if dry_run:
            return {
                "table": table,
                "rows": count,
                "status": "dry-run" if not demo else "dry-run (demo)",
                "elapsed": (datetime.now() - t0).total_seconds(),
            }
        jdbc_url = postgres_jdbc_url()
        # Spark JDBC fast options: batchsize 10000, truncate false then overwrite handles 10k+ quickly
        props = postgres_props()
        props["batchsize"] = str(jdbc_batch_size)
        props["truncate"] = "true"
        if demo:
            try:
                from sqlalchemy import text

                engine = _get_mart_engine()
                with engine.connect() as conn:
                    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{mart_schema}"'))
                    conn.commit()
                    conn.close()
                    engine.dispose()
                target = f"{mart_schema}.{table}"
                df.write.jdbc(url=jdbc_url, table=target, mode="overwrite", properties=props)
                return {
                    "table": table,
                    "rows": count,
                    "status": "published (demo)",
                    "elapsed": (datetime.now() - t0).total_seconds(),
                }
            except Exception as e:
                log.warning(f"Demo mart publish skipped, postgres not reachable for {table}: {e}")
                return {
                    "table": table,
                    "rows": count,
                    "status": "demo: postgres not reachable, counted only",
                    "elapsed": (datetime.now() - t0).total_seconds(),
                }
        # Ensure mart schema exists in MART DB
        try:
            from sqlalchemy import text

            engine = _get_mart_engine()
            with engine.connect() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{mart_schema}"'))
                conn.commit()
                conn.close()
                engine.dispose()
        except Exception as e:
            log.warning(f"Could not ensure mart schema {mart_schema} in MART DB: {e}")

        target = f"{mart_schema}.{table}"
        df.write.jdbc(url=jdbc_url, table=target, mode="overwrite", properties=props)
        return {
            "table": table,
            "rows": count,
            "status": "published",
            "elapsed": (datetime.now() - t0).total_seconds(),
        }
    except FileNotFoundError as e:
        # Missing gold source is a skip, not an error traceback
        log.warning(f"Skipped {table}: {e}")
        return {
            "table": table,
            "rows": 0,
            "status": f"skipped: {str(e)[:140]}",
            "elapsed": (datetime.now() - t0).total_seconds(),
        }
    except Exception as e:
        log.exception(f"Failed to publish {table}")
        return {
            "table": table,
            "rows": 0,
            "status": f"error: {str(e)[:180]}",
            "elapsed": (datetime.now() - t0).total_seconds(),
        }


def main() -> None:
    args = parse_args()
    tables: list[str] = (
        [t.strip() for t in args.tables.split(",") if t.strip()]
        if args.tables
        else DEFAULT_GOLD_TABLES
    )
    write_mode = resolve_write_mode(args.write_mode)
    mode_label = f"{write_mode}" + (" + demo" if args.demo else "")
    console.print(
        f"[cyan]Publishing {len(tables)} gold tables {args.gold_schema} -> Postgres {args.mart_schema} (mode={mode_label})[/cyan]"
    )
    if args.dry_run:
        console.print("[yellow]dry-run: counting only[/yellow]")
    if args.demo:
        console.print("[yellow]demo: synthetic gold, no UC/Delta required[/yellow]")

    spark = build_spark_session(args.catalog, write_mode if not args.demo else "local")
    results = []
    for tbl in tables:
        console.print(f"Publishing {tbl}...")
        res = publish_table(
            spark,
            tbl,
            args.catalog,
            args.gold_schema,
            args.mart_schema,
            write_mode,
            args.local_gold_path,
            args.dry_run,
            demo=args.demo,
            jdbc_batch_size=args.jdbc_batch_size,
            repartition=args.repartition,
        )
        results.append(res)

    summary = Table(title="Publish summary PG_MART")
    summary.add_column("table")
    summary.add_column("rows", justify="right")
    summary.add_column("status")
    summary.add_column("elapsed", justify="right")
    total = 0
    for r in results:
        total += r["rows"]
        summary.add_row(r["table"], f"{r['rows']:,}", r["status"], f"{r['elapsed']:.1f}s")
    console.print(summary)
    console.print(
        f"[bold green]Done.[/bold green] {total:,} rows across {len(results)} tables -> {args.mart_schema}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
