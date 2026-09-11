"""
freightlake_bronze.py
====================
Bronze DAG: Sources -> Bronze Delta with parallel watermark ingestion.

Tasks perform in parallel (per request):
  PG_OLTP -- PySpark JDBC watermark updated_at --> BRONZE  (bronze_pg)
  MONGO -- PySpark connector watermark event_ts --> BRONZE (bronze_mongo)

Both bronze tasks run in parallel, downstream silver is triggered via
TriggerDagRunOperator or schedule after success.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_ROOT = Path(__file__).resolve().parents[2]

default_args = {
    "owner": "freightlake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="freightlake_bronze",
    description="Bronze ingestion: PG_OLTP + MONGO watermark to Delta in parallel",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["freightlake", "bronze", "pg", "mongo"],
    doc_md=__doc__,
    max_active_tasks=2,
) as dag:

    bronze_pg = BashOperator(
        task_id="bronze_pg",
        bash_command="uv run python -m src.jobs.pg_extract_incremental --target-schema bronze",
        cwd=str(REPO_ROOT),
        doc_md="PG_OLTP -- PySpark JDBC watermark updated_at --> BRONZE",
        execution_timeout=timedelta(hours=1),
    )

    bronze_mongo = BashOperator(
        task_id="bronze_mongo",
        bash_command="uv run python -m src.jobs.mongo_extract_incremental --target-schema bronze",
        cwd=str(REPO_ROOT),
        doc_md="MONGO -- PySpark connector watermark event_ts --> BRONZE",
        execution_timeout=timedelta(hours=1),
    )

    # Explicit parallel: no dependency between PG and MONGO, both start together
    # If you view Graph, they are side by side.
    [bronze_pg, bronze_mongo]
