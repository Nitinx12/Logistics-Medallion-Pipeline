"""
freightlake_bronze.py
=====================
Bronze DAG: Sources -> Bronze Delta with parallel watermark ingestion.

Tasks perform in parallel (per request):
  PG_OLTP -- PySpark JDBC watermark updated_at --> BRONZE  (bronze_pg)
  MONGO -- PySpark connector watermark event_ts --> BRONZE (bronze_mongo)

Both bronze tasks run in parallel, downstream silver is triggered via
ExternalTaskSensor in freightlake_silver after success.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


def _repo_root() -> Path:
    """Repo root that works locally and inside the Airflow container.

    Locally this file lives at <repo>/airflow/dags/freightlake_bronze.py so
    parents[2] is the repo root. In docker/compose.yml the same file is
    mounted at /opt/airflow/dags/ and the jobs live under /opt/airflow, so
    parents[2] would wrongly resolve to /opt. Walk up until the marker
    directories are found instead of trusting a fixed number of levels.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "src" / "jobs").is_dir() and (parent / "dbt" / "dbt_project.yml").is_file():
            return parent
    return here.parents[2]


REPO_ROOT = _repo_root()

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
