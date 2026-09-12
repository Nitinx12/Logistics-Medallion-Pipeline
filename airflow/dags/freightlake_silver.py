"""
freightlake_silver.py
=====================
Silver DAG: Bronze -> Silver (cleaned + SCD2).

Chained after the bronze DAG via ExternalTaskSensor, so silver never builds
from a half finished bronze run.

  BRONZE -- dbt build --> SILVER -- GX gate --> (freightlake_gold waits on gx_silver)
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


def _repo_root() -> Path:
    """Repo root that works locally and inside the Airflow container.

    See freightlake_bronze._repo_root for why parents[2] alone is wrong once
    the dags folder is mounted at /opt/airflow/dags by docker/compose.yml.
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
    dag_id="freightlake_silver",
    description="Silver: Bronze Delta -> cleaned Silver via dbt",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["freightlake", "silver", "dbt"],
    doc_md=__doc__,
) as dag:

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="freightlake_bronze",
        external_task_ids=["bronze_pg", "bronze_mongo"],
        poke_interval=60,
        timeout=timedelta(hours=2),
        mode="reschedule",
        doc_md="Wait for both bronze extractions before building silver.",
    )

    silver = BashOperator(
        task_id="silver",
        bash_command="cd dbt && uv run dbt build --select tag:silver --profiles-dir .",
        cwd=str(REPO_ROOT),
        doc_md="BRONZE -- dbt build --> SILVER",
    )

    # No --demo fallback on purpose: a failed Postgres validation must fail
    # this task, otherwise the quality gate can never block promotion.
    gx_silver = BashOperator(
        task_id="gx_silver",
        bash_command="uv run python gx/run_validations.py --postgres --layer silver",
        cwd=str(REPO_ROOT),
        doc_md="GX gate for all silver rules, fails the DAG on error severity",
    )

    wait_for_bronze >> silver >> gx_silver
