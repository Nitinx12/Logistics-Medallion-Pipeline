"""
freightlake_gold.py
==================
Gold DAG: Silver -> Gold star -> PG_MART

  SILVER -- dbt star schema --> GOLD -- GX gate --> PySpark publish --> PG_MART --> BI

Chained after the silver DAG via ExternalTaskSensor on gx_silver, so gold
never builds before silver passed its quality gate.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow import DAG


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
    dag_id="freightlake_gold",
    description="Gold star + PG Mart publish per diagram",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["freightlake", "gold", "mart", "dbt"],
    doc_md=__doc__,
) as dag:
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="freightlake_silver",
        external_task_id="gx_silver",
        poke_interval=60,
        timeout=timedelta(hours=2),
        mode="reschedule",
        doc_md="Wait for silver plus its GX gate before building gold.",
    )

    gold = BashOperator(
        task_id="gold",
        bash_command="cd dbt && uv run dbt build --select tag:gold",
        cwd=str(REPO_ROOT),
        doc_md="SILVER -- dbt star schema --> GOLD",
    )

    # No --demo fallback on purpose: a failed Postgres validation must fail
    # this task, otherwise the quality gate can never block promotion.
    gx_gold = BashOperator(
        task_id="gx_gold",
        bash_command="uv run python gx/run_validations.py --postgres --layer gold",
        cwd=str(REPO_ROOT),
        doc_md="GX gate for all gold rules, fails the DAG on error severity",
    )

    # BashOperator + the job's own CLI instead of importing the job into the
    # scheduler process: the scheduler interpreter is the container's system
    # Python, which does not have pyspark. uv run uses the project venv.
    mart_publish = BashOperator(
        task_id="mart_publish",
        bash_command="uv run python -m src.jobs.publish_gold_to_postgres",
        cwd=str(REPO_ROOT),
        doc_md="GOLD -- PySpark publish --> PG_MART (serving mart for BI)",
        execution_timeout=timedelta(hours=1),
    )

    wait_for_silver >> gold >> gx_gold >> mart_publish
