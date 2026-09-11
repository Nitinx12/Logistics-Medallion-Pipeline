"""
freightlake_silver.py
====================
Silver DAG: Bronze -> Silver (cleaned + SCD2).

Triggered after bronze DAG succeeds via Airflow scheduling or TriggerDagRunOperator.

  BRONZE -- dbt build --> SILVER
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
    dag_id="freightlake_silver",
    description="Silver: Bronze Delta -> cleaned Silver via dbt",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["freightlake", "silver", "dbt"],
    doc_md=__doc__,
) as dag:

    silver = BashOperator(
        task_id="silver",
        bash_command="cd dbt && dbt build --select tag:silver --profiles-dir .",
        cwd=str(REPO_ROOT),
        doc_md="BRONZE -- dbt build --> SILVER",
    )

    gx_silver = BashOperator(
        task_id="gx_silver",
        bash_command="uv run python gx/run_validations.py --suite silver.customers --postgres || uv run python gx/run_validations.py --suite silver.customers --demo",
        cwd=str(REPO_ROOT),
        doc_md="GX check for silver rules",
    )

    silver >> gx_silver
