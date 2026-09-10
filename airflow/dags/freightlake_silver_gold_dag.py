"""FreightLake silver gold DAG — dbt build downstream of bronze."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow import DAG

default_args = {
    "owner": "freightlake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="freightlake_silver_gold_dag",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["freightlake", "silver", "gold"],
    max_active_runs=1,
) as dag:
    wait_bronze = ExternalTaskSensor(
        task_id="wait_bronze",
        external_dag_id="freightlake_bronze_dag",
        external_task_id=None,
        timeout=7200,
        poke_interval=60,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/dbt && dbt snapshot --profiles-dir . --target dev",
    )

    gx_validate = BashOperator(
        task_id="gx_validate_silver",
        bash_command="cd /opt/airflow && uv run python -m great_expectations checkpoint run silver_checkpoint",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt && dbt build --profiles-dir . --target dev",
    )

    wait_bronze >> dbt_snapshot >> gx_validate >> dbt_build
