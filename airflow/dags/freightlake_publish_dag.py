"""FreightLake publish DAG — gold to mart downstream of silver gold."""

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
    dag_id="freightlake_publish_dag",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 7 * * *",
    catchup=False,
    default_args=default_args,
    tags=["freightlake", "publish"],
    max_active_runs=1,
) as dag:
    wait_silver_gold = ExternalTaskSensor(
        task_id="wait_silver_gold",
        external_dag_id="freightlake_silver_gold_dag",
        external_task_id="dbt_build",
        timeout=7200,
        poke_interval=60,
    )

    publish = BashOperator(
        task_id="publish_gold_to_postgres",
        bash_command="cd /opt/airflow && python -m spark_jobs.publish.publish_gold_to_postgres",
    )

    wait_silver_gold >> publish
