"""FreightLake bronze DAG — parallel extracts, daily, SLA 6 AM."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

default_args = {
    "owner": "freightlake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),  # must finish by 6 AM per PROJECT_PLAN.md:191
}

with DAG(
    dag_id="freightlake_bronze_dag",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 4 * * *",
    catchup=False,
    default_args=default_args,
    tags=["freightlake", "bronze"],
    max_active_runs=1,
) as dag:
    extract_postgres = BashOperator(
        task_id="extract_postgres_oltp",
        bash_command="python /opt/airflow/spark_jobs/bronze/extract_postgres_oltp.py",
        sla=timedelta(hours=2),
    )

    extract_mongo = BashOperator(
        task_id="extract_mongo_tracking",
        bash_command="python /opt/airflow/spark_jobs/bronze/extract_mongo_tracking.py",
        sla=timedelta(hours=2),
    )

    [extract_postgres, extract_mongo]  # noqa: B018  # parallel bronze extracts
