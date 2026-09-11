"""
freightlake_gold.py
==================
Gold DAG: Silver -> Gold star -> PG_MART

  SILVER -- dbt star schema --> GOLD -- PySpark publish --> PG_MART --> BI
  + GX validation after Gold
  + Airflow orchestrates GOLD and PG_MART
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

REPO_ROOT = Path(__file__).resolve().parents[2]

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

    gold = BashOperator(
        task_id="gold",
        bash_command="cd dbt && dbt build --select tag:gold",
        cwd=str(REPO_ROOT),
        doc_md="SILVER -- dbt star schema --> GOLD",
    )

    gx_gold = BashOperator(
        task_id="gx_gold",
        bash_command="uv run python gx/run_validations.py --all --postgres || uv run python gx/run_validations.py --demo",
        cwd=str(REPO_ROOT),
        doc_md="GX check whether gold satisfies defined rules",
    )

    def _publish_mart(**context):
        from src.jobs.publish_gold_to_postgres import build_spark_session, publish_table, resolve_write_mode
        import src.utils.engine as config

        catalog = getattr(config, "DATABRICKS_CATALOG", "freightlake")
        mart_schema = getattr(config, "POSTGRES_SCHEMA_GOLD", "gold") or "gold"
        write_mode = resolve_write_mode("auto")
        spark = build_spark_session(catalog, write_mode)
        try:
            from src.jobs.publish_gold_to_postgres import DEFAULT_GOLD_TABLES

            for tbl in DEFAULT_GOLD_TABLES:
                publish_table(spark, tbl, catalog, "gold", mart_schema, write_mode, str(REPO_ROOT / "spark-warehouse" / "gold"), dry_run=False)
        finally:
            spark.stop()

    mart_publish = PythonOperator(
        task_id="mart_publish",
        python_callable=_publish_mart,
        doc_md="GOLD -- PySpark publish --> PG_MART (serving mart for BI)",
    )

    gold >> gx_gold >> mart_publish
