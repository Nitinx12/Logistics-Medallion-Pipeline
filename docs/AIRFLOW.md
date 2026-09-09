# Airflow Orchestration

FreightLake uses **Apache Airflow 3.x** running inside Docker (via `docker/compose.yml`) to orchestrate the full data pipeline. Three DAGs, all defined in `airflow/dags/`, chain together using `ExternalTaskSensor` to enforce strict dependency ordering.

---

## DAG Overview

| DAG | Schedule | SLA | File |
|---|---|---|---|
| `freightlake_bronze_dag` | `0 4 * * *` (04:00 UTC daily) | 2 hours | `airflow/dags/freightlake_bronze_dag.py` |
| `freightlake_silver_gold_dag` | `0 6 * * *` (06:00 UTC daily) | None | `airflow/dags/freightlake_silver_gold_dag.py` |
| `freightlake_publish_dag` | `0 7 * * *` (07:00 UTC daily) | None | `airflow/dags/freightlake_publish_dag.py` |

---

## DAG 1: `freightlake_bronze_dag`

Runs both extraction jobs in **parallel** with a 2-hour SLA, retries once on failure (5 minute retry delay).

```
extract_postgres_oltp  ──┐
                          ├──► (both run concurrently, no dependency between them)
extract_mongo_tracking ──┘
```

**Tasks:**
- `extract_postgres_oltp`: Runs `spark_jobs/bronze/extract_postgres_oltp.py`. Incremental pull from Postgres OLTP across 9 tables using `updated_at` watermarks.
- `extract_mongo_tracking`: Runs `spark_jobs/bronze/extract_mongo_tracking.py`. Incremental pull from MongoDB across 3 collections using `event_ts` watermarks.

**Config:**
```python
default_args = {
    "owner": "freightlake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),  # must finish by 6 AM
}
```

---

## DAG 2: `freightlake_silver_gold_dag`

Waits for the Bronze DAG to complete, then triggers `dbt build` to run all Silver and Gold models including tests.

```
wait_bronze (ExternalTaskSensor) ──► dbt_build
```

**Tasks:**
- `wait_bronze`: `ExternalTaskSensor` watching `freightlake_bronze_dag`. Polls every 60 seconds, times out after 2 hours.
- `dbt_build`: Runs `dbt build --profiles-dir . --target dev` from the `/opt/airflow/dbt` directory. This executes all Silver and Gold models and their data quality tests in one command. A test failure here stops the pipeline.

---

## DAG 3: `freightlake_publish_dag`

Waits for `dbt_build` to succeed, then publishes the Gold layer to the Postgres serving mart.

```
wait_silver_gold (ExternalTaskSensor) ──► publish_gold_to_postgres
```

**Tasks:**
- `wait_silver_gold`: `ExternalTaskSensor` watching `freightlake_silver_gold_dag`, specifically the `dbt_build` task.
- `publish_gold_to_postgres`: Runs `spark_jobs/publish/publish_gold_to_postgres.py`. Upserts the Gold star schema tables into `freightlake_mart.mart.*` for BI consumption.

---

## Docker Setup for Airflow

The `docker/compose.yml` defines four Airflow services required for Airflow 3.x:

| Service | Role |
|---|---|
| `airflow-init` | Runs DB migration and creates admin user on first boot |
| `airflow-webserver` | UI at `http://localhost:8090` (port moved from 8080 to avoid collisions) |
| `airflow-scheduler` | Triggers DAG runs on schedule |
| `dag-processor` | Airflow 3.x required service, parses DAG files separately |

DAG files, `spark_jobs/`, and `dbt/` directories are mounted as read-only volumes into all Airflow containers.

---

## Generating Airflow Keys (first-time setup)

The `.env` file requires two generated secrets before Airflow starts:

```bash
# Fernet key (encrypts connection passwords in the metadata DB)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Webserver secret key
python -c "import secrets; print(secrets.token_hex(32))"
```

Set these as `AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__WEBSERVER__SECRET_KEY` in your `.env`.
