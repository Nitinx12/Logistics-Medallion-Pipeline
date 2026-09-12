# Troubleshooting

Symptom, cause, fix. Ordered by how often each one bites.

## Ports

| Symptom | Cause | Fix |
|---|---|---|
| `start_stack.sh` times out, Airflow never healthy | something else owns 8080 (another Airflow, Jenkins, a Compose project) | change the left side of the mapping in `docker/compose.yml` (`"8081:8080"`), keep the container side 8080, set `AIRFLOW_URL` accordingly |
| `port is already allocated` on compose up | 5432 taken by a local Postgres service | stop the local service or remap the host port |
| Mongo fails to start, `27017` in use | local mongod running | stop it or remap |

`start_stack.sh` rolls the stack back on timeout, so a failed start does
not leave half the services running.

## Databricks

| Symptom | Cause | Fix |
|---|---|---|
| 403 with `ErrorCode: 5108` or `5105` on bronze writes | managed table creation from outside Databricks compute is blocked | the job already falls back to the warehouse path; set `BRONZE_WRITE_MODE=warehouse` (or `local`) in `.env` to skip the failed attempt |
| `UNITY_CATALOG_EXTERNAL_CREATE_TABLE_REQUEST_FOR_NON_EXTERNAL_TABLE_DENIED` | same class of block, external path | use `uc_external` only with a real `DATABRICKS_EXTERNAL_LOCATION` plus a storage credential |
| `ClassNotFoundException: io.unitycatalog.spark.UCSingleCatalog` | the Unity Catalog Spark plugin jar is missing from `jars/` | download `unitycatalog-spark_4.2_2.13` matching your other 0.6.0 jars into `jars/`; the job prints the exact Maven URL |
| dbt fails to connect | missing or stale `DATABRICKS_HOST` / `HTTP_PATH` / `TOKEN` | `profiles.yml` reads them from the environment; check `.env` and that the warehouse is running |
| Warehouse inserts slow for large loads | batch size too small | the default is 5000 rows per statement; the knobs are in `pg_extract_incremental.py` (`batch_size` in `_write_via_warehouse`) |

## Airflow

| Symptom | Cause | Fix |
|---|---|---|
| `airflow db migrate` errors on first boot | the `airflow` database or role was not created | `docker/init/01_create_database.sql` runs only on an empty volume; `scripts/bash/reset_data.sh` (destructive) recreates everything |
| Tasks fail with `uv: command not found` or module errors | running outside the project venv, or stale image | tasks run `uv run ...` with `cwd` at the repo root; rebuild the image (`scripts/bash/bootstrap.sh`) after dependency changes |
| Task fails with `ModuleNotFoundError: pyspark` in a PythonOperator | importing job code into the scheduler interpreter | keep pipeline execution in BashOperator tasks calling the job CLIs |
| `ExternalTaskSensor` pokes forever | upstream DAG never ran for that logical date | trigger `freightlake_bronze` first; all three DAGs share `@daily` and the same start date so dates align |
| DAG list empty in the UI | `dag-processor` not running or DAGs paused | the `dag-processor` service is required by Airflow 3; new DAGs start paused by config, unpause in the UI |

## Watermarks and reruns

| Symptom | Cause | Fix |
|---|---|---|
| Table reported `up to date` but rows are missing | watermark advanced past the gap (source clock skew, late deletes) | rerun with an explicit lower bound: `--since 2026-01-01T00:00:00` |
| Duplicates in bronze after a manual rerun | full reload ran in append mode without truncation | use `--full`, which truncates the target first in warehouse and local modes |
| Everything reloads every run | target table missing, so every run is detected as a full load | check the target table exists (`SHOW TABLES IN freightlake.bronze`); a failed first write leaves no watermark |
| Tied timestamp rows loaded twice or never | the inclusive versus exclusive bound rules | both edge cases are covered and tested in `build_windows`; if you see it, capture the watermark values and file it with the run log |

## GX and Postgres

| Symptom | Cause | Fix |
|---|---|---|
| `gx_silver` fails with `Postgres unavailable` | mart Postgres down, or run from outside Docker with `.env` pointing at a stopped stack | this is a failed gate by design; start the stack (`scripts/bash/start_stack.sh`) and let the DAG retry |
| GX reports `SKIP ... no data or table not found` | the mart table was never published | run the gold DAG end to end once so `mart_publish` creates the tables |
| Connection refused from inside a container | `.env` has `localhost` hosts | compose overrides `POSTGRES_HOST` and `MONGO_URI` for the Airflow services; if you added a new service, add the same overrides |
| `password authentication failed` for the mart | split variables not set consistently | `POSTGRES_MART_USER`/`POSTGRES_MART_PASSWORD` must match the role created by `01_create_database.sql` |

## Docker and data

| Symptom | Cause | Fix |
|---|---|---|
| Init scripts did not run after editing them | `docker-entrypoint-initdb.d` runs only on an empty volume | `scripts/bash/reset_data.sh` with `CONFIRM=--yes` (destroys all data), then `start_stack.sh` |
| OLTP database exists but has no tables | `sql/oltp_schema/` holds no DDL yet (phase 12) | add the DDL there, then reset the volume |
| Spark jobs on the host cannot reach `postgres` hostname | that name only resolves inside the Docker network | on the host use `localhost`; inside containers the compose overrides apply |

## Environment

| Symptom | Cause | Fix |
|---|---|---|
| `OSError: Missing required environment variables` at import | `.env` missing or incomplete | copy `.env.example`, fill the Databricks section; `src/utils/engine.py` lists exactly what is required |
| `uv sync` fails on Python version | interpreter older than 3.13 | uv downloads 3.13 automatically; ensure no `python 3.11` pin is overriding in CI |
| pandas 3.0 warning from pyspark | pyspark does not fully support pandas 3 yet | warning only, tracked; do not pin down without checking pyspark release notes |
