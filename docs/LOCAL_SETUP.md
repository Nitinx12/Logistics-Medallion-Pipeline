# Local Setup

Everything needed to run FreightLake on a laptop. Troubleshooting lives in
`docs/TROUBLESHOOTING.md`.

## Prerequisites

| Tool | Version | Check |
|---|---|---|
| uv | latest | `uv --version` |
| Docker Desktop | with the compose plugin | `docker compose version` |
| Java | 17 or newer (for local Spark) | `java -version` |
| Git | any recent | `git --version` |

The project pins Python 3.13 through `pyproject.toml` and `.python-version`;
uv downloads the interpreter automatically, no system Python needed.

## Quick start

```bash
# 1. Clone and enter
git clone <repo url> FreightLake && cd FreightLake

# 2. Environment
cp .env.example .env          # then fill in real Databricks values
uv sync --all-extras          # creates .venv with Python 3.13

# 3. First time bootstrap (builds the Docker image, pulls bases)
scripts/bash/bootstrap.sh

# 4. Start the stack (Postgres, Mongo, Airflow) and wait for health
scripts/bash/start_stack.sh

# 5. Run the pipeline
scripts/bash/run_bronze.sh
scripts/bash/run_silver.sh
scripts/bash/run_gold.sh
scripts/bash/publish_mart.sh
```

Or drive one end to end pass with the Airflow UI: open
`http://localhost:8080` (user `airflow`, password `airflow`), unpause the
three DAGs, and trigger `freightlake_bronze`; the sensors chain the rest.

## Ports

| Port | Service | Collides with |
|---|---|---|
| 5432 | Postgres (OLTP, mart, Airflow metadata in one instance, three databases) | other local Postgres installs |
| 27017 | MongoDB | other local Mongo installs |
| 8080 | Airflow API server and UI | other Airflow or Compose projects, Jenkins |

If a port is taken, change the published port on the left side of the
mapping in `docker/compose.yml` (for example `"8081:8080"`) and adjust the
`AIRFLOW_URL` used by `scripts/bash/start_stack.sh`. See
`docs/TROUBLESHOOTING.md` for the known collision cases.

## Environment variables

Copy `.env.example` to `.env` and fill in the Databricks section; everything
else has working local defaults. The required set (enforced at import time
by `src/utils/engine.py`):

| Variable | Purpose |
|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` | source and mart Postgres location |
| `POSTGRES_DATABASE` / `POSTGRES_USERNAME` / `POSTGRES_PASSWORD` | legacy single database credentials, fallback for the split below |
| `POSTGRES_OLTP_DATABASE` / `POSTGRES_OLTP_USER` / `POSTGRES_OLTP_PASSWORD` | the `freightlake_oltp` source database |
| `POSTGRES_MART_DATABASE` / `POSTGRES_MART_USER` / `POSTGRES_MART_PASSWORD` | the `freightlake_mart` serving database |
| `MONGO_URI` / `MONGO_DB` | MongoDB location and database |
| `DATABRICKS_HOST` / `DATABRICKS_HTTP_PATH` / `DATABRICKS_TOKEN` | Databricks workspace and SQL warehouse |
| `DATABRICKS_CATALOG` | Unity Catalog catalog, `freightlake` by default |
| `BRONZE_WRITE_MODE` | `warehouse` (default), `local`, `uc_managed`, `uc_external` |
| `BRONZE_LOCAL_PATH` | local Delta location for `local` mode |

Container note: `docker/compose.yml` overrides `POSTGRES_HOST` to
`postgres` and `MONGO_URI` to `mongodb://mongo:27017` inside the Airflow
containers, because `localhost` there points at the container itself. Keep
`localhost` in `.env` for jobs run from the host.

## Command reference

| Command | Does |
|---|---|
| `uv sync --all-extras` | install or refresh the virtual environment |
| `scripts/bash/bootstrap.sh` | one time: sync env, build image, pull bases |
| `scripts/bash/start_stack.sh` / `stop_stack.sh` | bring the stack up (with health wait and rollback) or down |
| `scripts/bash/health_check_all.sh` | verify Postgres, Mongo, Airflow, jars |
| `scripts/bash/run_bronze.sh` | both extraction jobs |
| `scripts/bash/run_silver.sh` / `run_gold.sh` | dbt builds per layer |
| `scripts/bash/run_gx.sh` | Great Expectations gate, all suites |
| `scripts/bash/publish_mart.sh` | gold to Postgres mart |
| `scripts/bash/run_all_tests.sh` | full quality gate: ruff, mypy, pytest, dbt test |
| `scripts/bash/monitor_*.sh` | per service monitors, see `docs/MONITORING.md` |
| `scripts/bash/reset_data.sh` | destructive volume reset, requires `CONFIRM=--yes` |

## Offline mode (no Databricks)

Set `BRONZE_WRITE_MODE=local` in `.env` and the whole pipeline runs without
Unity Catalog: bronze lands as local Delta under `BRONZE_LOCAL_PATH`, dbt
targets the databricks profile only when you have a warehouse, and the
publisher reads local gold Delta. The GX gate can validate synthetic data
with `uv run python gx/run_validations.py --demo` when no Postgres is
available.

## Where things land

| Path | Content |
|---|---|
| `logs/` | one dated log file per script and job run |
| `spark-warehouse/bronze/` | local Delta bronze tables in `local` mode |
| `spark-warehouse/gold/` | local Delta gold tables |
| `dbt/target/` | dbt artifacts, run results, compiled SQL |
| `gx/uncommitted/` | GX validation results and data docs |
| `postgres_data`, `mongo_data` volumes | container data, destroyed only by `reset_data.sh` |
