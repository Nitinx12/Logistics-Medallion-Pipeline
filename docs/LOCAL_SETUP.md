# Local Setup Guide

This guide covers setting up FreightLake locally, from clone to a working pipeline, in about 5 minutes.

---

## Prerequisites

| Tool | Purpose | Check |
|---|---|---|
| Python 3.13+ | Required by `pyproject.toml` | `python --version` |
| `uv` | Package manager (mandatory, never use pip) | `uv --version` |
| Docker Desktop | Runs Postgres, MongoDB, Airflow | `docker --version` |
| Git | Source control | `git --version` |

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/Nitinx12/Logistics-Medallion-Pipeline.git
cd Logistics-Medallion-Pipeline

# 2. Configure environment
cp .env.example .env
# Edit .env — minimum required for local run:
#   AIRFLOW__CORE__FERNET_KEY  (generate below)
#   AIRFLOW__WEBSERVER__SECRET_KEY  (generate below)

# 3. Generate Airflow keys
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_hex(32))"

# 4. Install dependencies
uv sync

# 5. Run the full pipeline (Docker + seed + bronze + silver/gold + publish)
python main.py
```

---

## Running Without Docker (infrastructure already up)

```bash
python main.py --skip-docker
# or
make local-pipeline
```

---

## Preview Steps Without Executing

```bash
python main.py --dry-run
```

---

## Makefile Targets

All targets are thin wrappers around scripts in `scripts/bash/` and `scripts/powershell/`.

| Target | What it does |
|---|---|
| `make setup` | Runs `scripts/bash/setup.sh` — checks Docker, uv, Python version, then `uv sync` |
| `make setup-ps` | PowerShell equivalent of setup |
| `make docker-up` | Starts Postgres, MongoDB, and Airflow via `docker/compose.yml` |
| `make docker-down` | Stops all containers |
| `make seed` | Loads synthetic CSV data into Postgres and MongoDB |
| `make bronze` | Runs both PySpark bronze extractors locally |
| `make dbt-run` | Runs dbt models only (no tests) |
| `make dbt-build` | Runs dbt models + tests (equivalent to `make test`) |
| `make dbt-docs` | Generates and serves dbt lineage docs |
| `make publish` | Runs the Gold to Postgres mart publish job |
| `make pipeline` | Full run: equivalent to `python main.py` |
| `make local-pipeline` | Full run skipping Docker (`--skip-docker`) |
| `make local-pipeline-clean` | Wipes `delta/` and `watermarks.json`, then full run |
| `make lint` | Runs `ruff`, `mypy`, and `sqlfluff` |
| `make test` | Runs `pytest` and `dbt test` |
| `make ci` | Runs `lint` + `test` (same as CI) |
| `make clean` | Removes containers, volumes, and local build artifacts |
| `make databricks-init` | Initializes Databricks catalog, schemas, and tables |
| `make databricks-schemas` | Creates only schemas (subset of init) |

---

## PowerShell Equivalent (Windows)

```powershell
Copy-Item .env.example .env
uv sync
python main.py --skip-docker

# Or using the paired scripts:
.\scripts\powershell\setup.ps1
.\scripts\powershell\seed_data.ps1
.\scripts\powershell\run_pipeline.ps1
```

---

## Services and Ports

After `make docker-up`:

| Service | Host | Port | Credentials |
|---|---|---|---|
| Postgres | `localhost` | `5434` | `postgres / changeme` |
| MongoDB | `localhost` | `27017` | `root / changeme` |
| Airflow Webserver | `localhost` | `8090` | `admin / changeme` |

> The Airflow port is **8090**, not 8080, to avoid collisions with other local services.

---

## Databricks (Optional)

If you have Databricks access, fill in the Databricks variables in `.env`:

```bash
DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
DATABRICKS_TOKEN=dapi_replace_with_a_real_token
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse_id>
DATABRICKS_CATALOG=freightlake
DATABRICKS_SCHEMA_BRONZE=bronze
DATABRICKS_SCHEMA_SILVER=silver
DATABRICKS_SCHEMA_GOLD=gold
```

Then initialize the Databricks catalog and run SQL schemas:
```bash
make databricks-init
# If you get "sql scope" errors, run sql/databricks/schemas.sql in the Databricks SQL Warehouse UI
```

Databricks Community Edition works. Unity Catalog features degrade gracefully to the Hive metastore.

---

## Verifying the Pipeline Ran

```bash
# Check mart tables exist
psql postgresql://postgres:admin@localhost:5432/freightlake_mart \
  -c "SELECT tablename FROM pg_tables WHERE schemaname='mart'"

# Check local delta files
ls delta/bronze/
ls delta/silver/
ls delta/gold/

# Check watermarks advanced
cat watermarks.json
```

---

## Teardown

```bash
make docker-down          # stop containers, keep volumes
make clean                # stop containers + remove volumes + clean build artifacts
make local-pipeline-clean # wipe delta/ and watermarks.json then re-run
```
