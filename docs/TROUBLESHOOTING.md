# Troubleshooting Guide

Common issues when running FreightLake locally, with specific fixes based on the actual codebase.

---

## Port Conflicts

### Airflow Webserver

The Airflow webserver in `docker/compose.yml` is mapped to **port 8090** (not 8080) by default to avoid collisions with other local services:

```yaml
ports:
  - "${AIRFLOW_WEBSERVER_PORT:-8090}:8080"
```

If 8090 is also taken, change `AIRFLOW_WEBSERVER_PORT` in `.env`.

### Postgres

The Postgres container maps to **port 5434** (not 5432):

```yaml
ports:
  - "${POSTGRES_DOCKER_PORT:-5434}:5432"
```

Change `POSTGRES_DOCKER_PORT` in `.env` if 5434 is in use. Update connection strings in your `.env` accordingly.

---

## Databricks Errors

### `sql scope` error during `dbt build`

```
Error: sql scope not granted
```

**Fix**: Go to the Databricks SQL Warehouse UI and run `sql/databricks/schemas.sql` manually, or use a Personal Access Token with SQL scope. `main.py` automatically falls back to the local Pandas pipeline if dbt fails.

### Token / Host Missing

```
DATABRICKS_HOST or DATABRICKS_TOKEN is empty
```

**Fix**: Fill in `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_HTTP_PATH` in your `.env`. Copy from `.env.example` as a template.

---

## Airflow Won't Start

### Missing Fernet Key or Secret Key

```
AirflowConfigException: error_on_setting_missing_fernet_key
```

**Fix**: Generate the required keys and add them to `.env`:

```bash
# Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Webserver secret key
python -c "import secrets; print(secrets.token_hex(32))"
```

Set as `AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__WEBSERVER__SECRET_KEY`.

### `dag-processor` Service Missing

Airflow 3.x requires the `dag-processor` service. It is defined in `docker/compose.yml`. Do not remove it when modifying the compose file.

---

## Database Connection Errors

### Postgres: `Connection refused`

1. Check that Docker containers are running: `docker compose ps`
2. Check you are connecting to port **5434** (the mapped port), not 5432
3. Wait a few seconds after `docker-up` — Postgres has a healthcheck with a `start_period: 10s`

### MongoDB: `MongoServerSelectionError`

The Mongo extractor tries two URIs: authenticated first, then no-auth. If both fail:

1. Check the container is up: `docker compose ps freightlake-mongo`
2. Verify credentials match `.env` values (`MONGO_INITDB_ROOT_USERNAME`, `MONGO_INITDB_ROOT_PASSWORD`)
3. Try connecting manually: `mongosh "mongodb://root:changeme@localhost:27017/admin"`

---

## Seed Errors

### `COPY ERR: column X does not exist`

The OLTP schema DDL may not have been applied yet. `scripts/seed.py` applies DDL before loading, but if an SQL file errors silently, the table may be missing columns.

**Fix**: Inspect the output of `make seed` for `DDL <file> ERR` lines. Manually run the failing SQL file against the DB.

### `COPY ERR: duplicate key value`

This should not happen because seed does `TRUNCATE ... CASCADE` before `COPY`. If it does:

```bash
# Reset and re-seed
make local-pipeline-clean
```

---

## Watermark Issues

### Pipeline re-extracts all data every run

`watermarks.json` is missing or was deleted.

**Fix**: This is expected behavior on first run. The file is created automatically after the first successful extraction.

### Watermark stuck / not advancing

Check the Bronze extractor log at `logs/bronze.log`. Look for errors during the `watermark_set` call.

### Force a full reload

```bash
make local-pipeline-clean
# or manually:
rm watermarks.json
rm -rf delta/
python main.py --skip-docker
```

---

## uv / Python Environment

### `command not found: uv`

Install uv: `pip install uv` or follow [uv installation docs](https://docs.astral.sh/uv/getting-started/installation/).

### Wrong Python version

```
requires-python = ">=3.13"  # from pyproject.toml
```

**Fix**: `uv sync` will automatically use the pinned version in `.python-version`. If it errors, install Python 3.13 via `uv python install 3.13`.

### `pip install` was used instead of `uv add`

If you installed a package with pip directly, the `uv.lock` is now out of sync.

**Fix**: `uv remove <package>` then `uv add <package>` to add it properly.
