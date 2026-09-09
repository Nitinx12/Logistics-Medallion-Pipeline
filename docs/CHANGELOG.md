# Changelog

All notable changes to FreightLake are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] — 2026-09-09

### Added

**Infrastructure & Scaffolding**
- Docker Compose setup (`docker/compose.yml`) with Postgres 16, MongoDB 7, and Airflow 3.x services
- Airflow services: `airflow-init`, `airflow-webserver`, `airflow-scheduler`, `dag-processor` (required for Airflow 3.x)
- Airflow webserver on port 8090 (moved from 8080 to avoid local port collisions)
- Postgres mapped to port 5434 (avoiding conflict with local Postgres installs)
- `uv` project setup with `pyproject.toml` pinned to Python 3.13+
- `.env.example` documenting all required environment variables with placeholder values

**Synthetic Data**
- 14 CSV datasets in `data/` covering customers, drivers, trucks, trailers, facilities, routes, loads (~100K rows), trips (~100K rows), fuel purchases (~250K rows), and three MongoDB event collections (~300K delivery events)
- `scripts/seed.py` — idempotent seeder that applies OLTP DDL then loads CSV data into Postgres and MongoDB

**OLTP Schema (`sql/oltp_schema/`)**
- 9 DDL files: `01_customers.sql` through `09_fuel_purchases.sql`
- All tables include `updated_at TIMESTAMPTZ` for incremental watermark extraction

**Bronze Layer**
- `spark_jobs/bronze/extract_postgres_oltp.py` — watermark incremental extraction from all 9 Postgres OLTP tables, upserts to `delta/bronze/*.parquet`
- `spark_jobs/bronze/extract_mongo_tracking.py` — watermark incremental extraction from 3 MongoDB collections (`delivery_events`, `safety_incidents`, `maintenance_records`), upserts to `delta/bronze/*.parquet`
- `watermarks.json` file-based watermark tracking for both sources
- `_loaded_at` audit column added to every Bronze row

**Silver Layer (dbt)**
- 5 staging models: `stg_customers`, `stg_drivers`, `stg_trucks`, `stg_loads`, `stg_trips`
- 2 SCD Type 2 dimension models: `dim_driver`, `dim_vehicle` (using `LEAD()` window function with `valid_from`, `valid_to`, `is_current`, md5 surrogate keys)
- dbt tests: `not_null`, `unique`, `relationships` on all models in `dbt/models/silver/schema.yml`
- Materialized as `incremental` merge on Delta Lake

**Gold Layer (dbt)**
- 3 fact tables: `fct_orders`, `fct_shipments`, `fct_deliveries`
- 6 dimension tables: `dim_customer`, `dim_driver`, `dim_vehicle`, `dim_warehouse`, `dim_route`, `dim_date`
- `dim_date` generated as calendar 2022-01-01 to 2026-12-31
- dbt tests on all Gold models including referential integrity in `dbt/models/gold/schema.yml`
- Semantic layer metrics in `dbt/models/gold/metrics.yml`: `on_time_delivery_rate`, `avg_delivery_time`, `revenue_per_route`
- Materialized as `table` on Delta Lake

**Publish Layer**
- `spark_jobs/publish/publish_gold_to_postgres.py` — reads Gold Parquet and bulk-inserts into `freightlake_mart.mart.*` via `psycopg2.extras.execute_values`
- `scripts/run_local_pipeline.py` — full Bronze through Publish pipeline in Pandas, runs without Databricks

**Orchestration (Airflow DAGs)**
- `freightlake_bronze_dag` — runs both extractors in parallel, daily at 04:00 UTC, 2-hour SLA
- `freightlake_silver_gold_dag` — waits on Bronze via `ExternalTaskSensor`, runs `dbt build` at 06:00 UTC
- `freightlake_publish_dag` — waits on Silver/Gold via `ExternalTaskSensor`, publishes at 07:00 UTC

**Utilities**
- `spark_jobs/utils/logger.py` — centralized stage-scoped logging to both stdout and `logs/<stage>.log`
- `spark_jobs/utils/connection.py` — connection string helpers for Postgres OLTP, Postgres mart, MongoDB, and Databricks
- `spark_jobs/utils/engine.py` — shared utility module

**Entry Point**
- `main.py` — single-command pipeline runner with `--skip-docker` and `--dry-run` flags, automatic fallback to local Pandas pipeline if dbt/Databricks unavailable

**Makefile**
- 15 targets covering setup, Docker, seed, bronze, dbt, publish, pipeline, lint, test, CI, clean, and Databricks init

**Scripts (Bash + PowerShell pairs)**
- `setup.sh` / `setup.ps1`
- `seed_data.sh` / `seed_data.ps1`
- `run_pipeline.sh` / `run_pipeline.ps1`
- `docker.sh` / `clean.sh` / `dbt.sh`
- `scripts/databricks_init.py` — Databricks catalog and schema initialization

**CI/CD (GitHub Actions)**
- `python-ci.yml` — ruff + mypy via uv
- `sql-lint.yml` — sqlfluff for postgres and databricks dialects
- `dbt-ci.yml` — dbt build against CI scratch schema
- `docker-build.yml` — Docker image build + smoke test

**Documentation**
- `README.md` — short overview with architecture diagram, quick start, and docs index
- `ARCHITECTURE.md` — four Mermaid diagrams, per-layer design decisions
- `docs/PROJECT_PLAN.md` — full roadmap, tech stack, build order, 40-term concept map
- `docs/AIRFLOW.md` — DAG schedules, tasks, Docker services
- `docs/DATABRICKS.md` — catalog layout, layer materialization, Databricks setup
- `docs/DBT.md` — models, SCD2 logic, tests, metrics, commands
- `docs/PIPELINE.md` — step-by-step flow, watermark behavior, local delta layout
- `docs/LOCAL_SETUP.md` — prerequisites, quick start, Makefile reference, port table
- `docs/POSTGRES.md` — OLTP and mart schemas, table inventory, connection details
- `docs/MONGODB.md` — collections, watermark field, seed behavior
- `docs/DATA_QUALITY.md` — dbt tests, Great Expectations, quality gate
- `docs/DATA_DICTIONARY.md` — Bronze/Silver/Gold table and column reference
- `docs/TESTING.md` — lint and test commands, CI workflow descriptions, definition of done
- `docs/TROUBLESHOOTING.md` — port collisions, Databricks errors, Airflow setup, watermark resets
- `docs/MONITORING.md` — Airflow UI, application logs, watermarks, dbt docs
- `docs/CONTRIBUTING.md` — branching, commits, paired scripts rule, code style, definition of done
