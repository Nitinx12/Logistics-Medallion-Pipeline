# Changelog

All notable changes to FreightLake are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.2.0] — 2026-09-10

### Added

**Medallion — Silver (`dbt/models/silver/`)**
- 12 incremental staging models: all 9 Postgres tables `stg_customers`, `stg_drivers`, `stg_trucks`, `stg_trailers`, `stg_facilities`, `stg_routes`, `stg_loads`, `stg_trips`, `stg_fuel_purchases` plus 3 Mongo `stg_delivery_events`, `stg_safety_incidents`, `stg_maintenance_records` (`materialized='incremental'`, `incremental_strategy='merge'`, `unique_key` natural key, `file_format='delta'`, `on_schema_change='sync_all_columns'`)
- `is_incremental()` filter on `updated_at::timestamp` and `row_number() partition by natural_key order by updated_at desc` dedup CTE where `row_num` is dropped before final select to avoid surrogate drift
- Light normalization: `trim`/`lower`/`upper`, `::bigint`/`::double`/`::timestamp`/`to_date`, boolean casts for `*_flag` columns with source value caveat
- `dbt/macros/incremental_utils.sql` — `incremental_filter()` and `dedup_by_key()` macros to deduplicate the 12x repeated incremental pattern
- `dbt/macros/generic_tests.sql` — custom generic tests `no_orphan_rows` and `accepted_range` for orphan and range checks alongside dbt_utils

**Medallion — Silver tests (`dbt/models/silver/schema.yml`)**
- Full coverage for all 12 `stg_*` models (previously 5 plus misplaced dims): `not_null`/`unique` on natural keys, `not_null` on `updated_at`, `accepted_values` from real `data/*.csv` distincts (e.g. `customer_type` contract/dedicated/spot, `account_status` active/inactive, `load_type` dry van/refrigerated, `event_type` delivery/pickup, `incident_type` 5 values, `maintenance_type` 7 values), `relationships` for every FK (`loads.customer_id→stg_customers`, `trips.load_id→stg_loads`, `delivery_events.trip_id→stg_trips` etc), column `description` for `dbt docs generate`

**Medallion — Gold (`dbt/models/gold/`)**
- `dim_customer` Type 1 from `ref('stg_customers')` (contract terms can be promoted to SCD2 via snapshot later)
- `dim_warehouse` Type 1 now from `ref('stg_facilities')` (was `source('bronze','facilities')`) and `dim_route` from `ref('stg_routes')` with `fuel_surcharge_rate`/`typical_transit_days` retained
- `dim_date` expanded to `year/quarter/month/day/day_of_week/week_of_year/day_name/month_name` on 2022-01-01 to 2026-12-31 spine
- `dim_driver`/`dim_vehicle` SCD2 now sourced from `drivers_snapshot`/`vehicles_snapshot` (`timestamp` strategy `CAST(updated_at AS TIMESTAMP)`) reshaping `dbt_valid_from`/`dbt_valid_to` to `valid_from`/`valid_to`/`is_current` with stable `md5(natural_key||valid_from)`; renamed `dim_customers.sql→dim_customer.sql` and `dim_drivers.sql→dim_driver.sql` to match `spark_jobs/publish/publish_gold_to_postgres.py:29` `GOLD_TABLES`
- `fct_deliveries` now from `ref('stg_delivery_events')` (was bronze) with `warehouse_id`, `detention_minutes`, `scheduled_datetime`/`actual_datetime`, `location_*`; `fct_orders`/`fct_shipments` enriched with `load_status`/`trip_status` and SCD2 join guidance `dispatch_date between valid_from and valid_to`
- `dbt/models/gold/schema.yml` rewritten for 9 models with correct SCD2 `driver_sk`/`vehicle_sk` uniqueness, `dim_date.date_id` uniqueness and `fct_*` referential integrity (`fct_orders→dim_customer/dim_route`, `fct_shipments→fct_orders`, `fct_deliveries→dim_warehouse`)

**OLTP Schema (`sql/oltp_schema/`)**
- `01_customers.sql:10` `annual_revenue_potential DOUBLE PRECISION` → `BIGINT` to match `data/customers.csv` ints and silver `::bigint`
- `03_trucks.sql:4` `unit_number VARCHAR(20)` → `INT`, `04_trailers.sql:3` `trailer_number VARCHAR(20)` → `INT`
- `06_routes.sql:7` `typical_distance_miles DOUBLE PRECISION` → `INT`, `typical_transit_days DOUBLE` → `INT`
- `07_loads.sql:8` `weight_lbs DOUBLE` → `BIGINT`, `accessorial_charges DOUBLE` → `INT`
- `08_trips.sql:9` `actual_distance_miles DOUBLE` → `BIGINT` to align with `sql/databricks/bronze_tables.sql` and CSV ints
- `02_drivers.sql:15` added `CREATE INDEX idx_drivers_updated_at`

**Bronze DDL (`sql/databricks/`)**
- `bronze_tables.sql:24` added `_loaded_date DATE GENERATED ALWAYS AS (CAST(_loaded_at AS DATE))` to 5 tables `customers`, `loads`, `trips`, `fuel_purchases`, `delivery_events` to fix `PARTITIONED BY (_loaded_date)` referencing non-existent column
- Reconciled column lists against real introspected schemas: `trucks` added `vin`/`acquisition_*`/`fuel_type`/`tank_capacity_gallons`, `trailers` added `trailer_number`/`length_feet`/`model_year`/`vin`/`acquisition_date`/`current_location`, `facilities` added `latitude`/`longitude`/`dock_doors`/`operating_hours`, `routes` added `fuel_surcharge_rate`/`typical_transit_days`, `trips` added 6 columns, `fuel_purchases` restored `trip_id`/`location_*`/`total_cost`/`fuel_card_number`, `delivery_events` flattened location and restored `updated_at` (required for incremental), `safety_incidents` rebuilt to 17 columns, `maintenance_records` reverted `record_id`→`maintenance_id`/`vehicle_id`→`truck_id` and restored 3 cost columns
- `schemas.sql:20` corrected silver schema comment to note snapshots land in silver
- `watermark.sql:14` replaced Postgres `INSERT ... ON CONFLICT DO NOTHING` with Databricks `MERGE INTO freightlake.bronze.etl_watermark ... WHEN NOT MATCHED THEN INSERT` idempotent seeding for 12 source tables; moved schema creation to `schemas.sql`

**Serving Mart (`sql/serving_mart/01_mart_schema.sql`)**
- `dim_customer` added `contract_start_date`, `annual_revenue_potential`, `credit_terms_days`, `primary_freight_type`
- `dim_driver`/`dim_vehicle` converted to SCD2 with `driver_sk`/`vehicle_sk VARCHAR(64) PRIMARY KEY`, `valid_from`/`valid_to`/`is_current` plus indexes on natural key and `is_current`
- `dim_warehouse` added `latitude`/`longitude`/`dock_doors`/`operating_hours`
- `dim_route` added `fuel_surcharge_rate`/`typical_transit_days`/`rate_per_mile`
- `dim_date` added `quarter`/`day`/`day_of_week`/`week_of_year`/`day_name`/`month_name`
- `fct_orders` added `load_type`/`weight_lbs`/`pieces`/`fuel_surcharge`/`accessorial_charges`/`load_status`/`booking_type`/`updated_at`
- `fct_shipments` added `trailer_id`/`duration_hours`/`fuel_gallons_used`/`average_mpg`/`idle_time_hours`/`trip_status`/`updated_at` and separate driver/vehicle indexes
- `fct_deliveries` renamed `status`→`event_type`, added `warehouse_id FK`, `detention_minutes`, `scheduled_datetime`/`actual_datetime`, `location_*` and indexes on warehouse and `delivery_ts`

**Orchestration**
- `airflow/dags/freightlake_silver_gold_dag.py:34` inserted `dbt_snapshot` `BashOperator` (`dbt snapshot --profiles-dir . --target dev`) between `wait_bronze` and `dbt_build` → `wait_bronze >> dbt_snapshot >> dbt_build`; required order is `bronze extract → watermark → dbt snapshot → dbt build → dbt test`
- `scripts/bash/dbt.sh:16` and `scripts/powershell/dbt.ps1:5` now expose `snapshot` action and `build` runs `snapshot` then `build` (both warn but do not fail when warehouse sql scope is missing)
- `Makefile` lint target unchanged but now passes with new watermark `MERGE` syntax

### Changed

**Scripts — audit and parity**
- `scripts/seed.py:20` `POSTGRES_URL` hardcoded `postgresql://postgres:admin@localhost:5432/freight_lake` now respects `POSTGRES_OLTP_URL`/`DATABASE_URL`/`.env` and auto-switches to `POSTGRES_DOCKER_PORT` 5434 when host 5432 is occupied (aligns with `docker/compose.yml` and `docker/init/01_create_databases.sql:19` `freightlake_oltp`)
- `scripts/powershell/setup.ps1:6` added uv fallback search mirroring `scripts/bash/setup.sh:10` (`hermes`/`cargo`/`Py313/Scripts`)
- `scripts/powershell/seed_data.ps1:1` and `run_pipeline.ps1:1` added `Set-StrictMode`/`$ErrorActionPreference="Stop"` and `$LASTEXITCODE` checks to match bash `set -euo pipefail` (previously silent failures); `run_pipeline.ps1` now fails fast on extract/publish and warns only on `dbt build` (expected without warehouse)
- `scripts/databricks_init.py:43` `via_files()` fallback now prints `schemas.sql`, `watermark.sql`, `bronze_tables.sql` (was only first 500 chars of schemas)
- `scripts/load_bronze_to_databricks.py:14` ruff `I001` import sort and trailing-comma formatting fixed (`make lint` now green)

**Snapshots**
- `dbt/snapshots/drivers_snapshot.sql:8` and `vehicles_snapshot.sql:8` `updated_at='updated_at'` → `updated_at="CAST(updated_at AS TIMESTAMP)"` to avoid lexicographic sort of raw text

### Fixed

- Trailing newline (`LT12`) on all `dbt/models/**/*.sql` and `sql/databricks/*.sql` for `sqlfluff` compliance
- `dbt/models/silver/schema.yml` `accepted_values` now uses `arguments:` wrapper and omits `quote: false` so values with spaces (`dry van`, `consumer goods`, `distribution center`, `customer complaint`) compile without `extraneous input` warnings; `dbt compile --write-catalog` now 135 success
- `sql/databricks/watermark.sql` duplicate of `schemas.sql` removed

### Verification

- `uv run dbt parse` clean, `uv run dbt compile --write-catalog` 21 models 112 tests 2 snapshots 3 metrics 135 success
- `uv run sqlfluff lint sql/oltp_schema --dialect postgres`, `sql/serving_mart --dialect postgres`, `sql/databricks --dialect databricks`, `dbt/models --dialect databricks` pass
- `uv run ruff check`/`ruff format --check`/`mypy spark_jobs` pass
- `uv run pytest tests -v` 4 passed

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
