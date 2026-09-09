# Pipeline: End-to-End Flow

The FreightLake pipeline has two execution modes: a **local mode** (no Databricks, no Airflow) and a **production mode** (Databricks + Airflow). Both follow the same logical flow.

---

## Execution Modes

| Mode | Entry Point | When to Use |
|---|---|---|
| Local (single command) | `python main.py` | Development, demos, portfolios without Databricks |
| Local (skip Docker) | `python main.py --skip-docker` | Docker already running |
| Airflow (production) | Airflow UI at `localhost:8090` | Scheduled daily runs |
| Dry run | `python main.py --dry-run` | Preview steps without executing |

---

## Pipeline Steps

`main.py` executes these 6 steps in order, stopping on any failure and printing a summary:

```
1. setup     → uv run python --version (sanity check)
2. docker    → docker compose up -d postgres mongo
3. seed      → scripts/seed.py
4. bronze_pg → spark_jobs/bronze/extract_postgres_oltp.py
5. bronze_mongo → spark_jobs/bronze/extract_mongo_tracking.py
6. dbt       → dbt build --project-dir dbt --profiles-dir dbt --target dev
7. publish   → spark_jobs/publish/publish_gold_to_postgres.py
```

If `dbt build` fails (e.g. no Databricks SQL warehouse with `sql` scope), `main.py` automatically falls back to `scripts/run_local_pipeline.py` to run Silver and Gold locally using Pandas.

---

## Step 1 & 2: Infrastructure

Docker Compose (`docker/compose.yml`) starts:
- **Postgres 16** on port `5434` (configurable via `POSTGRES_DOCKER_PORT`) — hosts the OLTP DB, mart DB, and Airflow metadata DB
- **MongoDB 7** on port `27017` — hosts the tracking event collections

---

## Step 3: Seed

`scripts/seed.py` loads synthetic CSV data from `data/` into both sources:

**Postgres OLTP** (applies DDL from `sql/oltp_schema/` first, then COPY from CSV):

| CSV | Table | Approx. Size |
|---|---|---|
| `customers.csv` | `customers` | ~500 rows |
| `drivers.csv` | `drivers` | ~500 rows |
| `trucks.csv` | `trucks` | ~250 rows |
| `trailers.csv` | `trailers` | ~250 rows |
| `facilities.csv` | `facilities` | ~100 rows |
| `routes.csv` | `routes` | ~100 rows |
| `loads.csv` | `loads` | ~100K rows |
| `trips.csv` | `trips` | ~100K rows |
| `fuel_purchases.csv` | `fuel_purchases` | ~250K rows |

**MongoDB** (`freight_lake` database):

| CSV | Collection | Approx. Size |
|---|---|---|
| `delivery_events.csv` | `delivery_events` | ~300K docs |
| `safety_incidents.csv` | `safety_incidents` | ~5K docs |
| `maintenance_records.csv` | `maintenance_records` | ~10K docs |

Seed is **idempotent**: Postgres tables are `TRUNCATE`d then re-COPYed, Mongo collections are `drop()`ped then re-inserted.

---

## Step 4 & 5: Bronze (Incremental Extraction)

**Postgres extractor** (`spark_jobs/bronze/extract_postgres_oltp.py`):
- Reads `watermarks.json` for each table's last high watermark (key: `pg:<table>`)
- Queries `SELECT * FROM <table> WHERE updated_at > <watermark>`
- Upserts into `delta/bronze/<table>.parquet` by deduplicating on the first column (PK) keeping the latest `updated_at`
- Advances the watermark to `max(updated_at)` of new rows
- Writes a `_loaded_at` audit column on every row

**Mongo extractor** (`spark_jobs/bronze/extract_mongo_tracking.py`):
- Reads `watermarks.json` for each collection (key: `mongo:<collection>`)
- Queries with `{ event_ts: { $gt: <watermark> } }`, falls back to `updated_at` if no `event_ts`
- On first run with no watermark, loads all documents
- Upserts into `delta/bronze/<collection>.parquet` deduplicating on first column (PK)
- Advances watermark to `max(event_ts)`

Both jobs use `logs/bronze.log` for structured output via `spark_jobs/utils/logger.py`.

---

## Step 6: Silver + Gold (dbt Build)

`dbt build` in one command runs models, snapshots, seeds, and tests for both layers:

**Silver** (`dbt/models/silver/`):
- Trims whitespace from string columns (OLTP has padded data)
- Deduplicates by primary key
- Generates SCD Type 2 for `dim_driver` and `dim_vehicle` using `LEAD()` window function
- Runs `not_null`, `unique`, and `relationships` tests

**Gold** (`dbt/models/gold/`):
- Builds fact tables (`fct_orders`, `fct_shipments`, `fct_deliveries`) from Silver staging tables
- Builds dimension tables (`dim_customer`, `dim_warehouse`, `dim_route`, `dim_date`)
- `dim_date` is generated as a calendar from 2022-01-01 to 2026-12-31
- Runs full test suite including referential integrity checks

---

## Step 7: Publish (Gold to Postgres Mart)

`spark_jobs/publish/publish_gold_to_postgres.py` reads each Gold Parquet file and:
1. Creates `mart` schema in `freightlake_mart` if not exists
2. Drops and recreates each table with inferred column types (INT, FLOAT, TIMESTAMPTZ, BOOLEAN, TEXT)
3. Bulk-inserts rows using `psycopg2.extras.execute_values` in pages of 5,000

**Published tables** in `freightlake_mart.mart.*`:

```
dim_customer   dim_driver   dim_vehicle
dim_warehouse  dim_route    dim_date
fct_orders     fct_shipments  fct_deliveries
```

**Verify the publish completed:**
```bash
psql freightlake_mart -c "SELECT tablename FROM pg_tables WHERE schemaname='mart'"
```

---

## Watermark File

`watermarks.json` at the project root tracks the high watermark for every source table and collection. It is updated atomically after each successful extraction.

```json
{
  "pg:customers":         "2026-09-01T12:00:00",
  "pg:loads":             "2026-09-01T12:00:00",
  "mongo:delivery_events": "2026-09-01T11:59:59"
}
```

To force a full reload, delete `watermarks.json` and re-run (or use `make local-pipeline-clean`).

---

## Local Delta Output

When running locally (without Databricks), Bronze/Silver/Gold output is written to local Parquet files under `delta/`:

```
delta/
├── bronze/
│   ├── customers.parquet
│   ├── drivers.parquet
│   └── ...
├── silver/
│   ├── dim_customer.parquet
│   ├── dim_driver.parquet   (SCD2)
│   └── ...
└── gold/
    ├── fct_orders.parquet
    ├── dim_customer.parquet
    └── ...
```
