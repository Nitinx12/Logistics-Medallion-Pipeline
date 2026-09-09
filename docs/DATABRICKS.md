# Databricks Lakehouse

FreightLake targets Databricks as its lakehouse compute and storage platform. All Bronze, Silver, and Gold layers use **Delta Lake** as the table format. Databricks Community Edition is sufficient — Unity Catalog features degrade gracefully to the Hive metastore.

---

## When Databricks Is Used

`main.py` attempts to run `dbt build` against Databricks. If it fails (no token, no SQL scope, Community Edition limitation), it automatically falls back to the local pipeline in `scripts/run_local_pipeline.py` using Pandas and local Parquet files under `delta/`.

---

## Catalog & Schema Layout

```
freightlake  (catalog)
├── bronze   (schema) — raw Delta tables, watermark incremental
├── silver   (schema) — cleaned, SCD2 dimensions, dbt incremental
└── gold     (schema) — star schema facts and dims, dbt tables
```

Environment variables control the names:

```bash
DATABRICKS_CATALOG=freightlake
DATABRICKS_SCHEMA_BRONZE=bronze
DATABRICKS_SCHEMA_SILVER=silver
DATABRICKS_SCHEMA_GOLD=gold
```

---

## Bronze Layer

**Jobs:** `spark_jobs/bronze/extract_postgres_oltp.py` and `spark_jobs/bronze/extract_mongo_tracking.py`

On Databricks, these jobs would use:
- PySpark JDBC connector to read from Postgres over watermarks
- PySpark MongoDB connector to read from Mongo over `event_ts` watermarks
- `MERGE INTO freightlake.bronze.<table>` for idempotent upserts
- Watermark state stored in `freightlake.bronze.etl_watermark`

SQL schema for Bronze tables: `sql/databricks/bronze_tables.sql`
SQL schema for watermark table: `sql/databricks/watermark.sql`

Locally (without Databricks), Bronze writes Parquet to `delta/bronze/`.

---

## Silver Layer

**Tool:** dbt with `dbt-databricks` adapter

- Materialized as `incremental` (merge strategy) on Delta Lake
- `dim_driver` and `dim_vehicle` use SCD Type 2 via `LEAD()` window function
- Snapshots stored in the Silver schema, configured via:
  ```yaml
  # dbt/dbt_project.yml
  snapshots:
    freightlake:
      +strategy: timestamp
      +updated_at: updated_at
      +target_schema: "{{ env_var('DATABRICKS_SCHEMA_SILVER', 'silver') }}"
  ```

---

## Gold Layer

**Tool:** dbt with `dbt-databricks` adapter

- Materialized as `table` (full rebuild) on Delta Lake
- Star schema: `fct_orders`, `fct_shipments`, `fct_deliveries`, `dim_customer`, `dim_driver`, `dim_vehicle`, `dim_warehouse`, `dim_route`, `dim_date`
- Metrics defined centrally in `dbt/models/gold/metrics.yml`

SQL catalog setup: `sql/databricks/schemas.sql`

---

## Databricks Configuration

Fill in `.env` before running:

```bash
DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
DATABRICKS_TOKEN=dapi_replace_with_a_real_token
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse_id>
```

`dbt/profiles.yml` reads all credentials from env — no hardcoded tokens:

```yaml
freightlake:
  target: dev
  outputs:
    dev:
      type: databricks
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      catalog: "{{ env_var('DATABRICKS_CATALOG', 'freightlake') }}"
      schema: "{{ env_var('DATABRICKS_SCHEMA_GOLD', 'gold') }}"
```

---

## Initialize Databricks (first-time)

```bash
# Create catalog, schemas, and Bronze Delta tables
make databricks-init
# or just schemas
make databricks-schemas

# If dbt fails with "sql scope" error, run this manually in the Databricks SQL Warehouse UI:
# sql/databricks/schemas.sql
```

---

## Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Table format | Delta Lake (not Iceberg) | Native Databricks integration, mature `MERGE INTO` support |
| Pattern | ELT (not ETL) | Raw data lands first, transforms happen inside Databricks via dbt |
| Executor | LocalExecutor for Airflow | Keeps Docker Compose simple, no Celery broker needed |
| Community Edition | Supported | Unity Catalog degrades gracefully, Delta + PySpark + dbt fully work |
