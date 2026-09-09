# dbt Transformation Layer

dbt is the transformation engine for FreightLake's Silver and Gold layers. All models run on Databricks using the `dbt-databricks` adapter. When Databricks is unavailable, the fallback local pipeline in `scripts/run_local_pipeline.py` mirrors the same logic using Pandas.

The dbt project is located at `dbt/` and is configured in `dbt/dbt_project.yml`.

---

## Materialization Strategy

| Layer | Strategy | Format |
|---|---|---|
| Silver | `incremental` (merge) | Delta Lake |
| Gold | `table` (full rebuild) | Delta Lake |

```yaml
# dbt/dbt_project.yml
models:
  freightlake:
    silver:
      +materialized: incremental
      +incremental_strategy: merge
      +file_format: delta
    gold:
      +materialized: table
      +file_format: delta
```

---

## Silver Models (`dbt/models/silver/`)

Silver cleans, deduplicates, and conforms raw Bronze data. All string columns are trimmed (the OLTP source has padded spaces).

### Staging Models

| Model | Source | Primary Key |
|---|---|---|
| `stg_customers` | `bronze.customers` | `customer_id` |
| `stg_drivers` | `bronze.drivers` | `driver_id` |
| `stg_trucks` | `bronze.trucks` | `truck_id` |
| `stg_loads` | `bronze.loads` | `load_id` |
| `stg_trips` | `bronze.trips` | `trip_id` |

### SCD Type 2 Dimension Models

`dim_driver` and `dim_vehicle` implement **Slowly Changing Dimension Type 2** to track historical changes in driver employment status/region and truck assignments.

**How it works** (from `dbt/models/silver/dim_driver.sql`):
1. Pulls raw records from `bronze.drivers`
2. Uses `LEAD()` window function partitioned by `driver_id` ordered by `updated_at` to compute `valid_to`
3. Open rows get `valid_to = '9999-12-31'` and `is_current = true`
4. Surrogate key `driver_sk` generated via `md5(concat(driver_id, valid_from))`

```sql
WITH ranked AS (
  SELECT
    driver_id,
    updated_at::TIMESTAMP AS valid_from,
    LEAD(updated_at::TIMESTAMP) OVER (PARTITION BY driver_id ORDER BY updated_at) AS valid_to
  FROM {{ source('bronze', 'drivers') }}
)
SELECT
  md5(concat(driver_id, CAST(valid_from AS STRING))) AS driver_sk,
  driver_id,
  valid_from,
  COALESCE(valid_to, '9999-12-31') AS valid_to,
  valid_to IS NULL AS is_current
FROM ranked
```

---

## Gold Models (`dbt/models/gold/`)

Gold exposes the final star schema consumed by the Postgres serving mart and BI tools.

### Fact Tables

| Model | Grain | Key Source |
|---|---|---|
| `fct_orders` | One row per load/order | `stg_loads.load_id` → `order_id` |
| `fct_shipments` | One row per trip | `stg_trips.trip_id` |
| `fct_deliveries` | One row per delivery event | `delivery_events` |

**`fct_orders`** includes: `order_id`, `customer_id`, `route_id`, `load_date`, `revenue`, `fuel_surcharge`, `accessorial_charges`, `weight_lbs`, `pieces`

### Dimension Tables

| Model | Description | Key |
|---|---|---|
| `dim_customer` | Customer master from Silver | `customer_id` |
| `dim_driver` | Driver SCD2 (promoted from Silver) | `driver_sk` |
| `dim_vehicle` | Vehicle/truck SCD2 (promoted from Silver) | `vehicle_sk` |
| `dim_warehouse` | Facilities/warehouses | `warehouse_id` |
| `dim_route` | Route master data | `route_id` |
| `dim_date` | Generated calendar 2022-01-01 to 2026-12-31 | `date_id` (YYYYMMDD) |

---

## Semantic Layer (`dbt/models/gold/metrics.yml`)

Three business metrics are defined centrally so dashboards always use the same calculation:

| Metric | Model | Calculation |
|---|---|---|
| `on_time_delivery_rate` | `fct_deliveries` | Average of `is_on_time` by day/week/month |
| `avg_delivery_time` | `fct_shipments` | Average of `duration_hours` |
| `revenue_per_route` | `fct_orders` | Sum of `revenue` grouped by `route_id` |

---

## Data Quality Tests (`schema.yml`)

Every Silver and Gold model has tests defined in `schema.yml` files. These run automatically as part of `dbt build`.

**Silver tests** (from `dbt/models/silver/schema.yml`):

| Model | Column | Tests |
|---|---|---|
| `stg_customers` | `customer_id` | `not_null`, `unique` |
| `stg_customers` | `updated_at` | `not_null` |
| `stg_drivers` | `driver_id` | `not_null`, `unique` |
| `stg_trucks` | `truck_id` | `not_null`, `unique` |
| `stg_loads` | `load_id` | `not_null`, `unique` |
| `stg_loads` | `customer_id` | `not_null`, `relationships` → `stg_customers` |
| `stg_trips` | `trip_id` | `not_null`, `unique` |
| `stg_trips` | `load_id` | `not_null`, `relationships` → `stg_loads` |
| `dim_driver` | `driver_sk` | `not_null`, `unique` |
| `dim_vehicle` | `vehicle_sk` | `not_null`, `unique` |

**Gold tests** (from `dbt/models/gold/schema.yml`):

| Model | Column | Tests |
|---|---|---|
| `fct_orders` | `order_id` | `not_null`, `unique` |
| `fct_orders` | `customer_id` | `not_null`, `relationships` → `dim_customer` |
| `fct_shipments` | `shipment_id` | `not_null`, `unique` |
| `fct_deliveries` | `delivery_id` | `not_null`, `unique` |
| `dim_customer` | `customer_id` | `not_null`, `unique` |
| `dim_warehouse` | `warehouse_id` | `not_null`, `unique` |
| `dim_route` | `route_id` | `not_null`, `unique` |
| `dim_date` | `date_id` | `not_null`, `unique` |

---

## Common Commands

```bash
# Run all models and tests
make dbt-build
# Equivalent: uv run dbt build --project-dir dbt --profiles-dir dbt --target dev

# Run models only (no tests)
make dbt-run

# Generate and serve lineage docs
make dbt-docs
# Then open: dbt/target/index.html
```

---

## dbt Packages

```yaml
# dbt/packages.yml
packages:
  - package: dbt-labs/dbt_utils
```

Install with: `uv run dbt deps --project-dir dbt`
