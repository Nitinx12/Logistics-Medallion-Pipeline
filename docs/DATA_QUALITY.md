# Data Quality & Validation

FreightLake enforces data quality through two complementary frameworks. Both run as part of `dbt build` (triggered by `make dbt-build` or `make test`) before data is promoted to Gold.

---

## Framework 1: dbt Tests (Structural Validation)

dbt tests run automatically when you run `dbt build`. They validate the structural integrity of every Silver and Gold model.

### Test Types Used

| Test | What it checks |
|---|---|
| `not_null` | Column has no null values |
| `unique` | No duplicate values in the column |
| `relationships` | Every FK value exists in the referenced model's PK |

### Silver Tests (`dbt/models/silver/schema.yml`)

| Model | Column | Tests |
|---|---|---|
| `stg_customers` | `customer_id` | `not_null`, `unique` |
| `stg_customers` | `updated_at` | `not_null` |
| `stg_drivers` | `driver_id` | `not_null`, `unique` |
| `stg_trucks` | `truck_id` | `not_null`, `unique` |
| `stg_loads` | `load_id` | `not_null`, `unique` |
| `stg_loads` | `customer_id` | `not_null`, `relationships` → `stg_customers.customer_id` |
| `stg_trips` | `trip_id` | `not_null`, `unique` |
| `stg_trips` | `load_id` | `not_null`, `relationships` → `stg_loads.load_id` |
| `dim_driver` | `driver_sk` | `not_null`, `unique` |
| `dim_driver` | `driver_id` | `not_null` |
| `dim_vehicle` | `vehicle_sk` | `not_null`, `unique` |
| `dim_vehicle` | `truck_id` | `not_null` |

### Gold Tests (`dbt/models/gold/schema.yml`)

| Model | Column | Tests |
|---|---|---|
| `dim_customer` | `customer_id` | `not_null`, `unique` |
| `dim_warehouse` | `warehouse_id` | `not_null`, `unique` |
| `dim_route` | `route_id` | `not_null`, `unique` |
| `dim_date` | `date_id` | `not_null`, `unique` |
| `fct_orders` | `order_id` | `not_null`, `unique` |
| `fct_orders` | `customer_id` | `not_null`, `relationships` → `dim_customer.customer_id` |
| `fct_shipments` | `shipment_id` | `not_null`, `unique` |
| `fct_deliveries` | `delivery_id` | `not_null`, `unique` |

---

## Framework 2: Great Expectations (Statistical Validation)

Great Expectations is configured in the `great_expectations/` directory. It provides statistical and format-level checks that go beyond what dbt tests can express.

### What it checks

- **Value ranges**: Numeric columns stay within expected bounds (e.g., `revenue` is non-negative)
- **Row count minimums**: Tables must have at least N rows to be considered valid
- **Regex format checks**: String fields like tracking numbers match expected patterns
- **Null rates**: Columns with optional data don't exceed an acceptable null percentage

### Running Great Expectations

```bash
# Runs as part of full test suite
make test

# The test command also tries dbt test:
# uv run pytest tests -v
# bash scripts/bash/dbt.sh test
```

---

## The Quality Gate in Airflow

In the `freightlake_silver_gold_dag`, the `dbt_build` task runs `dbt build` which includes all model tests. If **any** test fails, the task fails, the DAG fails, and the `freightlake_publish_dag` never triggers.

This means:
- Bad data that fails a `not_null` or `relationships` test will **never** reach the Postgres mart
- The pipeline is fail-fast by design
- Do not lower test severity from `error` to `warn` to make a build pass — fix the root cause

---

## SQL Linting (Code Quality)

SQL files are linted with `sqlfluff` as part of `make lint`:

| Dialect | Files |
|---|---|
| `postgres` | `sql/oltp_schema/**`, `sql/serving_mart/**` |
| `databricks` | `sql/databricks/bronze_tables.sql`, `sql/databricks/schemas.sql`, `dbt/models/**` |
| `postgres` | `sql/databricks/watermark.sql` |

Configuration is in `.sqlfluff` at the project root.

---

## Running Locally

```bash
# Full quality check (lint + test)
make ci

# Just tests
make test

# Just lint
make lint
```
