# Data Dictionary

The full data dictionary is auto-generated from dbt documentation. This file provides a human-readable summary. For the authoritative lineage graph and column-level descriptions, generate the dbt docs site.

---

## Generating the dbt Docs Site

```bash
make dbt-docs
# or
uv run dbt docs generate --project-dir dbt --profiles-dir dbt --static
```

Then open `dbt/target/index.html` in your browser. The site includes:
- Column descriptions for every model
- Full lineage graph from Bronze sources to Gold facts
- Test coverage per column
- Source freshness status

---

## Bronze Layer (`delta/bronze/`)

Raw data landed from source systems. All tables include a `_loaded_at` audit timestamp.

### From Postgres OLTP

| Table | Primary Key | Watermark Column | Description |
|---|---|---|---|
| `customers` | `customer_id` | `updated_at` | Company customers — name, contact, billing address |
| `drivers` | `driver_id` | `updated_at` | Drivers — name, license, CDL class, employment status, home terminal |
| `trucks` | `truck_id` | `updated_at` | Trucks — make, model, year, VIN, status |
| `trailers` | `trailer_id` | `updated_at` | Trailers — type (flatbed/reefer/dry), capacity, status |
| `facilities` | `facility_id` | `updated_at` | Warehouses and terminals — name, location, type |
| `routes` | `route_id` | `updated_at` | Origin-destination pairs with distance and estimated transit time |
| `loads` | `load_id` | `updated_at` | Load/order records — customer, route, revenue, weight, pieces |
| `trips` | `trip_id` | `updated_at` | Trip records — load to driver and truck assignment, actual dates |
| `fuel_purchases` | `fuel_purchase_id` | `updated_at` | Fuel fill-up records per truck — gallons, price, location |

### From MongoDB

| Collection | Primary Key | Watermark Field | Description |
|---|---|---|---|
| `delivery_events` | `event_id` | `event_ts` | GPS pings and status transitions (picked up, in transit, delivered) |
| `safety_incidents` | `incident_id` | `event_ts` | Safety reports — accident, near-miss, damage, violation |
| `maintenance_records` | `maintenance_id` | `updated_at` | Truck maintenance and inspection records |

---

## Silver Layer (`delta/silver/`)

Cleaned, deduplicated, and conformed data. Built by dbt from Bronze.

### Staging Models

| Model | Source | Key Columns |
|---|---|---|
| `stg_customers` | `bronze.customers` | `customer_id`, `company_name`, `updated_at` |
| `stg_drivers` | `bronze.drivers` | `driver_id`, `driver_name`, `updated_at` |
| `stg_trucks` | `bronze.trucks` | `truck_id`, `make`, `model`, `status`, `updated_at` |
| `stg_loads` | `bronze.loads` | `load_id`, `customer_id`, `route_id`, `load_date`, `revenue` |
| `stg_trips` | `bronze.trips` | `trip_id`, `load_id`, `driver_id`, `truck_id` |

### SCD Type 2 Dimensions

| Model | Source | Surrogate Key | SCD Columns |
|---|---|---|---|
| `dim_driver` | `bronze.drivers` | `driver_sk` (md5) | `valid_from`, `valid_to`, `is_current` |
| `dim_vehicle` | `bronze.trucks` | `vehicle_sk` (md5) | `valid_from`, `valid_to`, `is_current` |

`valid_to` is `9999-12-31` for the current record. `is_current = true` when `valid_to IS NULL` before coalescing.

---

## Gold Layer (`delta/gold/`)

Star schema built from Silver. Consumed by the Postgres mart and BI tools.

### Fact Tables

| Model | Grain | Key Columns | Source |
|---|---|---|---|
| `fct_orders` | One row per load | `order_id`, `customer_id`, `route_id`, `load_date`, `revenue`, `weight_lbs`, `pieces` | `stg_loads` |
| `fct_shipments` | One row per trip | `shipment_id`, `load_id`, `driver_id`, `truck_id`, `ship_date`, `duration_hours` | `stg_trips` |
| `fct_deliveries` | One row per delivery event | `delivery_id`, `event_ts`, `status`, `is_on_time` | `delivery_events` |

### Dimension Tables

| Model | Description | Key |
|---|---|---|
| `dim_customer` | Customer master (SCD1) | `customer_id` |
| `dim_driver` | Driver with SCD2 history | `driver_sk` |
| `dim_vehicle` | Truck/vehicle with SCD2 history | `vehicle_sk` |
| `dim_warehouse` | Facilities (renamed from `facility_id` → `warehouse_id`) | `warehouse_id` |
| `dim_route` | Origin-destination pairs | `route_id` |
| `dim_date` | Calendar table 2022-01-01 to 2026-12-31 | `date_id` (YYYYMMDD format) |

---

## Serving Mart (`freightlake_mart.mart.*`)

Mirrors the Gold layer in Postgres for fast BI queries. Published by `spark_jobs/publish/publish_gold_to_postgres.py`. Same table names as Gold, schema prefix is `mart`.

---

## Business Metrics (Semantic Layer)

Defined in `dbt/models/gold/metrics.yml`, these metrics have a single canonical definition:

| Metric | Model | Calculation | Time Grains |
|---|---|---|---|
| `on_time_delivery_rate` | `fct_deliveries` | Average of `is_on_time` | day, week, month |
| `avg_delivery_time` | `fct_shipments` | Average of `duration_hours` | |
| `revenue_per_route` | `fct_orders` | Sum of `revenue` by `route_id` | |
