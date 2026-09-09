# Data Dictionary

Generated from `dbt docs generate` `manifest.json` — do not hand edit. Run `dbt docs generate` again after model changes per AGENTS.md:105.

## Bronze (Databricks `freightlake.bronze`)
- `customers` 200 rows, PK `customer_id` — from `sql/oltp_schema/01_customers.sql`
- `drivers` 150 rows, PK `driver_id` — SCD2 via `dbt/snapshots/drivers_snapshot.sql`
- `trucks` 120 rows, `trailers` 180, `facilities` 50, `routes` 58, `loads` 85410, `trips` 85410, `fuel_purchases` 196442
- `delivery_events` 170820, `safety_incidents` 170, `maintenance_records` 2920 — Mongo, `event_ts` watermark, schema evolution

## Silver (dbt `silver`)
- `stg_customers` — columns: customer_id, updated_at — tests: not_null/unique/relationships per `dbt/models/silver/schema.yml`
- `stg_drivers` — columns: driver_id — tests: not_null/unique/relationships per `dbt/models/silver/schema.yml`
- `stg_trucks` — columns: truck_id — tests: not_null/unique/relationships per `dbt/models/silver/schema.yml`
- `stg_loads` — columns: load_id, customer_id — tests: not_null/unique/relationships per `dbt/models/silver/schema.yml`
- `stg_trips` — columns: trip_id, load_id — tests: not_null/unique/relationships per `dbt/models/silver/schema.yml`
- `dim_driver` — columns: driver_sk, driver_id — tests: not_null/unique/relationships per `dbt/models/silver/schema.yml`
- `dim_vehicle` — columns: vehicle_sk, truck_id — tests: not_null/unique/relationships per `dbt/models/silver/schema.yml`

## Gold (dbt `gold` star schema)
- `dim_customer` — customer_id
- `dim_warehouse` — warehouse_id
- `dim_route` — route_id
- `dim_date` — date_id
- `dim_driver` — driver_sk, driver_id
- `dim_vehicle` — vehicle_sk, truck_id
- `fct_orders` — order_id, customer_id
- `fct_shipments` — shipment_id
- `fct_deliveries` — delivery_id

## Mart (Postgres `freightlake_mart.mart`)
- `mart.dim_customer 200`, `mart.dim_driver 150`, `mart.dim_vehicle 120`, `mart.dim_warehouse 50`, `mart.dim_route 58`, `mart.dim_date 1826`
- `mart.fct_orders 85410`, `mart.fct_shipments 85410`, `mart.fct_deliveries 170820`
- All facts `FOREIGN KEY` to dims, indexes on `customer_id, route_id, date` per `sql/serving_mart/01_mart_schema.sql`

## Lineage
See `dbt docs` lineage: `bronze -> silver -> gold -> mart` via `dbt/models/sources.yml` and `dbt/models/gold/metrics.yml` (on_time_delivery_rate, avg_delivery_time, revenue_per_route)
