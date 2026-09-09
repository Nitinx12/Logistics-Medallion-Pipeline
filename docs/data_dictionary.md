# Data Dictionary

See dbt docs lineage for full reference. This file lists bronze sources and gold mart.

## Bronze
- `freightlake.bronze.customers` from Postgres OLTP
- `freightlake.bronze.delivery_events` from Mongo, semi structured location

## Silver
- `dim_driver` SCD2 `valid_from`, `valid_to`, `is_current`

## Gold
- `dim_customer`, `fct_orders` star schema
