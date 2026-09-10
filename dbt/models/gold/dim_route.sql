{{ config(materialized='table') }}

/*
Conformed route dimension, Type 1 for now.
Sourced from silver stg_routes. If route rates change over time this can be
promoted to SCD2 via a routes_snapshot, same pattern as dim_driver.
*/

select
    route_id,
    origin_city,
    origin_state,
    destination_city,
    destination_state,
    typical_distance_miles as distance_miles,
    base_rate_per_mile as rate_per_mile,
    fuel_surcharge_rate,
    typical_transit_days
from {{ ref('stg_routes') }}
