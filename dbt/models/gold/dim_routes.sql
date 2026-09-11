{{
    config(
        materialized='table'
    )
}}

SELECT
    SHA2(CAST(route_id AS STRING), 256) AS route_sk,
    route_id,
    origin_city,
    origin_state,
    destination_city,
    destination_state,
    typical_distance_miles,
    base_rate_per_mile,
    fuel_surcharge_rate,
    typical_transit_days,
    updated_at
FROM {{ ref('routes') }}
