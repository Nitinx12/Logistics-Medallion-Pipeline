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

UNION ALL

SELECT
    SHA2('UNKNOWN', 256) AS route_sk,
    'UNKNOWN' AS route_id,
    'Atlanta' AS origin_city,
    'GA' AS origin_state,
    'Atlanta' AS destination_city,
    'GA' AS destination_state,
    CAST(NULL AS DOUBLE) AS typical_distance_miles,
    CAST(NULL AS DOUBLE) AS base_rate_per_mile,
    CAST(NULL AS DOUBLE) AS fuel_surcharge_rate,
    CAST(NULL AS INT) AS typical_transit_days,
    CAST(NULL AS TIMESTAMP) AS updated_at