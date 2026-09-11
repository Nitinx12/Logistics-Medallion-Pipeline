{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='route_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
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
    FROM {{ source('bronze', 'routes') }}
    {% if is_incremental() %}
        WHERE CAST(updated_at AS TIMESTAMP) >= (
            COALESCE(
                (
                    SELECT MAX(CAST(t.updated_at AS TIMESTAMP))
                    FROM {{ this }} AS t
                ),
                TIMESTAMP '1900-01-01 00:00:00'
            ) - INTERVAL 3 DAYS
        )
    {% endif %}
),

duplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY route_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
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
FROM duplicated
WHERE rnk = 1