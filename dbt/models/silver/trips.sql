{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trip_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        trip_id,
        load_id,
        driver_id,
        truck_id,
        trailer_id,
        dispatch_date,
        actual_distance_miles,
        actual_duration_hours,
        fuel_gallons_used,
        average_mpg,
        idle_time_hours,
        trip_status,
        updated_at
    FROM {{ source('bronze', 'trips') }}
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
            PARTITION BY trip_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    trip_id,
    load_id,
    driver_id,
    truck_id,
    trailer_id,
    dispatch_date,
    actual_distance_miles,
    actual_duration_hours,
    fuel_gallons_used,
    average_mpg,
    idle_time_hours,
    trip_status,
    updated_at
FROM duplicated
WHERE rnk = 1