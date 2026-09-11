{{
    config(
        materialized='table'
    )
}}

SELECT
    t.trip_id,
    t.load_id,
    -- surrogate keys from dimensions with Unknown handling
    COALESCE(d.driver_sk, SHA2('UNKNOWN', 256)) AS driver_sk,
    t.driver_id,
    COALESCE(tr.truck_sk, SHA2('UNKNOWN', 256)) AS truck_sk,
    t.truck_id,
    COALESCE(tl.trailer_sk, SHA2('UNKNOWN', 256)) AS trailer_sk,
    t.trailer_id,
    -- dates
    CAST(t.dispatch_date AS DATE) AS dispatch_date,
    CAST(date_format(CAST(t.dispatch_date AS DATE), 'yyyyMMdd') AS INT) AS dispatch_date_key,
    -- measures
    t.actual_distance_miles,
    t.actual_duration_hours,
    t.fuel_gallons_used,
    t.average_mpg,
    t.idle_time_hours,
    CASE WHEN t.actual_duration_hours > 0 THEN t.actual_distance_miles / t.actual_duration_hours ELSE NULL END AS avg_speed_mph,
    t.trip_status,
    CASE WHEN t.driver_id IS NULL OR t.truck_id IS NULL THEN TRUE ELSE FALSE END AS is_unassigned,
    t.updated_at
FROM {{ ref('trips') }} AS t
LEFT JOIN {{ ref('dim_drivers') }} AS d ON t.driver_id = d.driver_id
LEFT JOIN {{ ref('dim_trucks') }} AS tr ON t.truck_id = tr.truck_id
LEFT JOIN {{ ref('dim_trailers') }} AS tl ON t.trailer_id = tl.trailer_id
