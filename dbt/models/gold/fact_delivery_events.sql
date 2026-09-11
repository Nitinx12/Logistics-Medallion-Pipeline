{{
    config(
        materialized='table'
    )
}}

SELECT
    de.event_id,
    de.load_id,
    de.trip_id,
    COALESCE(f.facility_sk, SHA2('UNKNOWN', 256)) AS facility_sk,
    de.facility_id,
    de.event_type,
    CAST(de.scheduled_datetime AS TIMESTAMP) AS scheduled_datetime,
    CAST(de.actual_datetime AS TIMESTAMP) AS actual_datetime,
    CAST(
        (UNIX_TIMESTAMP(CAST(de.actual_datetime AS TIMESTAMP)) - UNIX_TIMESTAMP(CAST(de.scheduled_datetime AS TIMESTAMP))) / 60
    AS INT) AS delay_minutes,
    de.detention_minutes,
    de.on_time_flag,
    -- CAST to STRING handles both native BOOLEAN and STRING source columns;
    -- LOWER() handles any casing that shows up in the source.
    CASE
        WHEN LOWER(CAST(de.on_time_flag AS STRING)) IN ('true', '1', 't', 'yes') THEN 1
        WHEN LOWER(CAST(de.on_time_flag AS STRING)) IN ('false', '0', 'f', 'no') THEN 0
        ELSE NULL
    END AS on_time_int,
    de.location_city,
    de.location_state,
    de.updated_at,
    de.event_ts
FROM {{ ref('delivery_events') }} AS de
LEFT JOIN {{ ref('dim_facilities') }} AS f ON de.facility_id = f.facility_id