{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        event_id,
        load_id,
        trip_id,
        event_type,
        facility_id,
        scheduled_datetime,
        actual_datetime,
        detention_minutes,
        on_time_flag,
        location_city,
        location_state,
        updated_at,
        event_ts
    FROM {{ source('bronze', 'delivery_events') }}
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
            PARTITION BY event_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    event_id,
    load_id,
    trip_id,
    event_type,
    facility_id,
    scheduled_datetime,
    actual_datetime,
    detention_minutes,
    on_time_flag,
    location_city,
    location_state,
    updated_at,
    event_ts
FROM duplicated
WHERE rnk = 1