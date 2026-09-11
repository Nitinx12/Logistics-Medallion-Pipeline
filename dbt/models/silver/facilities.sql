{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='facility_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        facility_id,
        facility_name,
        facility_type,
        city,
        state,
        latitude,
        longitude,
        dock_doors,
        operating_hours,
        updated_at
    FROM {{ source('bronze', 'facilities') }}
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
            PARTITION BY facility_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    facility_id,
    facility_name,
    facility_type,
    city,
    state,
    latitude,
    longitude,
    dock_doors,
    operating_hours,
    updated_at
FROM duplicated
WHERE rnk = 1