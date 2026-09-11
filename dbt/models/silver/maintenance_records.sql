{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='maintenance_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        maintenance_id,
        truck_id,
        maintenance_date,
        maintenance_type,
        odometer_reading,
        labor_hours,
        labor_cost,
        parts_cost,
        total_cost,
        facility_location,
        downtime_hours,
        service_description,
        updated_at,
        event_ts
    FROM {{ source('bronze', 'maintenance_records') }}
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
            PARTITION BY maintenance_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    maintenance_id,
    truck_id,
    maintenance_date,
    maintenance_type,
    odometer_reading,
    labor_hours,
    labor_cost,
    parts_cost,
    total_cost,
    facility_location,
    downtime_hours,
    service_description,
    updated_at,
    event_ts
FROM duplicated
WHERE rnk = 1