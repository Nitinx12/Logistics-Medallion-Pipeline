{{
     config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='incident_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        incident_id,
        trip_id,
        NULLIF(truck_id, '') AS truck_id,
        NULLIF(driver_id, '') AS driver_id,
        incident_date,
        incident_type,
        location_city,
        location_state,
        at_fault_flag,
        injury_flag,
        vehicle_damage_cost,
        cargo_damage_cost,
        claim_amount,
        preventable_flag,
        description,
        updated_at,
        event_ts
    FROM {{ source('bronze', 'safety_incidents') }}
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
            PARTITION BY incident_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    incident_id,
    trip_id,
    truck_id,
    driver_id,
    incident_date,
    incident_type,
    location_city,
    location_state,
    at_fault_flag,
    injury_flag,
    vehicle_damage_cost,
    cargo_damage_cost,
    claim_amount,
    preventable_flag,
    description,
    updated_at,
    event_ts
FROM duplicated
WHERE rnk = 1