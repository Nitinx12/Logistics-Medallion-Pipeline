{{
    config(
        materialized='table'
    )
}}

SELECT
    si.incident_id,
    si.trip_id,
    COALESCE(d.driver_sk, SHA2('UNKNOWN', 256)) AS driver_sk,
    si.driver_id,
    COALESCE(t.truck_sk, SHA2('UNKNOWN', 256)) AS truck_sk,
    si.truck_id,
    CAST(si.incident_date AS DATE) AS incident_date,
    CAST(date_format(CAST(si.incident_date AS DATE), 'yyyyMMdd') AS INT) AS incident_date_key,
    si.incident_type,
    si.location_city,
    si.location_state,
    si.at_fault_flag,
    si.injury_flag,
    si.vehicle_damage_cost,
    si.cargo_damage_cost,
    si.claim_amount,
    si.preventable_flag,
    si.description,
    si.updated_at,
    si.event_ts
FROM {{ ref('safety_incidents') }} AS si
LEFT JOIN {{ ref('dim_drivers') }} AS d ON si.driver_id = d.driver_id
LEFT JOIN {{ ref('dim_trucks') }} AS t ON si.truck_id = t.truck_id