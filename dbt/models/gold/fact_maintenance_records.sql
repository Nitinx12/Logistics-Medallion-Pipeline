{{
    config(
        materialized='table'
    )
}}

SELECT
    mr.maintenance_id,
    COALESCE(t.truck_sk, SHA2('UNKNOWN', 256)) AS truck_sk,
    mr.truck_id,
    CAST(mr.maintenance_date AS DATE) AS maintenance_date,
    CAST(date_format(CAST(mr.maintenance_date AS DATE), 'yyyyMMdd') AS INT) AS maintenance_date_key,
    mr.maintenance_type,
    mr.odometer_reading,
    mr.labor_hours,
    mr.labor_cost,
    mr.parts_cost,
    mr.total_cost,
    mr.facility_location,
    mr.downtime_hours,
    mr.service_description,
    mr.updated_at,
    mr.event_ts
FROM {{ ref('maintenance_records') }} AS mr
LEFT JOIN {{ ref('dim_trucks') }} AS t ON mr.truck_id = t.truck_id