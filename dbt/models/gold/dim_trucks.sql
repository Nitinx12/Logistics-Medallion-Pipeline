{{
    config(
        materialized='table'
    )
}}

SELECT
    SHA2(CAST(truck_id AS STRING), 256) AS truck_sk,
    truck_id,
    unit_number,
    make,
    model_year,
    vin,
    acquisition_date,
    acquisition_mileage,
    fuel_type,
    tank_capacity_gallons,
    status AS truck_status,
    home_terminal,
    updated_at
FROM {{ ref('trucks') }}

UNION ALL

SELECT
    SHA2('UNKNOWN', 256) AS truck_sk,
    'UNKNOWN' AS truck_id,
    'UNKNOWN' AS unit_number,
    'Unknown' AS make,
    CAST(NULL AS INT) AS model_year,
    CAST(NULL AS STRING) AS vin,
    CAST(NULL AS DATE) AS acquisition_date,
    CAST(NULL AS INT) AS acquisition_mileage,
    CAST(NULL AS STRING) AS fuel_type,
    CAST(NULL AS INT) AS tank_capacity_gallons,
    'Unknown' AS truck_status,
    CAST(NULL AS STRING) AS home_terminal,
    CAST(NULL AS TIMESTAMP) AS updated_at
