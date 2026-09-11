{{
    config(
        materialized='table'
    )
}}

SELECT
    SHA2(CAST(trailer_id AS STRING), 256) AS trailer_sk,
    trailer_id,
    trailer_number,
    trailer_type,
    length_feet,
    model_year,
    vin,
    acquisition_date,
    status AS trailer_status,
    current_location,
    updated_at
FROM {{ ref('trailers') }}

UNION ALL

SELECT
    SHA2('UNKNOWN', 256) AS trailer_sk,
    CAST(-1 AS BIGINT) AS trailer_id,
    CAST(-1 AS STRING) AS trailer_number,
    'Dry Van' AS trailer_type,
    CAST(NULL AS INT) AS length_feet,
    CAST(NULL AS INT) AS model_year,
    CAST(NULL AS STRING) AS vin,
    CAST(NULL AS DATE) AS acquisition_date,
    'Active' AS trailer_status,
    'Atlanta' AS current_location,
    CAST(NULL AS TIMESTAMP) AS updated_at