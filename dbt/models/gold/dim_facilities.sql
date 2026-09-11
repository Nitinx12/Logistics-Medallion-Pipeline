{{
    config(
        materialized='table'
    )
}}

SELECT
    SHA2(CAST(facility_id AS STRING), 256) AS facility_sk,
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
FROM {{ ref('facilities') }}

UNION ALL

SELECT
    SHA2('UNKNOWN', 256) AS facility_sk,
    'UNKNOWN' AS facility_id,
    'Unknown' AS facility_name,
    'Unknown' AS facility_type,
    CAST(NULL AS STRING) AS city,
    CAST(NULL AS STRING) AS state,
    CAST(NULL AS DOUBLE) AS latitude,
    CAST(NULL AS DOUBLE) AS longitude,
    CAST(NULL AS INT) AS dock_doors,
    CAST(NULL AS STRING) AS operating_hours,
    CAST(NULL AS TIMESTAMP) AS updated_at
