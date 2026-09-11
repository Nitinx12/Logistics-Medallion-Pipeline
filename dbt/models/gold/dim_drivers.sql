{{
    config(
        materialized='table'
    )
}}

SELECT
    SHA2(CAST(driver_id AS STRING), 256) AS driver_sk,
    driver_id,
    first_name,
    last_name,
    CONCAT_WS(' ', first_name, last_name) AS driver_name,
    hire_date,
    termination_date,
    license_number,
    license_state,
    date_of_birth,
    home_terminal,
    employment_status,
    cdl_class,
    years_experience,
    updated_at
FROM {{ ref('drivers') }}

UNION ALL

SELECT
    SHA2('UNKNOWN', 256) AS driver_sk,
    'UNKNOWN' AS driver_id,
    'Unknown' AS first_name,
    'Unknown' AS last_name,
    'Unknown' AS driver_name,
    CAST(NULL AS DATE) AS hire_date,
    CAST(NULL AS DATE) AS termination_date,
    CAST(NULL AS STRING) AS license_number,
    CAST(NULL AS STRING) AS license_state,
    CAST(NULL AS DATE) AS date_of_birth,
    CAST(NULL AS STRING) AS home_terminal,
    'Unknown' AS employment_status,
    CAST(NULL AS STRING) AS cdl_class,
    CAST(NULL AS INT) AS years_experience,
    CAST(NULL AS TIMESTAMP) AS updated_at