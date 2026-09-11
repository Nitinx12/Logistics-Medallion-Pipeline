{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='driver_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        driver_id,
        first_name,
        last_name,
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
    FROM {{ source('bronze', 'drivers') }}
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
            PARTITION BY driver_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    driver_id,
    first_name,
    last_name,
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
FROM duplicated
WHERE rnk = 1