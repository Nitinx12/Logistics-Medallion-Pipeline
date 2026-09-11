{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trailer_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        trailer_id,
        trailer_number,
        trailer_type,
        length_feet,
        model_year,
        vin,
        acquisition_date,
        status,
        current_location,
        updated_at
    FROM {{ source('bronze', 'trailers') }}
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
            PARTITION BY trailer_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    trailer_id,
    trailer_number,
    trailer_type,
    length_feet,
    model_year,
    vin,
    acquisition_date,
    status,
    current_location,
    updated_at
FROM duplicated
WHERE rnk = 1