{{
    config(
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key='truck_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        truck_id,
        unit_number,
        make,
        model_year,
        vin,
        acquisition_date,
        acquisition_mileage,
        fuel_type,
        tank_capacity_gallons,
        status,
        home_terminal,
        updated_at
    FROM {{ source('bronze', 'trucks') }}
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
            PARTITION BY truck_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    truck_id,
    unit_number,
    make,
    model_year,
    vin,
    acquisition_date,
    acquisition_mileage,
    fuel_type,
    tank_capacity_gallons,
    status,
    home_terminal,
    updated_at
FROM duplicated
WHERE rnk = 1