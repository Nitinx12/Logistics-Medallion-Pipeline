{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='fuel_purchase_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        fuel_purchase_id,
        trip_id,
        truck_id,
        driver_id,
        purchase_date,
        location_city,
        location_state,
        gallons,
        price_per_gallon,
        total_cost,
        fuel_card_number,
        updated_at
    FROM {{ source('bronze', 'fuel_purchases') }}
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
            PARTITION BY fuel_purchase_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    fuel_purchase_id,
    trip_id,
    truck_id,
    driver_id,
    purchase_date,
    location_city,
    location_state,
    gallons,
    price_per_gallon,
    total_cost,
    fuel_card_number,
    updated_at
FROM duplicated
WHERE rnk = 1