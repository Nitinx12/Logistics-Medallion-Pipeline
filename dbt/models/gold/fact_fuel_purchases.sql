{{
    config(
        materialized='table'
    )
}}

SELECT
    fp.fuel_purchase_id,
    fp.trip_id,
    COALESCE(d.driver_sk, SHA2('UNKNOWN', 256)) AS driver_sk,
    fp.driver_id,
    COALESCE(t.truck_sk, SHA2('UNKNOWN', 256)) AS truck_sk,
    fp.truck_id,
    CAST(fp.purchase_date AS DATE) AS purchase_date,
    CAST(date_format(CAST(fp.purchase_date AS DATE), 'yyyyMMdd') AS INT) AS purchase_date_key,
    fp.location_city,
    fp.location_state,
    fp.gallons,
    fp.price_per_gallon,
    fp.total_cost,
    CASE WHEN fp.gallons > 0 THEN fp.total_cost / fp.gallons ELSE NULL END AS calculated_price_per_gallon,
    fp.fuel_card_number,
    CASE WHEN fp.truck_id IS NULL THEN TRUE ELSE FALSE END AS is_unmatched_fuel_card,
    fp.updated_at
FROM {{ ref('fuel_purchases') }} AS fp
LEFT JOIN {{ ref('dim_drivers') }} AS d ON fp.driver_id = d.driver_id
LEFT JOIN {{ ref('dim_trucks') }} AS t ON fp.truck_id = t.truck_id