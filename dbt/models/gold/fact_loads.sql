{{
    config(
        materialized='table'
    )
}}

SELECT
    l.load_id,
    COALESCE(c.customer_sk, SHA2('UNKNOWN', 256)) AS customer_sk,
    l.customer_id,
    COALESCE(r.route_sk, SHA2('UNKNOWN', 256)) AS route_sk,
    l.route_id,
    CAST(l.load_date AS DATE) AS load_date,
    CAST(date_format(CAST(l.load_date AS DATE), 'yyyyMMdd') AS INT) AS load_date_key,
    l.load_type,
    l.weight_lbs,
    l.pieces,
    l.revenue,
    l.fuel_surcharge,
    l.accessorial_charges,
    (l.revenue + COALESCE(l.fuel_surcharge, 0) + COALESCE(l.accessorial_charges, 0)) AS total_charge,
    l.load_status,
    l.booking_type,
    CASE WHEN c.customer_sk IS NULL THEN TRUE ELSE FALSE END AS is_unmatched_customer,
    CASE WHEN r.route_sk IS NULL THEN TRUE ELSE FALSE END AS is_unmatched_route,
    l.updated_at
FROM {{ ref('loads') }} AS l
LEFT JOIN {{ ref('dim_customers') }} AS c
    ON l.customer_id = c.customer_id
    AND c.is_current = TRUE
LEFT JOIN {{ ref('dim_routes') }} AS r
    ON l.route_id = r.route_id