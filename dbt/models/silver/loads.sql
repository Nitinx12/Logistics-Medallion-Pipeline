{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='load_id',
        on_schema_change='sync_all_columns'
    )
}}


WITH incremental_filter AS (
    SELECT
        load_id,
        customer_id,
        route_id,
        load_date,
        load_type,
        weight_lbs,
        pieces,
        revenue,
        fuel_surcharge,
        accessorial_charges,
        load_status,
        booking_type,
        updated_at
    FROM {{ source('bronze', 'loads') }}
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
            PARTITION BY load_id
            ORDER BY updated_at DESC
        ) AS rnk
    FROM incremental_filter
)

SELECT
    load_id,
    customer_id,
    route_id,
    load_date,
    load_type,
    weight_lbs,
    pieces,
    revenue,
    fuel_surcharge,
    accessorial_charges,
    load_status,
    booking_type,
    updated_at
FROM duplicated
WHERE rnk = 1