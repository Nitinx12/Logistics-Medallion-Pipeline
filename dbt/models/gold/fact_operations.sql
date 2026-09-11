{{
    config(
        materialized='table'
    )
}}

WITH trips_agg AS (
    SELECT
        CAST(dispatch_date AS DATE) AS ops_date,
        COUNT(*) AS trip_count,
        SUM(actual_distance_miles) AS total_distance_miles,
        SUM(fuel_gallons_used) AS total_fuel_gallons,
        -- Weighted average (total miles / total gallons), not AVG(average_mpg):
        -- averaging per-trip ratios directly would bias the daily figure
        -- toward short trips instead of reflecting actual fleet fuel economy.
        CASE
            WHEN SUM(fuel_gallons_used) > 0
                THEN SUM(actual_distance_miles) / SUM(fuel_gallons_used)
            ELSE NULL
        END AS avg_mpg,
        SUM(idle_time_hours) AS total_idle_hours
    FROM {{ ref('trips') }}
    GROUP BY 1
),
loads_agg AS (
    SELECT
        CAST(load_date AS DATE) AS ops_date,
        COUNT(*) AS load_count,
        SUM(revenue) AS total_revenue,
        SUM(weight_lbs) AS total_weight_lbs,
        SUM(pieces) AS total_pieces
    FROM {{ ref('loads') }}
    GROUP BY 1
),
fuel_agg AS (
    SELECT
        CAST(purchase_date AS DATE) AS ops_date,
        SUM(gallons) AS total_fuel_gallons_purchased,
        SUM(total_cost) AS total_fuel_cost
    FROM {{ ref('fuel_purchases') }}
    GROUP BY 1
),
maintenance_agg AS (
    SELECT
        CAST(maintenance_date AS DATE) AS ops_date,
        SUM(total_cost) AS total_maintenance_cost,
        SUM(downtime_hours) AS total_downtime_hours
    FROM {{ ref('maintenance_records') }}
    GROUP BY 1
),
safety_agg AS (
    SELECT
        CAST(incident_date AS DATE) AS ops_date,
        COUNT(*) AS incident_count,
        SUM(claim_amount) AS total_claim_amount
    FROM {{ ref('safety_incidents') }}
    GROUP BY 1
)

SELECT
    COALESCE(t.ops_date, l.ops_date, f.ops_date, m.ops_date, s.ops_date) AS ops_date,
    CAST(date_format(COALESCE(t.ops_date, l.ops_date, f.ops_date, m.ops_date, s.ops_date), 'yyyyMMdd') AS INT) AS ops_date_key,
    COALESCE(t.trip_count, 0) AS trip_count,
    COALESCE(t.total_distance_miles, 0) AS total_distance_miles,
    COALESCE(t.total_fuel_gallons, 0) AS total_fuel_gallons,
    COALESCE(t.avg_mpg, 0) AS avg_mpg,
    COALESCE(t.total_idle_hours, 0) AS total_idle_hours,
    COALESCE(l.load_count, 0) AS load_count,
    COALESCE(l.total_revenue, 0) AS total_revenue,
    COALESCE(l.total_weight_lbs, 0) AS total_weight_lbs,
    COALESCE(l.total_pieces, 0) AS total_pieces,
    COALESCE(f.total_fuel_gallons_purchased, 0) AS total_fuel_gallons_purchased,
    COALESCE(f.total_fuel_cost, 0) AS total_fuel_cost,
    COALESCE(m.total_maintenance_cost, 0) AS total_maintenance_cost,
    COALESCE(m.total_downtime_hours, 0) AS total_downtime_hours,
    COALESCE(s.incident_count, 0) AS incident_count,
    COALESCE(s.total_claim_amount, 0) AS total_claim_amount
FROM trips_agg AS t
FULL OUTER JOIN loads_agg AS l ON t.ops_date = l.ops_date
FULL OUTER JOIN fuel_agg AS f ON COALESCE(t.ops_date, l.ops_date) = f.ops_date
FULL OUTER JOIN maintenance_agg AS m ON COALESCE(t.ops_date, l.ops_date, f.ops_date) = m.ops_date
FULL OUTER JOIN safety_agg AS s ON COALESCE(t.ops_date, l.ops_date, f.ops_date, m.ops_date) = s.ops_date