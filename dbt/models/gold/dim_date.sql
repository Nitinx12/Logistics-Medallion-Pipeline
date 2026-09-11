{{
    config(
        materialized='table'
    )
}}

WITH date_spine AS (
    SELECT EXPLODE(SEQUENCE(
        DATE '2020-01-01',
        DATE '2030-12-31',
        INTERVAL 1 DAY
    )) AS date_day
)

SELECT
    CAST(date_format(date_day, 'yyyyMMdd') AS INT) AS date_key,
    date_day AS full_date,
    YEAR(date_day) AS year,
    QUARTER(date_day) AS quarter,
    MONTH(date_day) AS month,
    DAY(date_day) AS day,
    DAYOFWEEK(date_day) AS day_of_week,
    WEEKOFYEAR(date_day) AS week_of_year,
    DATE_FORMAT(date_day, 'EEEE') AS day_name,
    DATE_FORMAT(date_day, 'MMMM') AS month_name,
    CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine