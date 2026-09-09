{{ config(materialized='table') }}

/*
Date dimension — 2022 to 2026 spine for logistics facts.
*/

WITH dates AS (
  SELECT explode(sequence(to_date('2022-01-01'), to_date('2026-12-31'), interval 1 day)) AS date
)
SELECT
  date_format(date, 'yyyyMMdd')::INT AS date_id,
  date,
  year(date) AS year,
  month(date) AS month,
  day(date) AS day
FROM dates
