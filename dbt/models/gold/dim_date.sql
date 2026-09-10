{{ config(materialized='table') }}

/*
Date dimension — 2022 to 2026 spine for logistics facts.
Provides calendar attributes for time based rollups on every fact date.
*/

{% if target.type == 'duckdb' %}
with dates as (
  select unnest(generate_series('2022-01-01'::date, '2026-12-31'::date, interval 1 day))::date as date_day
)
select
  strftime(date_day, '%Y%m%d')::int as date_id,
  date_day as date,
  year(date_day) as year,
  quarter(date_day) as quarter,
  month(date_day) as month,
  day(date_day) as day,
  -- DuckDB dayofweek is 0 based Sunday equals 0, Spark is 1 based Sunday equals 1
  -- so add 1 to match Spark semantics
  (dayofweek(date_day) + 1)::int as day_of_week,
  weekofyear(date_day) as week_of_year,
  strftime(date_day, '%A') as day_name,
  strftime(date_day, '%B') as month_name
from dates
{% else %}
with dates as (
  select explode(sequence(to_date('2022-01-01'), to_date('2026-12-31'), interval 1 day)) as date_day
)
select
  date_format(date_day, 'yyyyMMdd')::int as date_id,
  date_day as date,
  year(date_day) as year,
  quarter(date_day) as quarter,
  month(date_day) as month,
  day(date_day) as day,
  dayofweek(date_day) as day_of_week,
  weekofyear(date_day) as week_of_year,
  date_format(date_day, 'EEEE') as day_name,
  date_format(date_day, 'MMMM') as month_name
from dates
{% endif %}
