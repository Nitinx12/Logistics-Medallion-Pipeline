{{
    config(
        materialized='table',
        meta={'time_spine': true}
    )
}}

-- MetricFlow time spine — required by dbt Semantic Layer (dbt >= 1.6).
-- Generates one row per calendar day from 2020-01-01 to 2030-12-31.
-- DuckDB's generate_series returns a list; unnest explodes it to rows.
select
    unnest(
        generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day')
    )::date as date_day
