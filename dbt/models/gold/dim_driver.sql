{{ config(materialized='table') }}

/*
SCD Type 2 for drivers, sourced from the drivers_snapshot (dbt native
snapshot, timestamp strategy on updated_at). The snapshot already tracks
dbt_valid_from / dbt_valid_to per version, so this model just reshapes
that into a business-friendly dimension with a stable surrogate key.
*/

with ranked as (
  select
    trim(driver_id) as driver_id,
    trim(first_name) || ' ' || trim(last_name) as driver_name,
    trim(employment_status) as employment_status,
    trim(home_terminal) as region,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to
  from {{ ref('drivers_snapshot') }}
)
select
  md5(concat(driver_id, cast(valid_from as string))) as driver_sk,
  driver_id,
  driver_name,
  employment_status,
  region,
  valid_from,
  coalesce(valid_to, '9999-12-31'::timestamp) as valid_to,
  valid_to is null as is_current
from ranked
