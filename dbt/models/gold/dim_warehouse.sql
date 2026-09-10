{{ config(materialized='table') }}

/*
Conformed warehouse dimension, Type 1.
Sourced from silver stg_facilities, not bronze. Facility attributes rarely
change, so Type 1 overwrite is sufficient per refactor notes.
*/

select
    facility_id as warehouse_id,
    facility_name as warehouse_name,
    facility_type as warehouse_type,
    city,
    state,
    latitude,
    longitude,
    dock_doors,
    operating_hours
from {{ ref('stg_facilities') }}
