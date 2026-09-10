-- FreightLake — Bronze Delta tables (Databricks)
-- Bronze mirrors the raw source types as closely as possible: columns that
-- are TEXT in the Postgres OLTP source (including every *_date and
-- updated_at column) land as STRING here, not DATE/TIMESTAMP — the cast to
-- proper types happens in the silver layer (see dbt/models/silver/), not
-- here. This keeps bronze a faithful, minimally-transformed landing zone.
--
-- Column lists below are reconciled against the real source schemas
-- (confirmed via DESCRIBE / schema introspection against Postgres and
-- Mongo), not the earlier draft. Where the earlier draft had renamed or
-- dropped columns, that's called out inline.

CREATE TABLE IF NOT EXISTS freightlake.bronze.customers (
  customer_id STRING NOT NULL,
  customer_name STRING,
  customer_type STRING,
  credit_terms_days BIGINT,
  primary_freight_type STRING,
  account_status STRING,
  contract_start_date STRING,
  annual_revenue_potential BIGINT,
  updated_at STRING,
  _loaded_at TIMESTAMP,
  _loaded_date DATE GENERATED ALWAYS AS (CAST(_loaded_at AS DATE))
) USING DELTA PARTITIONED BY (_loaded_date)
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true');

CREATE TABLE IF NOT EXISTS freightlake.bronze.drivers (
  driver_id          STRING NOT NULL,
  first_name         STRING,
  last_name          STRING,
  hire_date          STRING,
  termination_date   STRING,
  license_number     STRING,
  license_state      STRING,
  date_of_birth      STRING,
  home_terminal      STRING,
  employment_status  STRING,
  cdl_class          STRING,
  years_experience   BIGINT,
  updated_at         STRING,
  _loaded_at         TIMESTAMP
) USING DELTA;
-- NOTE: dropped the earlier draft's `license_type` — it didn't match any
-- real source column; license_number/license_state/cdl_class already cover
-- licensing. Added back termination_date, license_number, license_state,
-- date_of_birth, cdl_class, years_experience, which were missing entirely.

CREATE TABLE IF NOT EXISTS freightlake.bronze.trucks (
  truck_id               STRING NOT NULL,
  unit_number            BIGINT,
  make                   STRING,
  model_year             BIGINT,
  vin                    STRING,
  acquisition_date       STRING,
  acquisition_mileage    BIGINT,
  fuel_type              STRING,
  tank_capacity_gallons  BIGINT,
  status                 STRING,
  home_terminal          STRING,
  updated_at             STRING,
  _loaded_at             TIMESTAMP
) USING DELTA;
-- NOTE: added vin, acquisition_date, acquisition_mileage, fuel_type,
-- tank_capacity_gallons — all missing from the earlier draft. unit_number
-- corrected from STRING back to BIGINT.

CREATE TABLE IF NOT EXISTS freightlake.bronze.trailers (
  trailer_id        STRING NOT NULL,
  trailer_number     BIGINT,
  trailer_type       STRING,
  length_feet        BIGINT,
  model_year         BIGINT,
  vin                STRING,
  acquisition_date   STRING,
  status             STRING,
  current_location   STRING,
  updated_at         STRING,
  _loaded_at         TIMESTAMP
) USING DELTA;
-- NOTE: earlier draft only had trailer_id/trailer_type/status/updated_at.
-- Added trailer_number, length_feet, model_year, vin, acquisition_date,
-- current_location.

CREATE TABLE IF NOT EXISTS freightlake.bronze.facilities (
  facility_id      STRING NOT NULL,
  facility_name    STRING,
  facility_type    STRING,
  city             STRING,
  state            STRING,
  latitude         DOUBLE,
  longitude        DOUBLE,
  dock_doors       BIGINT,
  operating_hours  STRING,
  updated_at       STRING,
  _loaded_at       TIMESTAMP
) USING DELTA;
-- NOTE: dropped the earlier draft's `capacity` — not a real source column;
-- dock_doors is. Added latitude, longitude, dock_doors, operating_hours.

CREATE TABLE IF NOT EXISTS freightlake.bronze.routes (
  route_id                STRING NOT NULL,
  origin_city             STRING,
  origin_state            STRING,
  destination_city        STRING,
  destination_state       STRING,
  typical_distance_miles  BIGINT,
  base_rate_per_mile      DOUBLE,
  fuel_surcharge_rate     DOUBLE,
  typical_transit_days    BIGINT,
  updated_at              STRING,
  _loaded_at              TIMESTAMP
) USING DELTA;
-- NOTE: added fuel_surcharge_rate, typical_transit_days. Corrected
-- typical_distance_miles from DOUBLE back to BIGINT.

CREATE TABLE IF NOT EXISTS freightlake.bronze.loads (
  load_id               STRING NOT NULL,
  customer_id           STRING,
  route_id              STRING,
  load_date             STRING,
  load_type             STRING,
  weight_lbs            BIGINT,
  pieces                BIGINT,
  revenue               DOUBLE,
  fuel_surcharge        DOUBLE,
  accessorial_charges   BIGINT,
  load_status           STRING,
  booking_type          STRING,
  updated_at            STRING,
  _loaded_at            TIMESTAMP,
  _loaded_date DATE GENERATED ALWAYS AS (CAST(_loaded_at AS DATE))
) USING DELTA PARTITIONED BY (_loaded_date);
-- NOTE: only table that already had a complete column list; fixed the
-- partition bug and corrected weight_lbs/pieces/accessorial_charges back
-- to BIGINT (were DOUBLE/INT, which silently truncates on downcast).

CREATE TABLE IF NOT EXISTS freightlake.bronze.trips (
  trip_id                 STRING NOT NULL,
  load_id                 STRING,
  driver_id               STRING,
  truck_id                STRING,
  trailer_id              STRING,
  dispatch_date           STRING,
  actual_distance_miles   BIGINT,
  actual_duration_hours   DOUBLE,
  fuel_gallons_used       DOUBLE,
  average_mpg             DOUBLE,
  idle_time_hours         DOUBLE,
  trip_status             STRING,
  updated_at              STRING,
  _loaded_at              TIMESTAMP,
  _loaded_date DATE GENERATED ALWAYS AS (CAST(_loaded_at AS DATE))
) USING DELTA PARTITIONED BY (_loaded_date);
-- NOTE: earlier draft was missing actual_distance_miles,
-- actual_duration_hours, fuel_gallons_used, average_mpg, idle_time_hours,
-- trip_status — six of thirteen real columns. Fixed the partition bug too.

CREATE TABLE IF NOT EXISTS freightlake.bronze.fuel_purchases (
  fuel_purchase_id   STRING NOT NULL,
  trip_id            STRING,
  truck_id           STRING,
  driver_id          STRING,
  purchase_date      STRING,
  location_city      STRING,
  location_state     STRING,
  gallons            DOUBLE,
  price_per_gallon   DOUBLE,
  total_cost         DOUBLE,
  fuel_card_number   STRING,
  updated_at         STRING,
  _loaded_at         TIMESTAMP,
  _loaded_date DATE GENERATED ALWAYS AS (CAST(_loaded_at AS DATE))
) USING DELTA PARTITIONED BY (_loaded_date);
-- NOTE: earlier draft renamed purchase_date -> purchase_ts and typed it
-- TIMESTAMP; source has it as text under purchase_date, so reverted.
-- Added trip_id, location_city, location_state, total_cost,
-- fuel_card_number, which were missing. Fixed the partition bug.

-- Mongo collections — landed as raw string fields to match the source
-- documents; silver casts these to proper types (see silver_*.sql).
CREATE TABLE IF NOT EXISTS freightlake.bronze.delivery_events (
  _id                   STRING NOT NULL,
  event_id              STRING,
  load_id               STRING,
  trip_id               STRING,
  facility_id           STRING,
  event_type            STRING,
  scheduled_datetime    STRING,
  actual_datetime       STRING,
  on_time_flag          STRING,
  detention_minutes     STRING,
  location_city         STRING,
  location_state        STRING,
  event_ts              STRING,
  updated_at            STRING,
  _loaded_at            TIMESTAMP,
  _loaded_date DATE GENERATED ALWAYS AS (CAST(_loaded_at AS DATE))
) USING DELTA PARTITIONED BY (_loaded_date)
  TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5');
-- NOTE: earlier draft nested location as STRUCT<lat,lon,city,state> and
-- typed on_time_flag/detention_minutes as BOOLEAN/INT — none of that
-- matches the real Mongo document shape, which has flat location_city /
-- location_state strings and every field (besides _id) as string. Also
-- restored facility_id, actual_datetime, scheduled_datetime, and
-- updated_at, which were missing — updated_at's absence broke every
-- incremental merge in silver_delivery_events.sql. Dropped `status` and
-- `exception_reason`, which weren't in the schema you gave me for this
-- collection — add them back if they're real fields your introspection
-- missed (Mongo is schemaless, so that's possible).

CREATE TABLE IF NOT EXISTS freightlake.bronze.safety_incidents (
  _id                   STRING NOT NULL,
  incident_id           STRING,
  trip_id               STRING,
  truck_id              STRING,
  driver_id             STRING,
  incident_date         STRING,
  incident_type         STRING,
  description           STRING,
  at_fault_flag         STRING,
  preventable_flag      STRING,
  injury_flag           STRING,
  location_city         STRING,
  location_state        STRING,
  cargo_damage_cost     STRING,
  vehicle_damage_cost   STRING,
  claim_amount          STRING,
  event_ts              STRING,
  updated_at            STRING,
  _loaded_at            TIMESTAMP
) USING DELTA;
-- NOTE: earlier draft had only 6 of 17 real columns, plus a `severity`
-- field not present in the schema you gave me. Rebuilt to match the real
-- collection in full, including updated_at (missing before, and required
-- for the incremental merge in silver_safety_incidents.sql).

CREATE TABLE IF NOT EXISTS freightlake.bronze.maintenance_records (
  _id                   STRING NOT NULL,
  maintenance_id        STRING,
  truck_id              STRING,
  maintenance_date      STRING,
  maintenance_type      STRING,
  service_description   STRING,
  odometer_reading      STRING,
  downtime_hours        STRING,
  labor_hours           STRING,
  labor_cost            STRING,
  parts_cost            STRING,
  total_cost            STRING,
  facility_location     STRING,
  event_ts              STRING,
  updated_at            STRING,
  _loaded_at            TIMESTAMP
) USING DELTA;
-- NOTE: earlier draft used record_id/vehicle_id (renamed from the real
-- maintenance_id/truck_id) and a single `cost` column standing in for
-- three real cost fields. Reverted names to match source and silver, and
-- restored labor_cost/parts_cost/total_cost plus downtime_hours,
-- labor_hours, odometer_reading, service_description, facility_location,
-- maintenance_date, and updated_at, all missing before.