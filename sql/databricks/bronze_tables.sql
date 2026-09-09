-- FreightLake — Bronze Delta tables (Databricks)
-- These mirror the 9 Postgres OLTP tables + 3 Mongo collections.
-- All tables are partitioned by ingestion date and support schema evolution.

CREATE TABLE IF NOT EXISTS freightlake.bronze.customers (
  customer_id STRING NOT NULL,
  customer_name STRING,
  customer_type STRING,
  credit_terms_days INT,
  primary_freight_type STRING,
  account_status STRING,
  contract_start_date DATE,
  annual_revenue_potential DOUBLE,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA PARTITIONED BY (_loaded_date)
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true', 'delta.autoOptimize.autoCompact'='true');

CREATE TABLE IF NOT EXISTS freightlake.bronze.drivers (
  driver_id STRING NOT NULL,
  driver_name STRING,
  region STRING,
  employment_status STRING,
  license_type STRING,
  hire_date DATE,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS freightlake.bronze.trucks (
  truck_id STRING NOT NULL,
  truck_model STRING,
  capacity_lbs INT,
  status STRING,
  facility_id STRING,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS freightlake.bronze.trailers (
  trailer_id STRING NOT NULL,
  trailer_type STRING,
  status STRING,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS freightlake.bronze.facilities (
  facility_id STRING NOT NULL,
  facility_name STRING,
  facility_type STRING,
  region STRING,
  capacity INT,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS freightlake.bronze.routes (
  route_id STRING NOT NULL,
  origin_facility_id STRING,
  dest_facility_id STRING,
  distance_miles DOUBLE,
  estimated_hours DOUBLE,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS freightlake.bronze.loads (
  load_id STRING NOT NULL,
  customer_id STRING,
  route_id STRING,
  load_date DATE,
  load_type STRING,
  weight_lbs DOUBLE,
  pieces INT,
  revenue DOUBLE,
  fuel_surcharge DOUBLE,
  accessorial_charges DOUBLE,
  load_status STRING,
  booking_type STRING,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA PARTITIONED BY (_loaded_date);

CREATE TABLE IF NOT EXISTS freightlake.bronze.trips (
  trip_id STRING NOT NULL,
  load_id STRING,
  driver_id STRING,
  truck_id STRING,
  trailer_id STRING,
  departure_ts TIMESTAMP,
  arrival_ts TIMESTAMP,
  status STRING,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA PARTITIONED BY (_loaded_date);

CREATE TABLE IF NOT EXISTS freightlake.bronze.fuel_purchases (
  fuel_purchase_id STRING NOT NULL,
  truck_id STRING,
  driver_id STRING,
  purchase_ts TIMESTAMP,
  gallons DOUBLE,
  price_per_gallon DOUBLE,
  updated_at TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA PARTITIONED BY (_loaded_date);

-- Mongo collections — semi structured, schema evolution via delta column mapping
CREATE TABLE IF NOT EXISTS freightlake.bronze.delivery_events (
  event_id STRING NOT NULL,
  load_id STRING,
  trip_id STRING,
  event_ts TIMESTAMP,
  event_type STRING,
  status STRING,
  location STRUCT<lat:DOUBLE, lon:DOUBLE, city:STRING, state:STRING>,
  exception_reason STRING,
  _loaded_at TIMESTAMP
) USING DELTA PARTITIONED BY (_loaded_date)
  TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5');

CREATE TABLE IF NOT EXISTS freightlake.bronze.safety_incidents (
  incident_id STRING NOT NULL,
  driver_id STRING,
  incident_ts TIMESTAMP,
  incident_type STRING,
  severity STRING,
  event_ts TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS freightlake.bronze.maintenance_records (
  record_id STRING NOT NULL,
  vehicle_id STRING,
  maintenance_type STRING,
  cost DOUBLE,
  event_ts TIMESTAMP,
  _loaded_at TIMESTAMP
) USING DELTA;
