CREATE TABLE IF NOT EXISTS routes (
  route_id VARCHAR(20) PRIMARY KEY,
  origin_city VARCHAR(100),
  origin_state VARCHAR(10),
  destination_city VARCHAR(100),
  destination_state VARCHAR(10),
  typical_distance_miles DOUBLE PRECISION,
  base_rate_per_mile DOUBLE PRECISION,
  fuel_surcharge_rate DOUBLE PRECISION,
  typical_transit_days DOUBLE PRECISION,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_routes_updated_at ON routes (updated_at);
