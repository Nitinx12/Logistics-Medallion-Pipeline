CREATE TABLE IF NOT EXISTS fuel_purchases (
  fuel_purchase_id VARCHAR(20) PRIMARY KEY,
  trip_id VARCHAR(20) REFERENCES trips(trip_id),
  truck_id VARCHAR(20) REFERENCES trucks(truck_id),
  driver_id VARCHAR(20) REFERENCES drivers(driver_id),
  purchase_date DATE,
  location_city VARCHAR(100),
  location_state VARCHAR(10),
  gallons DOUBLE PRECISION,
  price_per_gallon DOUBLE PRECISION,
  total_cost DOUBLE PRECISION,
  fuel_card_number VARCHAR(50),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fuel_purchases_updated_at ON fuel_purchases (updated_at);
