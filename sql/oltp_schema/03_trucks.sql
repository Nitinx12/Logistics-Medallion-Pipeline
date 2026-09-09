-- FreightLake OLTP — trucks (vehicles)
CREATE TABLE IF NOT EXISTS trucks (
    truck_id VARCHAR(20) PRIMARY KEY,
    unit_number VARCHAR(20),
    make VARCHAR(100),
    model_year INT,
    vin VARCHAR(50),
    acquisition_date DATE,
    acquisition_mileage INT,
    fuel_type VARCHAR(20),
    tank_capacity_gallons INT,
    status VARCHAR(20),
    home_terminal VARCHAR(50),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_trucks_updated_at ON trucks (updated_at);
CREATE INDEX IF NOT EXISTS idx_trucks_status ON trucks (status);
