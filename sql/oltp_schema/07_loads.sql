-- loads = orders per PROJECT_PLAN.md:110
CREATE TABLE IF NOT EXISTS loads (
    load_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES customers (customer_id),
    route_id VARCHAR(20) REFERENCES routes (route_id),
    load_date DATE,
    load_type VARCHAR(50),
    weight_lbs DOUBLE PRECISION,
    pieces INT,
    revenue DOUBLE PRECISION,
    fuel_surcharge DOUBLE PRECISION,
    accessorial_charges DOUBLE PRECISION,
    load_status VARCHAR(20),
    booking_type VARCHAR(20),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_loads_updated_at ON loads (updated_at);
CREATE INDEX IF NOT EXISTS idx_loads_customer ON loads (customer_id);
CREATE INDEX IF NOT EXISTS idx_loads_route ON loads (route_id);
