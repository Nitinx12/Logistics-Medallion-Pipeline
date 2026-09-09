-- facilities = warehouses per PROJECT_PLAN.md:110
CREATE TABLE IF NOT EXISTS facilities (
    facility_id VARCHAR(20) PRIMARY KEY,
    facility_name VARCHAR(200),
    facility_type VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(10),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    dock_doors INT,
    operating_hours VARCHAR(20),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_facilities_updated_at ON facilities (updated_at);
