-- trips = order_items / shipment legs per PROJECT_PLAN.md:110
CREATE TABLE IF NOT EXISTS trips (
    trip_id VARCHAR(20) PRIMARY KEY,
    load_id VARCHAR(20) REFERENCES loads (load_id),
    driver_id VARCHAR(20) REFERENCES drivers (driver_id),
    truck_id VARCHAR(20) REFERENCES trucks (truck_id),
    trailer_id VARCHAR(20) REFERENCES trailers (trailer_id),
    dispatch_date DATE,
    actual_distance_miles DOUBLE PRECISION,
    actual_duration_hours DOUBLE PRECISION,
    fuel_gallons_used DOUBLE PRECISION,
    average_mpg DOUBLE PRECISION,
    idle_time_hours DOUBLE PRECISION,
    trip_status VARCHAR(20),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_trips_updated_at ON trips (updated_at);
CREATE INDEX IF NOT EXISTS idx_trips_load ON trips (load_id);
