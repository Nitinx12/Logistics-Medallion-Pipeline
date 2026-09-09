# OLTP Schema — mapping to PROJECT_PLAN.md Section 5.1

Plan generic names map to logistics realistic tables actually implemented:

- `customers` -> `customers` (200 rows)
- `warehouses` -> `facilities` (50 rows, Cross-Dock, Distribution Center)
- `vehicles` -> `trucks` (120) + `trailers` (180)
- `drivers` -> `drivers` (150)
- `routes` -> `routes` (58)
- `orders` -> `loads` (85410)
- `order_items` -> `trips` (85410) + `fuel_purchases` (196442)

All tables have `updated_at TIMESTAMPTZ` for watermark incremental extraction per Section 6.

Run `psql -U postgres -d freight_lake -f sql/oltp_schema/01_customers.sql` etc or `make seed` which applies all DDL in order.
