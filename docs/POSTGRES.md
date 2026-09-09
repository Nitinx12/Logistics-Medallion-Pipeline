# PostgreSQL in FreightLake

FreightLake uses a single Postgres 16 container (`freightlake-postgres`) with three logical databases serving different roles. The container is defined in `docker/compose.yml` and exposed on port `5434` (not 5432, to avoid collisions with any local Postgres installation).

---

## Three Databases

| Database | Role | User |
|---|---|---|
| `freightlake_oltp` | Simulated ERP source system | `freightlake_oltp_user` |
| `freightlake_mart` | Gold layer serving mart for BI | `freightlake_mart_user` |
| `airflow` | Airflow metadata (internal) | `airflow` |

---

## Database 1: `freightlake_oltp` (Simulated ERP)

The OLTP database simulates a logistics company's transactional system. All DDL lives in `sql/oltp_schema/` and is applied by `scripts/seed.py` before CSV data is loaded.

### Tables

| File | Table | Description |
|---|---|---|
| `01_customers.sql` | `customers` | Customer master: company name, contact, address |
| `02_drivers.sql` | `drivers` | Driver master: name, license, employment status, home terminal — feeds `dim_driver` SCD2 |
| `03_trucks.sql` | `trucks` | Truck/vehicle master: make, model, year, status — feeds `dim_vehicle` SCD2 |
| `04_trailers.sql` | `trailers` | Trailer inventory: type, capacity, status |
| `05_facilities.sql` | `facilities` | Warehouses and terminals — feeds `dim_warehouse` |
| `06_routes.sql` | `routes` | Origin/destination pairs — feeds `dim_route` |
| `07_loads.sql` | `loads` | Order/load records with revenue, weight, pieces — feeds `fct_orders` |
| `08_trips.sql` | `trips` | Trip records linking load to driver and truck — feeds `fct_shipments` |
| `09_fuel_purchases.sql` | `fuel_purchases` | Fuel transaction records per truck |

Every table has an `updated_at TIMESTAMPTZ` column. The Bronze extractor uses this column as the watermark for incremental extraction.

### Incremental Extraction Query Pattern

```sql
SELECT * FROM <table> WHERE updated_at > '<last_watermark>'
```

### Synthetic Data Scale

| Table | Approximate Rows |
|---|---|
| customers | ~500 |
| drivers | ~500 |
| trucks | ~250 |
| trailers | ~250 |
| facilities | ~100 |
| routes | ~100 |
| loads | ~100,000 |
| trips | ~100,000 |
| fuel_purchases | ~250,000 |

---

## Database 2: `freightlake_mart` (Serving Mart)

The mart database holds the final star schema published from the Gold Delta layer. BI tools (Power BI or any SQL client) connect here, not to Databricks, for fast low-latency queries.

### Schema: `mart`

Tables are created and populated by `spark_jobs/publish/publish_gold_to_postgres.py`:

```
mart.dim_customer
mart.dim_driver
mart.dim_vehicle
mart.dim_warehouse
mart.dim_route
mart.dim_date
mart.fct_orders
mart.fct_shipments
mart.fct_deliveries
```

Column types are inferred from Parquet dtypes:
- `int*` → `BIGINT`
- `float*` → `DOUBLE PRECISION`
- `datetime*` → `TIMESTAMPTZ`
- `bool` → `BOOLEAN`
- Everything else → `TEXT`

The publish job currently drops and recreates tables on each run (idempotent, no partial state).

---

## Connection Details

From `.env.example`:

```bash
# OLTP source
POSTGRES_OLTP_URL=postgresql://freightlake_oltp_user:changeme@localhost:5432/freightlake_oltp

# Serving mart
POSTGRES_MART_URL=postgresql://freightlake_mart_user:changeme@localhost:5432/freightlake_mart
```

Connection helpers are in `spark_jobs/utils/connection.py`:
- `pg_oltp_url()` — reads `POSTGRES_OLTP_URL` from env
- `pg_mart_url()` — reads `POSTGRES_MART_URL` from env

---

## Verify Data

```bash
# Connect to OLTP
psql postgresql://postgres:changeme@localhost:5434/freightlake_oltp

# Check row counts
SELECT schemaname, tablename, n_live_tup
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;

# Connect to mart and check published tables
psql postgresql://postgres:changeme@localhost:5434/freightlake_mart
SELECT tablename FROM pg_tables WHERE schemaname='mart';
```
