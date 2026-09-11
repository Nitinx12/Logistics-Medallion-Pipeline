# oltp_schema

SQL files here are mounted to `/tmp/oltp_schema` in `docker/compose.yml` and applied by `docker/init/02_oltp_schema.sh` on first Postgres init.

Place OLTP DDL here, one file per table, ordered alphabetically, e.g.:

* `01_customers.sql`
* `02_trucks.sql`
* `03_loads.sql`

Each file should be idempotent (`CREATE TABLE IF NOT EXISTS`) and include `updated_at TIMESTAMPTZ` for Bronzewatermark.

See `scripts/python/seed.py` for seed data generation.

Example `01_customers.sql`:

```sql
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_name VARCHAR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```
