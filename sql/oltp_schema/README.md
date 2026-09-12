# sql/oltp_schema

Postgres DDL for the `freightlake_oltp` source database (simulated ERP:
customers, drivers, trucks, trailers, routes, loads, trips, fuel_purchases,
delivery_events, maintenance_records, safety_incidents).

Every `*.sql` file in this folder is applied to `freightlake_oltp` in
alphabetical order on first container init by `docker/init/02_oltp_schema.sh`,
which mounts this folder at `/tmp/oltp_schema`. The script skips this folder
gracefully when it holds no `*.sql` files, so an empty folder means the OLTP
database comes up without tables, which the bronze jobs then report as empty
sources.

Seed data generators belong here too, as `NN_*.sql` files ordered after the
table DDL they populate.
