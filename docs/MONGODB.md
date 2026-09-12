# MongoDB

MongoDB 7 simulates the tracking feed: high volume operational events that
arrive as documents. It is one of the two bronze sources.

## Database and collections

| Collection | Grain | Feeds silver model |
|---|---|---|
| `delivery_events` | one document per pickup or delivery milestone | `delivery_events` |
| `maintenance_records` | one document per maintenance event | `maintenance_records` |
| `safety_incidents` | one document per incident | `safety_incidents` |

Collections are discovered with `list_collection_names()` on the database
named by `MONGO_DB` (default `freight_lake`), so adding a collection
automatically includes it in the next bulk extraction run. Use
`--exclude-collections` to skip ones you do not want.

The ERP side of the simulation (customers, drivers, trucks, loads, and the
other master and transaction tables) lives in Postgres; see
`docs/POSTGRES.md`.

## Watermark field

Every collection carries an `updated_at` field used exactly like the
Postgres watermark column:

1. If the target Delta table exists, the watermark is `MAX(updated_at)`
   read from the target.
2. If it does not exist, or `--full` is passed, the load starts from
   `MIN(updated_at)` in the collection.
3. Reads use a `$match` on the `updated_at` range for the current chunk
   window, pushed down into the Mongo connector.

Collections without the field follow `--no-updated-at-mode` (`overwrite`
full snapshot replace by default, or `skip`).

## Extraction job

`src/jobs/mongo_extract_incremental.py` mirrors the Postgres job command
for command:

```bash
# all collections, warehouse writes (default mode)
uv run python -m src.jobs.mongo_extract_incremental

# one collection, local Delta, dry run
uv run python -m src.jobs.mongo_extract_incremental \
  --collection delivery_events --write-mode local --dry-run

# force a full reload
uv run python -m src.jobs.mongo_extract_incremental --full
```

Differences from the Postgres job: reads go through the Spark Mongo
connector (`jars/mongo-spark-connector_*.jar`) instead of JDBC, so
`--fetch-size` and `--num-partitions` exist for parity but are not used;
document fields are flattened into columns by the connector's inferred
schema.

## Seed behavior

`scripts/bash/reset_data.sh` destroys all Docker volumes (Postgres and
Mongo together) and starts the stack again, which reruns every
`docker-entrypoint-initdb.d` script on the empty volumes. Mongo itself
gets no schema on init; the tracking collections appear once the seed data
generators (open work, `docs/PROJECT_PLAN.md` phase 12) or your own
simulator inserts documents.

## Monitoring

- `scripts/bash/monitor_mongo.sh`: collection counts and database stats.
- Health check: `db.adminCommand('ping')` from the compose healthcheck,
  surfaced by `scripts/bash/health_check_all.sh`.
- Ad hoc: `mongosh mongodb://localhost:27017/freight_lake --eval
  'db.delivery_events.countDocuments()'`.
