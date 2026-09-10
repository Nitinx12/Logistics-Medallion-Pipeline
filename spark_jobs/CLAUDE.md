# spark_jobs/ — bronze extraction and publish

Applies on top of the root `CLAUDE.md`. Shared helpers live in `utils/`:
`engine.py` (Spark session setup), `connection.py` (Postgres/Mongo
connection details, read from environment variables only), `logger.py`.

## Bronze (`bronze/extract_postgres_oltp.py`, `bronze/extract_mongo_tracking.py`)

- Read incrementally against the `freightlake.bronze.etl_watermark` table.
  Postgres job watermarks on `updated_at` via JDBC; Mongo job watermarks on
  `event_ts` via the PySpark Mongo connector.
- Write with `MERGE INTO`, keyed on the business id (Postgres) or event id
  (Mongo). This is the idempotency mechanism — a rerun must never duplicate
  or double-count rows. Don't use `overwrite` or plain `append`.
- Transformation is minimal: type casting and a `_loaded_at` audit column.
  If you're deduplicating, standardizing names, or joining across sources,
  that logic belongs in a silver dbt model instead — move it, don't add it
  here.
- Partition output by ingestion date.
- `tracking_events` has variable fields depending on device type — rely on
  Delta's schema merge/column mapping for this, don't hardcode a fixed
  schema that will break when a new field appears.
- After a successful run, advance the watermark table. A failed run must
  leave the watermark untouched so the next run retries the same window.

## Publish (`publish/publish_gold_to_postgres.py`)

- Reads gold Delta tables, upserts into the Postgres serving mart via the
  same watermark + `MERGE INTO` pattern as bronze.
- Keep it thin and fast — it exists so BI tools never query Databricks
  directly. Don't add transformation logic here; gold should already be
  BI-ready.

## Testing and style

- Unit tests for this directory live in `tests/python/`, run via
  `uv run pytest tests/python -v` (or `/test pytest`).
- `ruff` and `mypy` clean, managed through `uv`. No bare `pip install`.
- No credentials inline — everything through `utils/connection.py` reading
  environment variables (`POSTGRES_OLTP_*`, `MONGO_URI`, `DATABRICKS_*`).
