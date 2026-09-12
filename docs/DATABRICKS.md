# Databricks

Unity Catalog layout, materialization strategy per layer, and how the
project talks to Databricks from outside its compute.

## Catalog layout

`sql/databricks/schema.sql` creates the whole layout:

```text
catalog freightlake
  schema bronze       raw landing tables, one per source table
  schema silver       cleaned dbt models
  schema gold         star schema tables
  schema snapshots    dbt snapshot state for customers SCD2
  schema mart         reserved for mart related objects
```

The catalog name is configurable through `DATABRICKS_CATALOG` (default
`freightlake`); dbt reads it through `env_var('DATABRICKS_CATALOG')` in
`dbt/profiles.yml` and `dbt/dbt_project.yml`, and the extraction jobs
default their `--target-catalog` to it.

## Materialization per layer

| Layer | Materialization | Why |
|---|---|---|
| bronze | Delta tables written by the Spark jobs | append log semantics, watermark read back from the table |
| silver | `incremental` with `incremental_strategy: merge` | upsert on primary key, three day lookback |
| gold | `table` | full rebuild each run keeps the star schema trivially consistent |
| snapshots | `snapshot` (timestamp strategy) | SCD2 history for customers |
| mart (Postgres) | tables, truncate plus insert per publish | gold is already a full rebuild, so the publisher mirrors it |

## Connections

Two distinct paths, both configured only through environment variables:

1. **Spark sessions** (`build_spark_session` in each job) register a
   `UCSingleCatalog` catalog plugin pointing at the workspace UC API, with
   the Postgres JDBC, Mongo connector, Delta and Unity Catalog jars from
   `jars/` attached.
2. **SQL warehouse** (`get_databricks_connection` in
   `src/utils/connections.py`, via `databricks-sql-connector`) is what the
   `warehouse` write mode and the watermark checks use. Host, HTTP path
   and token come from `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`,
   `DATABRICKS_TOKEN`.

## Write modes and the 403 wall

Databricks blocks managed table creation and staging credential handout
from outside its own compute: direct staged writes fail with HTTP 403,
ErrorCode 5108 (`createStagingTable`) or 5105 (`getTableCredentials`), or
`UNITY_CATALOG_EXTERNAL_CREATE_TABLE_REQUEST_FOR_NON_EXTERNAL_TABLE_DENIED`.
The jobs therefore never assume direct writes:

| Mode | Path | Use when |
|---|---|---|
| `warehouse` (default) | SQL warehouse `CREATE TABLE` plus batched `INSERT` (5000 rows per statement) and `MERGE INTO` for merge mode | running from a laptop, the only verified path from outside |
| `local` | Delta files under `BRONZE_LOCAL_PATH` | offline development, no Unity Catalog needed |
| `uc_managed` | `UCSingleCatalog` `saveAsTable` | only inside Databricks compute |
| `uc_external` | external location tables, needs `DATABRICKS_EXTERNAL_LOCATION` plus a storage credential | external locations are set up in the workspace |

On a 403 the jobs fall back from the UC paths to the warehouse path and
log a warning; set `BRONZE_WRITE_MODE=warehouse` or `local` up front to
skip the failed attempt entirely.

## One time setup in the workspace

1. Create a SQL warehouse (Serverless works) and note its HTTP path.
2. Generate a personal access token.
3. Run `sql/databricks/schema.sql` (from a warehouse query tab or the
   editor) to create the catalog and schemas; grant yourself
   `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY` on each schema.
4. Fill `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`,
   `DATABRICKS_CATALOG=freightlake` in `.env`.
5. Verify with a dry run:
   `uv run python -m src.jobs.pg_extract_incremental --dry-run`.

## dbt targets

`dbt/profiles.yml` defines three targets, all reading credentials from
environment variables only:

| Target | Adapter | Use |
|---|---|---|
| `dev` (default) | databricks | silver and gold builds against the warehouse |
| `prod` | databricks | same shape, reserved for CI or a production schedule |
| `mart` | postgres | dbt access to the local serving mart, schema `gold` |

The `on-run-start` hook runs the `create_udfs` macro (currently a
placeholder `SELECT 1`) only when the target type is databricks.
