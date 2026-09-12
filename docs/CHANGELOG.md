# Changelog

Notable changes per version. Format follows Keep a Changelog; versions
follow the `pyproject.toml` version. Dates are absolute.

## 0.1.1, 2026-09-12

### Fixed

- dbt `tag:silver` and `tag:gold` selections built nothing: tags are now
  assigned in `dbt_project.yml`, so the Airflow DAGs actually build the
  layers.
- The containerized stack could not boot: Airflow containers now override
  `POSTGRES_HOST` and `MONGO_URI` with the compose service names, the
  Dockerfile gets a valid COPY and a uv managed Python 3.13 toolchain
  instead of a silent pip fallback, and the `oltp_schema` mount points at
  `sql/oltp_schema`.
- DAG task commands now resolve the repo root correctly inside the
  container, run dbt and the jobs through `uv run`, and `mart_publish`
  calls the publish job CLI instead of importing pyspark into the
  scheduler.
- The GX gate can fail again: no more `--demo` fallback in the DAGs, the
  silver gate validates every silver suite through a new `--layer` flag,
  and `run_gx.sh` calls the real runner instead of a nonexistent
  checkpoint.
- Mart connections (`dbt/profiles.yml` mart target,
  `get_postgres_engine`) target `freightlake_mart` instead of the legacy
  single database.
- Removed the squatted `mongo` and `polors` packages, which had pulled
  real polars in transitively.
- Publish unit tests updated for the repartition step added during
  performance tuning.
- DAGs chained bronze to silver to gold with `ExternalTaskSensor`.

### Added

- This documentation set: `ARCHITECTURE.md` and the fifteen files under
  `docs/`.

## 0.1.0, 2026-09-11

Initial working pipeline.

### Added

- Bronze extraction: Postgres JDBC job and Mongo connector job with
  watermark incremental loading, seven day chunk windows, parallel
  partitioned reads, and four write modes (warehouse, local, uc_managed,
  uc_external) with automatic fallback on Unity Catalog 403 errors.
- Silver layer: twelve dbt models, incremental merge on primary keys,
  deduplication, full schema test coverage in `_silver.yml`, six generic
  test macros made Databricks compatible.
- Gold layer: star schema (seven dimensions, seven facts), SCD Type 2 on
  customers via dbt snapshot, UNKNOWN member rows, `dim_date` spine,
  `fact_operations` daily aggregate with weighted average MPG.
- Great Expectations: eight suites mirroring the dbt tests, a runner with
  demo, Postgres, and layer modes.
- Airflow: three DAGs with parallel bronze extraction, sensor chaining,
  GX gates, and mart publish.
- Docker stack: Airflow 3 (apiserver, scheduler, dag-processor),
  Postgres with the three database init scripts, MongoDB.
- Operational scripts under `scripts/bash/` covering bootstrap, stack
  lifecycle, per layer runs, the full test gate, monitors, and a
  destructive data reset.
- `uv` managed environment, `src/utils` config and connection modules
  with fail fast validation, stage scoped logging to `logs/`.
- Serving mart with indexes, Unity Catalog schema DDL.

## Pre history

The branch `refactor/2026-09-18-recreate` rebuilds the project from the
original FreightLake prototype; the prototype's history is preserved on
the older feature branches (`feature/scaffold-databricks`,
`feature/silver-data-quality`, `fix/jars-check`, `fix/audit-phases-0-10`).
