# Project Plan

Roadmap, build order, and the concept map: which file demonstrates which
data engineering idea. Architecture rationale lives in `ARCHITECTURE.md`.

## Status snapshot (2026-09-12)

| Phase | Scope | Status |
|---|---|---|
| 0 | Environment, `uv`, config validation (`src/utils/`) | done |
| 1 | Spark jars and session setup (`jars/`, `build_spark_session`) | done |
| 2 | Bronze Postgres extract with watermark and chunking | done |
| 3 | Bronze write modes: warehouse, local, uc fallback | done |
| 4 | Bronze Mongo extract, parity with the Postgres job | done |
| 5 | Silver dbt models with full data quality yml | done |
| 6 | Gold star schema, SCD2 snapshot, operations aggregate | done |
| 7 | Great Expectations suites and runner | done |
| 8 | Airflow: three DAGs, parallel bronze, sensor chaining | done |
| 9 | Docker stack: Airflow 3, Postgres, Mongo | done |
| 10 | Operational scripts and SQL artifacts | done |
| 11 | Documentation set (this folder, `ARCHITECTURE.md`) | done |
| 12 | OLTP DDL and seed data in `sql/oltp_schema/` | open |
| 13 | PowerShell counterparts for `scripts/bash/` | open |
| 14 | GitHub Actions CI wiring for `run_all_tests.sh` | open |
| 15 | Repo wide ruff and mypy cleanup | open |

## Tech stack

| Concern | Choice | Where configured |
|---|---|---|
| Language runtime | Python 3.13, `uv` managed | `pyproject.toml`, `.python-version` |
| Distributed processing | PySpark 4.2, local mode | `src/jobs/` |
| Lakehouse storage | Delta Lake 4.4 jars | `jars/` |
| Catalog | Databricks Unity Catalog | `dbt/profiles.yml`, `sql/databricks/schema.sql` |
| Transformation | dbt core with the databricks adapter | `dbt/` |
| Sources | Postgres 16 (OLTP), MongoDB 7 | `docker/compose.yml` |
| Serving mart | Postgres 16, `freightlake_mart` database | `docker/init/01_create_database.sql` |
| Orchestration | Apache Airflow 3, LocalExecutor | `airflow/dags/`, `docker/compose.yml` |
| Data quality | dbt tests + Great Expectations 1.x | `dbt/models/`, `gx/` |
| Containerization | Docker Compose | `docker/compose.yml` |
| Lint and types | ruff, sqlfluff, mypy | `pyproject.toml` |

## Build order rationale

The phases above were executed in order for a reason:

1. **Config before code.** `src/utils/engine.py` validates every required
   environment variable at import time, so every later job fails fast with
   a clear message instead of a deep driver error.
2. **Extraction before transformation.** Bronze had to prove it could land
   data from both sources idempotently before dbt had anything to build on.
3. **Write modes before scale tuning.** The Unity Catalog 403 wall was hit
   early; solving it produced the `BRONZE_WRITE_MODE` routing that every
   later writer reuses.
4. **Quality before gold.** Silver got its full test yml before the star
   schema existed, so gold inherited testable inputs.
5. **Orchestration last.** The DAGs wrap commands that were already proven
   to work standalone; orchestration adds scheduling, not correctness.

## Concept map

Forty data engineering concepts and the file that demonstrates each one.

| # | Concept | Where it lives |
|---|---|---|
| 1 | Medallion architecture | repo layout: `dbt/models/{bronze,silver,gold}` |
| 2 | Watermark incremental extraction | `src/jobs/pg_extract_incremental.py`, `resolve_watermark` |
| 3 | Idempotency on rerun | exclusive lower bound in `build_windows` plus silver merge |
| 4 | MERGE upsert | silver `incremental_strategy: merge`; warehouse `MERGE INTO` in `write_chunk` |
| 5 | Append log raw layer | bronze default `--mode append` |
| 6 | Time window chunking | `build_windows`, `--chunk-days 7` |
| 7 | Partitioned JDBC reads | `read_window`, partitionColumn with `--num-partitions 4` |
| 8 | Fetch size tuning | `--fetch-size 50000` |
| 9 | Delta Lake writes | `df.write.format("delta")` paths in both extract jobs |
| 10 | Unity Catalog integration | `spark.sql.catalog.<name>` config in `build_spark_session` |
| 11 | Managed versus external tables | `BRONZE_WRITE_MODE` options in `.env.example` |
| 12 | SQL warehouse as a write path | `_write_via_warehouse` |
| 13 | Handling cloud permission errors | `_is_uc_managed_blocked` (403, ErrorCode 5108/5105) |
| 14 | dbt incremental models | silver model configs, `is_incremental()` |
| 15 | dbt snapshots | `dbt/snapshots/customers_snapshot.sql` |
| 16 | SCD Type 2 | `dim_customers` with `effective_from`/`effective_to`/`is_current` |
| 17 | Surrogate keys | `SHA2(CONCAT_WS(...), 256)` in gold models |
| 18 | Star schema | `dbt/models/gold/` |
| 19 | Fact grain declaration | model descriptions in `_gold.yml` |
| 20 | Unknown member pattern | UNION ALL UNKNOWN row in every dimension |
| 21 | Date dimension spine | `dim_date`, 2020 through 2030 |
| 22 | Conformed dimensions | `dim_date` keys in every fact |
| 23 | Generic dbt tests | `dbt/tests/generic/` |
| 24 | Relationship integrity tests | `relationships` plus `no_orphan_rows` in the yml files |
| 25 | Test severity levels | `severity: error` versus `warn` throughout the yml files |
| 26 | Expectation suites | `gx/expectations/*.json` |
| 27 | Data quality gate | `gx_silver` and `gx_gold` DAG tasks |
| 28 | DAG orchestration | `airflow/dags/freightlake_*.py` |
| 29 | Cross DAG dependencies | `ExternalTaskSensor` in silver and gold |
| 30 | Parallel task fan out | `bronze_pg` and `bronze_mongo` side by side |
| 31 | Containerized services | `docker/compose.yml` |
| 32 | Airflow 3 service split | apiserver, scheduler, dag-processor services |
| 33 | Fail fast configuration | required variable checks in `src/utils/engine.py` |
| 34 | Secrets through environment | `.env.example`, `env_var()` in `dbt/profiles.yml` |
| 35 | Lockfile dependency management | `uv.lock`, uv only rule in `AGENTS.md` |
| 36 | Dependency supply chain hygiene | removal of the squatted `mongo` and `polors` packages |
| 37 | Serving mart with indexes | `sql/serving_mart/indexes.sql` |
| 38 | Bulk overwrite publishing | `publish_gold_to_postgres.py` truncate plus insert |
| 39 | Weighted average versus average of ratios | `fact_operations` `avg_mpg` |
| 40 | Operational runbooks | `scripts/bash/` |

## Open work priorities

1. **OLTP DDL and seed data** (phase 12) is the blocking gap: without it a
   fresh clone has empty sources and every downstream layer builds nothing.
2. **CI wiring** (phase 14): `scripts/bash/run_all_tests.sh` is the full
   gate; it needs a GitHub Actions workflow that runs it on pull requests.
3. **PowerShell script pairs** (phase 13) per the paired scripts rule in
   `AGENTS.md`.
4. **Lint cleanup** (phase 15): the repo carries pre-existing ruff findings,
   concentrated in naive datetime usage and broad exception handling.
