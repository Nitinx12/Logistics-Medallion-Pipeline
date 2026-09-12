# FreightLake

Logistics medallion lakehouse. Postgres OLTP (simulated ERP) + MongoDB
(simulated tracking feed) -> Databricks Delta bronze/silver/gold -> Postgres
serving mart -> Power BI, orchestrated by Airflow, transformed by dbt,
quality gated by dbt tests plus Great Expectations.

## Tech stack

Postgres (OLTP + mart) · MongoDB · Databricks (Delta Lake) · dbt core
(databricks adapter) · PySpark · Apache Airflow 3 · Great Expectations ·
Docker Compose · `uv` for all Python environments · GitHub Actions.

## Commands

There is no Makefile; run the underlying tools directly from the repo root.

| Action | Command |
|---|---|
| First time setup | `uv sync --all-extras`, copy `.env.example` to `.env` |
| Start / stop the stack | `scripts/bash/start_stack.sh` / `stop_stack.sh` |
| Health check everything | `scripts/bash/health_check_all.sh` |
| Bronze extraction (both sources) | `uv run python -m src.jobs.pg_extract_incremental --target-schema bronze` and `uv run python -m src.jobs.mongo_extract_incremental --target-schema bronze` |
| Silver + gold build | `cd dbt && uv run dbt build --select tag:silver --profiles-dir .` (same with `tag:gold`) |
| Data quality gate | `uv run python gx/run_validations.py --postgres --all` (or `--layer silver` / `--layer gold`) |
| Publish gold to the mart | `uv run python -m src.jobs.publish_gold_to_postgres` |
| Lint | `uv run ruff check`, `uv run sqlfluff lint`, `uv run mypy` |
| Tests | `uv run pytest`, dbt tests run as part of `dbt build`, GX via the gate command above |
| Full pipeline locally | `scripts/bash/bootstrap.sh`, then seed, bronze, dbt, publish in order |

The `data-engineer` and `code-reviewer` subagents in `.claude/agents/`
already know the conventions below, delegate to them for implementation and
review work rather than re explaining this file.

## Documentation

| Document | Content |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Four Mermaid diagrams, per layer design decisions |
| [docs/PROJECT_PLAN.md](docs/PROJECT_PLAN.md) | Roadmap, tech stack, build order, 40 term concept map |
| [docs/PIPELINE.md](docs/PIPELINE.md) | Step by step pipeline flow with watermark and fallback detail |
| [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md) | Prerequisites, quick start, commands, port table |
| [docs/AIRFLOW.md](docs/AIRFLOW.md) | DAG schedules, task breakdown, Docker Airflow services |
| [docs/DATABRICKS.md](docs/DATABRICKS.md) | Catalog layout, materializations, write modes, setup steps |
| [docs/DBT.md](docs/DBT.md) | Models, SCD2 implementation, tests, commands |
| [docs/POSTGRES.md](docs/POSTGRES.md) | OLTP tables, mart schema, connection detail |
| [docs/MONGODB.md](docs/MONGODB.md) | Collections, watermark field, seed behavior |
| [docs/DATA_QUALITY.md](docs/DATA_QUALITY.md) | dbt tests, Great Expectations, quality gate logic |
| [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) | Full table and column reference for all three layers |
| [docs/TESTING.md](docs/TESTING.md) | Test inventory, commands, CI description |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Port conflicts, Databricks errors, Airflow setup, watermark resets |
| [docs/MONITORING.md](docs/MONITORING.md) | Airflow UI, logs, watermarks, Docker health |
| [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) | Branching, commit style, paired scripts rule, definition of done |
| [docs/CHANGELOG.md](docs/CHANGELOG.md) | What changed in each version |

## Repo layout

```
ARCHITECTURE.md          system views and per layer design decisions
docs/                    one topic per file: pipeline, setup, quality, dictionary, ...
airflow/dags/            three DAGs: bronze, silver, gold (chained by ExternalTaskSensor)
src/jobs/                pg_extract_incremental, mongo_extract_incremental, publish_gold_to_postgres
src/utils/               engine.py (env config), connections.py, logger.py
dbt/models/{silver,gold}/  dbt transformations, silver incremental merge, gold star schema
dbt/snapshots/           customers SCD Type 2 snapshot
dbt/tests/generic/       shared test macros (no_empty_strings, no_orphan_rows, ...)
gx/                      standalone Great Expectations project (suites + runner)
sql/oltp_schema/         Postgres DDL applied on first container init
sql/serving_mart/        mart indexes
sql/databricks/          Unity Catalog DDL
docker/                  compose stack: Airflow 3, Postgres, Mongo
scripts/bash/            operational scripts (each logs to logs/)
tests/                   pytest unit tests and GX suite tests
jars/                    JDBC, Delta, Mongo, Unity Catalog jars for Spark
```

## Non negotiable conventions

- **Idempotency**: bronze reads each table incrementally using a watermark on
  `updated_at`, computed as `MAX(updated_at)` from the target table itself,
  so a rerun never reprocesses rows it already captured. Bronze lands rows as
  an append log; silver deduplicates with `ROW_NUMBER` and merges on the
  primary key, which is what makes downstream layers idempotent. Never a
  plain full reload of a source table.
- **Layer boundaries**: bronze does type casting and landing only. Cleaning,
  dedup, and business logic belong in silver. Silver feeds a strict star
  schema in gold (`dim_*` / `fact_*`), no new gold tables outside that shape.
- **SCD Type 2**: `dim_customers` only, via `dbt snapshot`
  (`dbt/snapshots/customers_snapshot.sql`) with `effective_from` /
  `effective_to` / `is_current`. Do not invent a parallel history tracking
  pattern.
- **Quality gate**: the GX tasks in the silver and gold DAGs run against live
  Postgres and fail the DAG on any error severity expectation. There is no
  demo fallback inside the DAGs on purpose: a failed gate must block
  promotion. `warn` severity mirrors known issues documented in `gx/README.md`.
- **Orchestration**: exactly three Airflow DAGs, `freightlake_bronze` ->
  `freightlake_silver` -> `freightlake_gold`, chained with
  `ExternalTaskSensor`. Bronze keeps its two extraction tasks parallel.
- **Secrets**: environment variables only. `.env.example` and
  `dbt/profiles.yml` never hold real values.
- **Style**: Python via `uv`, `ruff` clean. SQL via `sqlfluff`, dialect
  `postgres` for `sql/`, dialect `databricks`/`sparksql` for `dbt/models/`.

Do not add tools or patterns outside this stack (no Kafka or Flink, no bare
`pip`, no row level security).
