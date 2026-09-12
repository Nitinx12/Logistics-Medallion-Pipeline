# FreightLake

Logistics medallion lakehouse. Postgres OLTP (simulated ERP) + MongoDB
(simulated tracking feed) -> Databricks Delta bronze/silver/gold -> Postgres
serving mart -> Power BI, orchestrated by Airflow, transformed by dbt,
quality-gated by dbt tests + Great Expectations.

Full design rationale and diagrams live in `ARCHITECTURE.md`. The complete
build plan, phase order, and the "which file demonstrates which concept"
mapping live in `docs/PROJECT_PLAN.md`. Read the relevant section there
before making a design decision this file doesn't cover — don't guess.

## Tech stack

Postgres (OLTP + mart) · MongoDB · Databricks (Delta Lake) · dbt core
(databricks adapter) · PySpark · Apache Airflow 3 · Great Expectations ·
Docker Compose · `uv` for all Python environments.

## Commands

Every Makefile target is a thin wrapper around a script in `scripts/` or a
uv command — the logic lives in one place. `main.py` is the single entry
point for a full local run and runs exactly the commands the Airflow DAGs
run.

| Command | Does |
|---|---|
| `make setup` | `uv sync` + build the Docker image (`scripts/bash/bootstrap.sh`) |
| `make docker-up` / `make docker-down` | Start/stop Postgres, Mongo, Airflow |
| `make bronze` / `make silver` / `make gold` | Run one layer end to end |
| `make gx` | Great Expectations gate against Postgres |
| `make publish` | Gold -> Postgres mart publish job |
| `make pipeline` | `uv run python main.py`: docker, bronze, silver, gold, publish |
| `make dbt-docs` | `dbt docs generate` |
| `make lint` | `ruff check`, `sqlfluff lint`, `mypy` |
| `make test` | pytest |
| `make ci` | lint + test |

The `data-engineer` and `code-reviewer` subagents in `.claude/agents/`
already know the conventions below — delegate to them for implementation
and review work rather than re-explaining this file.

## Repo layout

```
main.py                single entry point for the full local pipeline
airflow/dags/          three DAGs: freightlake_bronze, _silver, _gold
src/jobs/              pg_extract_incremental, mongo_extract_incremental, publish_gold_to_postgres
src/utils/             engine config, connections, logger
dbt/models/{silver,gold}/   transformations, tagged tag:silver / tag:gold
dbt/snapshots/         customers SCD Type 2 snapshot
gx/                    Great Expectations suites + runner
sql/{oltp_schema,serving_mart,databricks}/
docker/                compose stack: Airflow 3, Postgres, MongoDB
scripts/{bash,powershell}/  paired scripts, same behavior and summary output
tests/                 pytest unit tests and GX suite tests
jars/                  JDBC, Delta, Mongo, Unity Catalog jars for Spark
docs/                  one topic per file, indexed from README.md
```

## Non negotiable conventions

- **Idempotency**: bronze reads each table incrementally using a watermark
  on `updated_at`, computed as `MAX(updated_at)` from the target Delta
  table itself — no separate watermark state table or file. Bronze lands
  rows as an append log; silver deduplicates with `ROW_NUMBER` and merges
  on the primary key. Never a full reload of a source table.
- **Layer boundaries**: bronze does type casting and landing only.
  Cleaning, dedup, and business logic belong in silver. Silver feeds a
  strict star schema in gold (`dim_*` / `fact_*`) — no new gold tables
  outside that shape.
- **SCD Type 2**: `dim_customers` only, via `dbt snapshot`
  (`dbt/snapshots/customers_snapshot.sql`) with `effective_from` /
  `effective_to` / `is_current`. Do not invent a parallel pattern to track
  history.
- **Quality gate**: the GX tasks in the silver and gold DAGs fail the DAG
  on any error severity expectation. There is no `--demo` fallback inside
  the DAGs on purpose: a failed gate must block promotion. Never lower a
  severity from `error` to `warn` to make a build pass; known gaps are
  documented in `gx/README.md`.
- **Orchestration**: exactly three Airflow DAGs — `freightlake_bronze` ->
  `freightlake_silver` -> `freightlake_gold` — chained with
  `ExternalTaskSensor`. Bronze keeps its two extraction tasks parallel.
- **Secrets**: environment variables only. `.env.example` and
  `dbt/profiles.yml` never hold real values.
- **Script pairs**: a change to `scripts/bash/*.sh` needs the matching
  `scripts/powershell/*.ps1` update, same behavior, same summary output.
  Several older bash scripts still lack their pair; do not widen the gap.
- **Style**: Python via `uv`, `ruff` + `mypy` clean. SQL via `sqlfluff`:
  root `.sqlfluff` is dialect `postgres` for `sql/`, `dbt/.sqlfluff` is
  dialect `databricks` with the dbt templater for `dbt/models/`.

Don't add tools or patterns outside this stack (no Kafka/Flink, no bare
`pip`, no row-level security) — those are documented as deliberate
non-goals in `docs/PROJECT_PLAN.md`, not gaps to fill.
