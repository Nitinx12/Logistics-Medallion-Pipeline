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
(databricks adapter) · PySpark · Apache Airflow · Great Expectations ·
Docker Compose · `uv` for all Python environments · GitHub Actions.

## Commands

Every Makefile target is a thin wrapper around a script in `scripts/` — the
logic lives in one place. Prefer these over calling tools directly:

| Command | Does |
|---|---|
| `make setup` | `uv sync`, install pre-commit hooks |
| `make docker-up` / `make docker-down` | Start/stop Postgres, Mongo, Airflow |
| `make seed` | Generate synthetic data, load both sources |
| `make bronze` | Run both PySpark extraction jobs locally |
| `make dbt-run` | `dbt build` against silver + gold |
| `make dbt-docs` | `dbt docs generate` and serve |
| `make publish` | Run the gold -> Postgres publish job |
| `make pipeline` | Full local run: seed, bronze, dbt-run, publish |
| `make lint` | `ruff check`, `sqlfluff lint`, `mypy` |
| `make test` | pytest + dbt test + Great Expectations |
| `make ci` | Everything CI runs |

The `/lint`, `/test`, and `/dbt` slash commands in `.claude/commands/` wrap
these. The `data-engineer` and `code-reviewer` subagents in `.claude/agents/`
already know the conventions below — delegate to them for implementation and
review work rather than re-explaining this file.

## Repo layout

```
airflow/dags/        dbt/models/{silver,gold}/    spark_jobs/{bronze,publish,utils}/
sql/{oltp_schema,serving_mart,seed_data}/          great_expectations/
scripts/{bash,powershell}/  (every .sh has a matching .ps1)
tests/python/         docs/data_dictionary.md
```

## Non-negotiable conventions

- **Idempotency**: every bronze and publish write is a `MERGE INTO` upsert
  keyed on a business or event id, driven by the `etl_watermark` table.
  Never a full reload, never a plain insert/overwrite.
- **Layer boundaries**: bronze does type casting + `_loaded_at` only.
  Cleaning, dedup, and business logic belong in silver. Silver feeds a
  strict star schema in gold (`fct_*` / `dim_*`) — no new gold tables
  outside that shape.
- **SCD Type 2**: `dim_driver` and `dim_vehicle` only, via `dbt snapshot`,
  with `valid_from` / `valid_to` / `is_current`. Don't invent a parallel
  history-tracking pattern.
- **Metrics defined once**: on time delivery rate, average delivery time,
  revenue per route live only in `dbt/models/gold/metrics.yml`. Never
  recompute a metric inline elsewhere.
- **Quality gate**: 95% pass rate across dbt tests + Great Expectations
  before silver promotes to gold. Flag explicitly if a change would weaken
  this.
- **Orchestration**: exactly three Airflow DAGs — bronze -> silver_gold ->
  publish — chained by `ExternalTaskSensor`. Bronze keeps its 6 AM SLA and
  source freshness checks.
- **Secrets**: environment variables only. `.env.example` and
  `dbt/profiles.yml.example` never hold real values.
- **Script pairs**: a change to `scripts/bash/*.sh` needs the matching
  `scripts/powershell/*.ps1` update, same behavior, same summary output.
- **Style**: Python via `uv`, `ruff` + `mypy` clean. SQL via `sqlfluff`,
  dialect `postgres` for `sql/`, dialect `databricks`/`sparksql` for
  `dbt/models/`.

Don't add tools or patterns outside this stack (no Kafka/Flink, no bare
`pip`, no row-level security) — those are documented as deliberate
non-goals or stretch goals in `docs/PROJECT_PLAN.md`, not gaps to fill.
