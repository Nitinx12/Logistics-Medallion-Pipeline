# FreightLake

Logistics lakehouse with Postgres plus MongoDB sources, bronze silver gold medallion on Databricks, dbt plus PySpark, orchestrated by Airflow, published to Postgres mart. Containerized and automated for local one command runs.

See `ARCHITECTURE.md` for diagrams and layer decisions and `docs/PROJECT_PLAN.md` for the full roadmap and the 40 term concept map.

## Quick start

```bash
cp .env.example .env   # fill Databricks values
make setup             # uv sync + hooks
make docker-up         # Postgres, Mongo, Airflow at localhost:8090
make pipeline          # seed + bronze + dbt build + publish
make dbt-docs          # lineage graph
```

PowerShell: `scripts/powershell/setup.ps1`, `seed_data.ps1`, `run_pipeline.ps1`.

## Stack

Postgres, MongoDB, Databricks Delta Lake, dbt, PySpark, Airflow, Docker, uv.

## Databricks setup

Run `make databricks-init` or execute `sql/databricks/schemas.sql` in your warehouse UI. Your `.env` already has the workspace `dbc-f19f322c-4167` configured. If you see `sql scopes` error, regenerate the token with SQL scope.

## Docs

- `docs/PROJECT_PLAN.md` — roadmap and phases
- `docs/data_dictionary.md` — table and column dictionary
- `dbt docs generate` — lineage
