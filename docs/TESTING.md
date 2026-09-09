# Testing & CI/CD

FreightLake enforces code and data quality through local `make` targets and GitHub Actions workflows on every pull request.

---

## Running Tests Locally

Always run these before opening a pull request:

```bash
# Full check: lint + test (same as what CI runs)
make ci

# Lint only: ruff + mypy + sqlfluff
make lint

# Test only: pytest + dbt test
make test
```

---

## Linting (`make lint`)

Four linters run sequentially:

```bash
uv run ruff check spark_jobs scripts tests         # Python style
uv run ruff format --check spark_jobs scripts tests # Python formatting
uv run mypy spark_jobs --ignore-missing-imports     # Python type checking

# SQL linting by dialect
uv run sqlfluff lint sql/oltp_schema --dialect postgres
uv run sqlfluff lint sql/serving_mart --dialect postgres
uv run sqlfluff lint sql/databricks/bronze_tables.sql sql/databricks/schemas.sql --dialect databricks
uv run sqlfluff lint sql/databricks/watermark.sql --dialect postgres
uv run sqlfluff lint dbt/models --dialect databricks
```

SQLFluff configuration is in `.sqlfluff` at the project root.

---

## Testing (`make test`)

```bash
# pytest for PySpark and Python logic
uv run pytest tests -v

# dbt tests (runs against connected Databricks or skips gracefully)
bash scripts/bash/dbt.sh test  # or 'dbt test' directly
```

pytest lives in `tests/python/`. Tests cover at minimum:
- Watermark filtering logic
- Upsert/merge deduplication behavior

---

## GitHub Actions Workflows (`.github/workflows/`)

Four workflows run on every pull request to `main`:

### 1. `python-ci.yml` — Python Linting & Type Checking

```yaml
# Runs: ruff check + mypy via uv
# Triggers: PR to main
```

### 2. `sql-lint.yml` — SQL Linting

```yaml
# Runs: sqlfluff lint for postgres and databricks dialects
# Triggers: PR to main
```

### 3. `dbt-ci.yml` — dbt Build & Test

```yaml
# Runs: dbt build against a CI scratch schema
# Fails the build on any dbt test failure
# Triggers: PR to main
```

### 4. `docker-build.yml` — Docker Image Build

```yaml
# Builds the docker/Dockerfile job image
# Runs an import smoke test
# Triggers: PR to main
```

All CI badges are displayed in `README.md`.

---

## Definition of Done

A change is ready to merge when:

1. `make lint` passes locally
2. `make test` passes locally
3. Any new dbt model has `not_null` + `unique` tests on its PK, plus `relationships` tests on all FKs
4. Any new Bash script in `scripts/bash/` has a matching PowerShell script in `scripts/powershell/`
5. Documentation affected by the change is updated in the same PR
6. No secret, token, or credential appears anywhere in the diff
7. All four GitHub Actions workflows pass on the PR

---

## Quality Rules

- **Never silence a failing test** by changing severity from `error` to `warn`. Fix the root cause, or document the gap in `docs/DATA_DICTIONARY.md`.
- **Never commit directly to `main`**. Always work on a branch and open a PR.
- **Never bypass `uv`** for dependency management. Always use `uv add <package>`.
