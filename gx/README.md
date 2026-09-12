# Great Expectations for FreightLake

Define rules for your data and automatically check whether the data satisfies them.

This folder is a standalone Great Expectations project at the repo root, separate from dbt tests. It mirrors the dbt silver and gold tests so you can validate both the lakehouse and Postgres serving mart with the same rules, and it runs without Databricks when you use demo or Postgres mode.

## Layout

```
gx/
  gx.yml                         File DataContext config
  expectations/                  Expectation suites, one JSON per model, mirrors dbt _silver.yml and _gold.yml
    silver_customers.json
    silver_trucks.json
    silver_trailers.json         includes WARN for 4 duplicate trailer_number
    silver_fuel_purchases.json   includes WARN for 3880 null truck_id
    silver_trips.json
    gold_dim_customers.json
    gold_dim_date.json           spine 2020 to 2030, no future date check by design, 1572 rows are expected
    gold_fact_loads.json
  checkpoints/
    silver.yml
    gold.yml
  uncommitted/                   Validation results and data docs, gitignored
  run_validations.py             Runner that works with pandas demo data or live Postgres
  README.md
```

## Install

Dependencies are managed through `uv` at the repo root.

```powershell
uv sync --group dev
# great-expectations is in [dependency-groups.dev]
uv run python -c "import great_expectations; print(great_expectations.__version__)"
```

## Quick start, no database required

```powershell
uv run python gx/run_validations.py --demo
uv run python gx/run_validations.py --demo --bad-demo
uv run python gx/run_validations.py --list
uv run python gx/run_validations.py --suite silver.customers --demo
```

Demo mode builds synthetic pandas DataFrames that satisfy each suite, evaluates the rules with a lightweight evaluator, and prints PASS or FAIL per expectation. `--bad-demo` injects a null and an out of set value to show a failing run.

## Validate live Postgres

```powershell
# reads connection from .env via src.utils.connections.get_postgres_engine
uv run python gx/run_validations.py --postgres --all
uv run python gx/run_validations.py --postgres --suite silver.fuel_purchases
uv run python gx/run_validations.py --postgres --layer silver
```

The runner tries `silver.<table>` then `gold.<table>` then `public.<table>` and limits to 10000 rows. If Postgres is unreachable it prints a SKIPPED message and exits non zero, use `--demo` in CI when no DB is available.

## Validate Databricks

Gold tables live in Databricks Unity Catalog. Add a Databricks datasource to `gx.yml` or extend `run_validations.py` with `databricks-sql-connector`. The expectation suites already cover gold models, so the same JSON can be reused once the datasource is added.

## Adding a new rule

1. Add the dbt test to `dbt/models/silver/_silver.yml` or `dbt/models/gold/_gold.yml`.
2. Add the matching GX expectation to `gx/expectations/<layer>_<model>.json`. Use type `expect_column_values_to_not_be_null`, `expect_column_values_to_be_unique`, `expect_column_values_to_be_in_set`, `expect_column_values_to_be_between`, `expect_column_values_to_match_regex`, or `expect_table_row_count_to_be_between`.
3. Run `uv run python gx/run_validations.py --demo --suite <suite>` and `dbt build` to keep both in sync.
4. Commit the JSON, do not hand edit generated data docs.

## CI

Run demo validations in CI where no warehouse is available:

```yaml
- run: uv run python gx/run_validations.py --demo
```

Run live Postgres validations in environments that expose the serving mart:

```yaml
- run: uv run python gx/run_validations.py --postgres --all
```

Severity is encoded in `meta.severity` per expectation. `warn` does not fail the runner unless you promote it, matching the dbt `severity: warn` for known issues such as `fuel_purchases.truck_id` nulls and `trailers.trailer_number` dupes and `dim_date` future rows.
