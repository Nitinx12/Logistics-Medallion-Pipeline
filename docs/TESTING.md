# Testing

What is tested, how to run it, and what the full quality gate covers.

## Test inventory

| Suite | Location | Covers |
|---|---|---|
| Unit tests | `tests/unit/` | window building, watermark coercion, job config, publish logic, engine config, logger, DAG wiring |
| GX suite tests | `tests/gx_tests/` | all demo suites pass, `--bad-demo` fails as expected |
| dbt tests | `dbt/models/*/_*.yml`, `dbt/tests/generic/` | data contracts on every silver and gold model |
| dbt snapshot | `dbt/snapshots/` | SCD2 history (validated through `dim_customers` tests) |
| GX gate | `gx/run_validations.py` | the live Postgres validation the DAGs run |

Unit test highlights:

- `test_pg_extract_incremental.py` / `test_mongo_extract_incremental.py`:
  `build_windows` edge cases (start greater than end, tied timestamps,
  fractional chunk days, full versus incremental bounds), `_coerce_datetime`
  variants, job config construction.
- `test_publish_gold_to_postgres.py`: default table coverage, write mode
  resolution, JDBC URL targeting, dry run and overwrite behavior, error
  handling.
- `test_dag.py`: the three DAGs parse, bronze tasks stay parallel, the
  compose file keeps its required services.
- `test_engine.py`: required variables raise, optional ones only warn,
  write mode defaults.

## Commands

```bash
# fast local loop
uv run pytest tests/unit tests/gx_tests -q

# one file
uv run pytest tests/unit/test_pg_extract_incremental.py -q

# lint and types
uv run ruff check .
uv run sqlfluff lint
uv run mypy .

# dbt tests (needs the Databricks warehouse from .env)
cd dbt && uv run dbt test --profiles-dir .

# the whole gate, same order CI uses
scripts/bash/run_all_tests.sh
```

## run_all_tests.sh

The full gate, first failure stops the run, everything logged to
`logs/run_all_tests_<date>.log`:

```mermaid
flowchart LR
    R["ruff check ."] --> M["mypy ."] --> P["pytest"] --> D["dbt test<br/>(profiles dir dbt/)"]
    R & M & P & D -->|"any failure"| F["exit non zero,<br/>blocks merge"]

    classDef step fill:#d8dee9,stroke:#7b8894,color:#222
    classDef stop fill:#e74c3c,stroke:#922b21,color:#fff
    class R,M,P,D step
    class F stop
```

`dbt test` inside the script uses the system dbt when available and falls
back to `uv run dbt`.

## CI

GitHub Actions is planned (phase 14 in `docs/PROJECT_PLAN.md`): a pull
request workflow that runs `scripts/bash/run_all_tests.sh` plus the GX
demo validations, which need no database:

```yaml
- run: uv sync --all-extras
- run: uv run python gx/run_validations.py --demo
- run: scripts/bash/run_all_tests.sh
```

Until it is wired up, run the script locally before opening a pull request
(the definition of done in `AGENTS.md`).

## Testing conventions

- New PySpark logic needs a pytest covering at minimum the watermark
  filtering and the merge or write condition.
- New or changed dbt models need not null and unique tests on the primary
  key plus a relationships test on every foreign key.
- Never silence a failing test instead of fixing the root cause, and never
  lower a severity from error to warn to make a build pass.
- After any dbt model change, regenerate the docs (`dbt docs generate`)
  before committing so the lineage graph does not drift.
