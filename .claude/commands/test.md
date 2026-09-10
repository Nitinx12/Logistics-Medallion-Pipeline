---
description: Run the FreightLake test suite — pytest, dbt test, and Great Expectations (mirrors `make test`)
argument-hint: [pytest|dbt|gx|all]
allowed-tools: Bash(uv run:*), Bash(dbt:*), Bash(great_expectations:*), Bash(cd:*), Bash(make test:*)
---

## Context

- Requested target: $ARGUMENTS (treat as `all` if empty)
- Current git status: !`git status --short`

## Task

Run the FreightLake test suite for target "$ARGUMENTS". This mirrors
`make test`, which chains three independent suites:

1. **pytest** — `uv run pytest tests/python -v`
   Unit tests covering `spark_jobs/utils/` helpers, watermark logic, and
   transformation functions.
2. **dbt test** — from the `dbt/` directory, `dbt build --select silver+ gold+`
   (or `dbt test` to run only the tests without rebuilding). Exercises the
   not-null, unique, relationships, and accepted-values tests defined on
   every silver and gold model.
3. **Great Expectations** — run the checkpoint(s) under `great_expectations/`
   for statistical/format checks: value ranges, row count minimums, and
   regex format checks on fields like tracking numbers.

Rules:
- If $ARGUMENTS is `all` or empty, run all three suites in order and stop at
  the first failing suite.
- If $ARGUMENTS names one suite (`pytest`, `dbt`, or `gx`), run only that one.
- Don't modify code to make a test pass — report the failure and, if the fix
  is obvious and safe (e.g. a stale fixture), ask before editing anything.

After running, report:
- Pass/fail counts per suite.
- For each failure: the specific test/assertion name, the file or model it
  belongs to, and the actual vs. expected output — not a raw log dump.
- Whether a failure would block the project's 95% pass-rate quality gate for
  silver -> gold promotion, called out explicitly.
