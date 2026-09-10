---
description: Run FreightLake linting — ruff, sqlfluff, and mypy (mirrors `make lint`)
argument-hint: [ruff|sqlfluff|mypy|all]
allowed-tools: Bash(uv run:*), Bash(ruff:*), Bash(sqlfluff:*), Bash(mypy:*), Bash(make lint:*)
---

## Context

- Requested target: $ARGUMENTS (treat as `all` if empty)

## Task

Run FreightLake's lint suite for target "$ARGUMENTS". This mirrors
`make lint`, which chains three tools:

1. **ruff** — `uv run ruff check .`
   Covers `spark_jobs/`, `scripts/`, `tests/python/`, and the synthetic data
   generator.
2. **sqlfluff** — `uv run sqlfluff lint <path> --dialect <dialect>`
   Use dialect `postgres` for files under `sql/oltp_schema/` and
   `sql/serving_mart/`, and dialect `databricks` (or `sparksql`) for dbt
   models under `dbt/models/`. Lint each path with its correct dialect
   separately — don't run one dialect across the whole `sql/` and `dbt/`
   tree.
3. **mypy** — `uv run mypy spark_jobs/`
   Type checks the PySpark jobs and their shared utils.

Rules:
- If $ARGUMENTS is `all` or empty, run all three tools.
- If $ARGUMENTS names one tool (`ruff`, `sqlfluff`, or `mypy`), run only that
  one.
- Don't run autofix (`ruff check --fix`, `sqlfluff fix`) without asking first
  — report what autofix would change, then apply it only on confirmation.

After running, report remaining issues grouped by file, with the specific
rule or error code that fired for each. Summarize pass/fail per tool at the
top.
