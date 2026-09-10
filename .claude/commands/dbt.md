---
description: Run dbt (build/run/test/snapshot/docs/freshness) for the FreightLake silver and gold layers
argument-hint: [build|run|test|snapshot|docs generate|source freshness] [--select ...]
allowed-tools: Bash(cd:*), Bash(dbt:*)
---

## Context

- dbt project root: `dbt/` (uses `dbt/dbt_project.yml` and
  `dbt/profiles.yml.example` / your local `profiles.yml`)
- Command: $ARGUMENTS (treat as `build` if empty)

## Task

From the `dbt/` directory, run: `dbt $ARGUMENTS`

Reference for the subcommands this project actually uses — pick the closest
match to what was asked for, or use $ARGUMENTS verbatim if it's already a
full dbt invocation:

- `dbt build` — build silver and gold models plus their tests, in dependency
  order. This is what `freightlake_silver_gold_dag` triggers in Airflow, and
  what gates silver -> gold promotion.
- `dbt run --select silver` / `dbt run --select gold` — build one layer only,
  without running tests.
- `dbt test` — run only the schema tests (not-null, unique, relationships,
  accepted-values) without rebuilding models.
- `dbt snapshot` — refresh the SCD Type 2 snapshots for `dim_driver` and
  `dim_vehicle` (`valid_from`, `valid_to`, `is_current`).
- `dbt source freshness` — check freshness on the Postgres OLTP and MongoDB
  tracking sources that feed the bronze-layer SLA.
- `dbt docs generate` — regenerate the lineage graph; cross-reference output
  against `docs/data_dictionary.md` and flag if it's gone stale.

Rules:
- If $ARGUMENTS is empty, run `dbt build`.
- Preserve whatever `--select`, `--target`, or other flags were passed after
  the subcommand.

After running, report:
- Which models built/passed vs. failed, grouped by layer (silver/gold).
- Any test failures, naming the specific test and model.
- For a `build` or `test` run, whether the project's 95% pass-rate quality
  gate for silver -> gold promotion would be met.
