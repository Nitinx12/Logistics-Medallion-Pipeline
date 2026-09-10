# dbt/ — silver and gold layers

Applies on top of the root `CLAUDE.md`. Source config: `dbt_project.yml`,
`profiles.yml.example` (copy to `profiles.yml` locally, never commit real
credentials), `models/sources.yml`.

## Layers

- `models/silver/` — cleaning, dedup, standardized types. Naming: `stg_*`
  for staging models. `dim_driver` and `dim_vehicle` are **snapshots**
  (SCD Type 2), not regular models — define them as `dbt snapshot` configs
  with `valid_from`, `valid_to`, `is_current`, not as incremental models
  that fake history.
- `models/gold/` — star schema only. Facts: `fct_orders`, `fct_shipments`,
  `fct_deliveries`. Dimensions: `dim_customer`, `dim_driver`, `dim_vehicle`,
  `dim_warehouse`, `dim_route`, `dim_date`. Don't add a gold model that
  isn't a fact or a conformed dimension.
- `models/gold/metrics.yml` — the only place on time delivery rate, average
  delivery time, and revenue per route are defined. If a model needs one of
  these, reference the metric, don't recompute the SQL.

## Tests

Every silver and gold model needs schema tests: not null, unique,
relationships (referential integrity to its parent dim), accepted values
where the column has a fixed domain. A new column without a test is a
review blocker, not a nice-to-have. The Great Expectations suite in
`great_expectations/` runs alongside dbt tests for statistical/format
checks (ranges, row count minimums, tracking-number regex) — dbt tests
don't replace it.

Quality gate: 95% combined pass rate (dbt tests + GX) before silver
promotes to gold. `freightlake_silver_gold_dag` fails the run below this
threshold — don't work around that in the DAG, fix the underlying test or
data issue.

## Source freshness

`models/sources.yml` defines freshness checks on both raw sources
(Postgres OLTP, Mongo tracking). Keep these in sync with the bronze DAG's
6 AM SLA — if the SLA changes, the freshness thresholds should too.

## Commands

Use the `/dbt` slash command, or directly:

```
dbt build                    # models + tests, dependency order (what CI/Airflow run)
dbt run --select silver      # one layer, no tests
dbt test                     # tests only, no rebuild
dbt snapshot                 # refresh dim_driver / dim_vehicle SCD2
dbt source freshness         # check both raw sources
dbt docs generate            # lineage graph; cross-check docs/data_dictionary.md
```

Dialect for `sqlfluff` on files in this directory: `databricks` (or
`sparksql`), not `postgres`.
