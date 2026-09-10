# airflow/ — orchestration

Applies on top of the root `CLAUDE.md`. LocalExecutor, run via Docker
Compose (`docker/compose.yml`) — webserver, scheduler, and dag-processor
services (Airflow 3.x requires dag-processor as its own service).

## The three DAGs — don't add a fourth

`dags/freightlake_bronze_dag.py` -> `dags/freightlake_silver_gold_dag.py` ->
`dags/freightlake_publish_dag.py`, chained with `ExternalTaskSensor` (or
Airflow 3.x asset/dataset scheduling if updating the pattern).

1. **bronze** — both extraction jobs (`extract_postgres_oltp.py`,
   `extract_mongo_tracking.py`) run in parallel, daily. SLA: must finish by
   6 AM. Keep the SLA on this DAG specifically — it's the term-40 example
   (SLA/SLO/freshness) referenced in `docs/PROJECT_PLAN.md`.
2. **silver_gold** — triggers `dbt build` (models + tests). Must fail the
   run when the dbt test / Great Expectations quality gate doesn't pass.
   Don't catch and swallow that failure to keep the DAG green.
3. **publish** — runs `publish_gold_to_postgres.py`, only downstream of a
   successful silver_gold run.

## Rules

- Don't collapse the three-DAG split into fewer DAGs, and don't add new
  DAGs outside this chain — if a new job is needed, it belongs inside one
  of these three or as a new phase discussed in `docs/PROJECT_PLAN.md`
  first.
- Keep dbt source freshness checks wired into the bronze/silver_gold
  boundary; if the bronze SLA time changes, revisit the freshness
  thresholds in `dbt/models/sources.yml` too.
- Watch the webserver port if this stack runs alongside another local
  Airflow instance — move off 8080 rather than stopping the other project.
- DAG code changes should stay testable with `airflow dags test` locally
  before relying on the scheduler; note this in the PR description when
  changing DAG structure or dependencies.
