# Monitoring FreightLake

FreightLake has several surfaces for monitoring the health of the pipeline and data quality.

---

## 1. Airflow UI

**URL**: `http://localhost:8090` (admin / changeme by default)

The three DAGs visible in the UI are:

| DAG | Schedule | What to watch |
|---|---|---|
| `freightlake_bronze_dag` | 04:00 UTC daily | Both tasks should run in parallel and finish within the 2-hour SLA |
| `freightlake_silver_gold_dag` | 06:00 UTC daily | `dbt_build` task — check logs for test failures |
| `freightlake_publish_dag` | 07:00 UTC daily | `publish_gold_to_postgres` task |

### Reading Task Logs in Airflow

1. Click a DAG → click a run → click a task → **Logs** tab
2. Bronze task logs show the watermark values and row/doc counts per table
3. dbt task logs show model compilation and test results inline
4. Publish task logs show rows inserted per mart table

### SLA Miss Alerts

The Bronze DAG has a 2-hour SLA (`sla=timedelta(hours=2)` in `default_args`). Airflow will flag a miss in the UI if extraction takes longer than expected.

---

## 2. Application Logs (`logs/`)

Every PySpark job writes structured logs to `logs/<stage>.log` via `spark_jobs/utils/logger.py`.

| Log File | Written By |
|---|---|
| `logs/bronze.log` | `extract_postgres_oltp.py`, `extract_mongo_tracking.py` |

**Log format:**
```
2026-09-09 04:01:23,456 | INFO | spark_jobs.bronze.extract_postgres_oltp | Extracting postgres table=loads watermark 2026-09-08T12:00:00
2026-09-09 04:01:24,789 | INFO | spark_jobs.bronze.extract_postgres_oltp |   loads MERGE 1234 rows (upsert on load_id) -> delta/bronze/loads.parquet
```

Tail live during a run:
```bash
tail -f logs/bronze.log
```

---

## 3. Watermarks (`watermarks.json`)

The watermark file shows the last successfully extracted timestamp for every source table and collection. If a watermark is not advancing, extraction is either finding no new rows or failing silently.

```bash
cat watermarks.json
```

Expected output after a successful run:
```json
{
  "pg:customers": "2026-09-08T12:00:00",
  "pg:loads": "2026-09-08T12:00:00",
  "mongo:delivery_events": "2026-09-08T11:59:58"
}
```

---

## 4. dbt Docs & Lineage

```bash
make dbt-docs
# Opens dbt/target/index.html
```

The lineage graph shows the full DAG from Bronze sources through Silver and Gold. Use it to:
- Trace where a bad value originated
- Verify all tests are passing (shown as green in the lineage graph)
- See source freshness status

---

## 5. Postgres Mart Verification

After the publish step:

```bash
# Count rows per mart table
psql postgresql://postgres:changeme@localhost:5434/freightlake_mart \
  -c "SELECT schemaname, tablename, n_live_tup FROM pg_stat_user_tables WHERE schemaname='mart' ORDER BY n_live_tup DESC"
```

---

## 6. Docker Container Health

```bash
# Check all container statuses
docker compose -f docker/compose.yml ps

# Stream all container logs
docker compose -f docker/compose.yml logs -f

# Check one specific service
docker compose -f docker/compose.yml logs airflow-scheduler
```
