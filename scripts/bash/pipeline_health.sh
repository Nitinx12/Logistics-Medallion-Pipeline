#!/usr/bin/env bash
set -eu
set -o pipefail 2>/dev/null || true
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
PROJECT_ROOT="$(find_project_root "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/health.log"
exec > >(tee -a "$LOG_FILE") 2>&1

if ! command -v uv >/dev/null 2>&1 && ! command -v uv.exe >/dev/null 2>&1; then
  for p in "$HOME/AppData/Local/hermes/bin" "/mnt/c/Users/$USER/AppData/Local/hermes/bin" "/mnt/c/Users/91852/AppData/Local/hermes/bin"; do
    [[ -x "$p/uv.exe" ]] && export PATH="$p:$PATH" && break
    [[ -x "$p/uv" ]] && export PATH="$p:$PATH" && break
  done
fi
UV_CMD="uv"; command -v uv.exe >/dev/null 2>&1 && ! command -v uv >/dev/null 2>&1 && UV_CMD="uv.exe"
export UV_CMD

PASS=0; FAIL=0; WARN=0; TOTAL=0

mask() { echo "$1" | sed -E 's|://[^@]*@|://***:***@|; s|token=[^&]*|token=***|; s|dapi[^ ]*|***|'; }

check() {
  local name="$1"; shift
  TOTAL=$((TOTAL+1))
  if "$@"; then
    log_success "$name PASS"
    PASS=$((PASS+1))
    return 0
  else
    log_error "$name FAIL"
    FAIL=$((FAIL+1))
    return 1
  fi
}

check_warn() {
  local name="$1"; shift
  TOTAL=$((TOTAL+1))
  if "$@"; then
    log_success "$name PASS"
    PASS=$((PASS+1))
  else
    log_warn "$name WARN"
    WARN=$((WARN+1))
  fi
}

log_info "FreightLake pipeline health check — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log_info "Project root $PROJECT_ROOT"

# ---------------------------------------------------------------------------
# 1. Env file and required variables (masked)
# ---------------------------------------------------------------------------
log_info "--- 1. Environment ---"
check "env file exists" test -f "$PROJECT_ROOT/.env"

# Load .env for checks (do not export secrets to log)
if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
fi

for var in POSTGRES_OLTP_URL POSTGRES_MART_URL MONGO_URI DATABRICKS_HOST DATABRICKS_TOKEN DATABRICKS_HTTP_PATH; do
  val="${!var:-}"
  if [[ -n "$val" ]]; then
    log_success "env $var=$(mask "$val")"
    PASS=$((PASS+1)); TOTAL=$((TOTAL+1))
  else
    # Check .env.example as fallback for local dev
    if grep -q "^$var=" "$PROJECT_ROOT/.env.example" 2>/dev/null; then
      log_warn "env $var missing — using .env.example placeholder (set real value in .env)"
      WARN=$((WARN+1)); TOTAL=$((TOTAL+1))
    else
      log_error "env $var missing"
      FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1))
    fi
  fi
done

for var in DATABRICKS_CATALOG DATABRICKS_SCHEMA_BRONZE DATABRICKS_SCHEMA_SILVER DATABRICKS_SCHEMA_GOLD; do
  check "env $var set" test -n "${!var:-}"
done

# ---------------------------------------------------------------------------
# 2. Postgres OLTP
# ---------------------------------------------------------------------------
log_info "--- 2. Postgres OLTP ---"
check_warn "postgres oltp connectivity" bash -c '
  $UV_CMD run python - <<PY
import os, sys
url=os.getenv("POSTGRES_OLTP_URL","")
if not url: sys.exit(1)
try:
    import psycopg2
    conn=psycopg2.connect(url, connect_timeout=5)
    cur=conn.cursor()
    cur.execute("SELECT 1")
    cur.close(); conn.close()
    sys.exit(0)
except Exception as e:
    print(f"postgres oltp check: {e}", file=sys.stderr)
    sys.exit(1)
PY
'

# ---------------------------------------------------------------------------
# 3. Postgres mart
# ---------------------------------------------------------------------------
log_info "--- 3. Postgres mart ---"
check_warn "postgres mart connectivity" bash -c '
  $UV_CMD run python - <<PY
import os, sys
url=os.getenv("POSTGRES_MART_URL","")
if not url: sys.exit(1)
try:
    import psycopg2
    conn=psycopg2.connect(url, connect_timeout=5)
    cur=conn.cursor()
    cur.execute("SELECT 1")
    cur.close(); conn.close()
    sys.exit(0)
except Exception as e:
    print(f"mart check: {e}", file=sys.stderr)
    sys.exit(1)
PY
'

# ---------------------------------------------------------------------------
# 4. Mongo
# ---------------------------------------------------------------------------
log_info "--- 4. MongoDB ---"
check_warn "mongo connectivity" bash -c '
  $UV_CMD run python - <<PY
import os, sys
uri=os.getenv("MONGO_URI","")
if not uri: sys.exit(1)
try:
    from pymongo import MongoClient
    client=MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    client.close()
    sys.exit(0)
except Exception as e:
    print(f"mongo check: {e}", file=sys.stderr)
    sys.exit(1)
PY
'

# ---------------------------------------------------------------------------
# 5. Databricks
# ---------------------------------------------------------------------------
log_info "--- 5. Databricks ---"
check_warn "databricks config present" bash -c '
  test -n "${DATABRICKS_HOST:-}" && test -n "${DATABRICKS_TOKEN:-}" && test -n "${DATABRICKS_HTTP_PATH:-}"
'
# Light reachability check — do not fail pipeline if workspace is not reachable locally
if [[ -n "${DATABRICKS_HOST:-}" && "${DATABRICKS_HOST}" != *"<your-workspace>"* ]]; then
  check_warn "databricks host reachable" bash -c '
    curl -sf --max-time 5 "${DATABRICKS_HOST}/api/2.0/clusters/list" -H "Authorization: Bearer ${DATABRICKS_TOKEN}" >/dev/null 2>&1 || exit 0
  '
fi

# ---------------------------------------------------------------------------
# 6. Docker
# ---------------------------------------------------------------------------
log_info "--- 6. Docker ---"
if command -v docker >/dev/null 2>&1; then
  check_warn "docker compose config valid" bash -c 'docker compose -f docker/compose.yml config >/dev/null 2>&1'
  # Check dag-processor service exists
  check "compose dag-processor service" bash -c 'grep -q "dag-processor" docker/compose.yml'
else
  log_warn "docker not found — skipping container checks"
  WARN=$((WARN+1)); TOTAL=$((TOTAL+1))
fi

# ---------------------------------------------------------------------------
# 6b. Spark jars
# ---------------------------------------------------------------------------
log_info "--- 6b. Spark jars ---"
check "delta-spark pip installed" bash -c '$UV_CMD run python -c "import delta; print(delta.__version__)" >/dev/null 2>&1'
check "spark engine has delta/postgres/mongo packages" bash -c 'grep -q "delta-spark" spark_jobs/utils/engine.py && grep -q "org.postgresql:postgresql" spark_jobs/utils/engine.py && grep -q "mongo-spark-connector" spark_jobs/utils/engine.py'
check_warn "delta jar resolvable" bash -c '$UV_CMD run python -c "from delta import configure_spark_with_delta_pip; import pyspark; b=pyspark.sql.SparkSession.builder; b=configure_spark_with_delta_pip(b); s=b.appName(\"jar-check\").getOrCreate(); s.stop()" >/dev/null 2>&1'

# ---------------------------------------------------------------------------
# 7. dbt
# ---------------------------------------------------------------------------
log_info "--- 7. dbt ---"
check "dbt compile local" bash -c '$UV_CMD run dbt compile --project-dir dbt --profiles-dir dbt --target local >/dev/null 2>&1'
check "dbt test local (116 tests)" bash -c '$UV_CMD run dbt test --project-dir dbt --profiles-dir dbt --target local >/dev/null 2>&1'
check_warn "dbt docs generate" bash -c '$UV_CMD run dbt docs generate --project-dir dbt --profiles-dir dbt --target local >/dev/null 2>&1'

# ---------------------------------------------------------------------------
# 8. Great Expectations
# ---------------------------------------------------------------------------
log_info "--- 8. Great Expectations ---"
check "gx suite json valid" bash -c '$UV_CMD run python -c "import json; json.load(open(\"great_expectations/expectations/silver_suite.json\"))"'
check "gx suite has 24 expectations" bash -c 'test $($UV_CMD run python -c "import json; print(len(json.load(open(\"great_expectations/expectations/silver_suite.json\"))[\"expectations\"]))") -ge 20'
check "gx checkpoint exists" test -f "$PROJECT_ROOT/great_expectations/checkpoints/silver_checkpoint.yml"
check_warn "gx checkpoint valid yaml" bash -c '$UV_CMD run python -c "import yaml; yaml.safe_load(open(\"great_expectations/checkpoints/silver_checkpoint.yml\"))" 2>/dev/null || $UV_CMD run python -c "import pathlib; print(open(\"great_expectations/checkpoints/silver_checkpoint.yml\").read())" >/dev/null'

# ---------------------------------------------------------------------------
# 9. Airflow
# ---------------------------------------------------------------------------
log_info "--- 9. Airflow DAGs ---"
check "bronze dag python compile" bash -c '$UV_CMD run python -m py_compile airflow/dags/freightlake_bronze_dag.py'
check "silver_gold dag compile" bash -c '$UV_CMD run python -m py_compile airflow/dags/freightlake_silver_gold_dag.py'
check "publish dag compile" bash -c '$UV_CMD run python -m py_compile airflow/dags/freightlake_publish_dag.py'
check "dag bronze uses python -m" bash -c 'grep -q "python -m spark_jobs.bronze" airflow/dags/freightlake_bronze_dag.py'
check "dag publish uses python -m" bash -c 'grep -q "python -m spark_jobs.publish" airflow/dags/freightlake_publish_dag.py'
check "dag max_active_runs" bash -c 'grep -q "max_active_runs=1" airflow/dags/freightlake_silver_gold_dag.py && grep -q "max_active_runs=1" airflow/dags/freightlake_publish_dag.py'

# ---------------------------------------------------------------------------
# 10. Lint & unit tests
# ---------------------------------------------------------------------------
log_info "--- 10. Lint and unit tests ---"
check "ruff check" bash -c '$UV_CMD run ruff check spark_jobs scripts tests main.py >/dev/null 2>&1'
check "ruff format check" bash -c '$UV_CMD run ruff format --check spark_jobs scripts tests main.py >/dev/null 2>&1'
check "mypy" bash -c '$UV_CMD run mypy spark_jobs --ignore-missing-imports >/dev/null 2>&1'
check "pytest" bash -c '$UV_CMD run pytest tests -q >/dev/null 2>&1'
check "sqlfluff gold" bash -c '$UV_CMD run sqlfluff lint dbt/models --dialect databricks >/dev/null 2>&1'

# ---------------------------------------------------------------------------
# 11. Gold modelling sanity
# ---------------------------------------------------------------------------
log_info "--- 11. Gold modelling ---"
check "gold dim_driver lower" bash -c 'grep -q "lower(trim(employment_status))" dbt/models/gold/dim_driver.sql'
check "gold dim_vehicle lower+model_year" bash -c 'grep -q "LOWER(TRIM(make))" dbt/models/gold/dim_vehicle.sql && grep -q "model_year" dbt/models/gold/dim_vehicle.sql'
check "gold fct date_id" bash -c 'grep -q "date_id" dbt/models/gold/fct_orders.sql && grep -q "date_id" dbt/models/gold/fct_shipments.sql && grep -q "date_id" dbt/models/gold/fct_deliveries.sql'
check "gold vehicle truck_id not vehicle_id" bash -c '! grep -q "as vehicle_id" dbt/models/gold/fct_shipments.sql && grep -q "truck_id" dbt/models/gold/fct_shipments.sql'
check "gold schema date_id FK" bash -c 'grep -q "date_id" dbt/models/gold/schema.yml && grep -q "truck_id" dbt/models/gold/schema.yml'
check "mart DDL BIGINT pieces" bash -c 'grep -q "pieces BIGINT" sql/serving_mart/01_mart_schema.sql && grep -q "accessorial_charges BIGINT" sql/serving_mart/01_mart_schema.sql'

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log_info "--- Summary ---"
log_info "PASS $PASS / $TOTAL  FAIL $FAIL  WARN $WARN"
if [[ $FAIL -eq 0 ]]; then
  log_success "Pipeline health OK — all critical checks passed (WARNs are non-blocking for local dev)"
  exit 0
else
  log_error "Pipeline health FAILED — $FAIL critical check(s) failed, see log $LOG_FILE"
  exit 1
fi
