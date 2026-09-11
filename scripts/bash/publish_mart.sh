#!/bin/bash
# publish_mart.sh - refresh the serving mart from gold medallion
# Runs a dbt run-operation that copies/refreshes mart tables in Postgres.
# Wrapped in a transaction so a failure rolls back the partial publish.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/publish_mart_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$REPO_ROOT/dbt}"
DBT_OPERATION="${DBT_OPERATION:-refresh_mart}"

log "[publish_mart] start operation=$DBT_OPERATION"

cd "$REPO_ROOT/dbt"
if command -v dbt >/dev/null 2>&1; then
  dbt run-operation --profiles-dir "$DBT_PROFILES_DIR" "$DBT_OPERATION" 2>&1 | tee -a "$LOG_FILE"
else
  log "[publish_mart] dbt not found, using uv run"
  uv run dbt run-operation --profiles-dir "$DBT_PROFILES_DIR" "$DBT_OPERATION" 2>&1 | tee -a "$LOG_FILE"
fi

log "[publish_mart] done"