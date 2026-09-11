#!/bin/bash
# run_silver.sh - execute the silver medallion dbt models
# Runs dbt select on the silver models and fails fast on errors.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/run_silver_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$REPO_ROOT/dbt}"
DBT_SELECT="${DBT_SELECT:-silver}"

log "[run_silver] start profiles=$DBT_PROFILES_DIR select=$DBT_SELECT"

cd "$REPO_ROOT/dbt"
if command -v dbt >/dev/null 2>&1; then
  dbt run --profiles-dir "$DBT_PROFILES_DIR" --select "$DBT_SELECT" 2>&1 | tee -a "$LOG_FILE"
else
  log "[run_silver] dbt not found, using uv run dbt"
  uv run dbt run --profiles-dir "$DBT_PROFILES_DIR" --select "$DBT_SELECT" 2>&1 | tee -a "$LOG_FILE"
fi

log "[run_silver] done"