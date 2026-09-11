#!/bin/bash
# run_gold.sh - execute the gold medallion dbt models and their tests
# Runs dbt select on gold, then tests gold. Exits non-zero if any test fails.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/run_gold_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$REPO_ROOT/dbt}"
DBT_SELECT="${DBT_SELECT:-gold}"

log "[run_gold] start profiles=$DBT_PROFILES_DIR select=$DBT_SELECT"

cd "$REPO_ROOT/dbt"
if command -v dbt >/dev/null 2>&1; then
  dbt run --profiles-dir "$DBT_PROFILES_DIR" --select "$DBT_SELECT" 2>&1 | tee -a "$LOG_FILE"
  dbt test --profiles-dir "$DBT_PROFILES_DIR" --select "$DBT_SELECT" 2>&1 | tee -a "$LOG_FILE"
else
  log "[run_gold] dbt not found, using uv run dbt"
  uv run dbt run --profiles-dir "$DBT_PROFILES_DIR" --select "$DBT_SELECT" 2>&1 | tee -a "$LOG_FILE"
  uv run dbt test --profiles-dir "$DBT_PROFILES_DIR" --select "$DBT_SELECT" 2>&1 | tee -a "$LOG_FILE"
fi

log "[run_gold] done"