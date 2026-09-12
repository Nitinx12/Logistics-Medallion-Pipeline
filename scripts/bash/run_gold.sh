#!/bin/bash
# run_gold.sh - build and test the gold dbt models
# dbt build runs the models and their tests in dependency order and fails
# fast on the first failing test.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/run_gold_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$REPO_ROOT/dbt}"
DBT_SELECT="${DBT_SELECT:-tag:gold}"

log "[run_gold] start profiles=$DBT_PROFILES_DIR select=$DBT_SELECT"

cd "$REPO_ROOT/dbt"
uv run dbt build --profiles-dir "$DBT_PROFILES_DIR" --select "$DBT_SELECT" 2>&1 | tee -a "$LOG_FILE"

log "[run_gold] done"
