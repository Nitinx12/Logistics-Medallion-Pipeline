#!/bin/bash
# run_all_tests.sh - full CI quality gate for FreightLake
# Runs ruff lint, mypy type check, pytest, and dbt tests.
# Exits non-zero on the first failing step.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/run_all_tests_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$REPO_ROOT/dbt}"

log "[run_all_tests] start"

log "[run_all_tests] ruff check"
cd "$REPO_ROOT"
ruff check . 2>&1 | tee -a "$LOG_FILE"

log "[run_all_tests] mypy"
mypy . 2>&1 | tee -a "$LOG_FILE"

log "[run_all_tests] pytest"
pytest 2>&1 | tee -a "$LOG_FILE"

log "[run_all_tests] dbt test"
cd "$REPO_ROOT/dbt"
if command -v dbt >/dev/null 2>&1; then
  dbt test --profiles-dir "$DBT_PROFILES_DIR" 2>&1 | tee -a "$LOG_FILE"
else
  uv run dbt test --profiles-dir "$DBT_PROFILES_DIR" 2>&1 | tee -a "$LOG_FILE"
fi

log "[run_all_tests] done"