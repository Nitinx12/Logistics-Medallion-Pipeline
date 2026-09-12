#!/bin/bash
# run_gx.sh - run the Great Expectations checkpoint for the medallion
# Validates silver and gold data quality. Exits non-zero on any failed expectation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/run_gx_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

GX_CHECKPOINT="${GX_CHECKPOINT:-medallion_silver_gold}"
GX_DIR="${GX_DIR:-$REPO_ROOT/gx}"

log "[run_gx] start checkpoint=$GX_CHECKPOINT dir=$GX_DIR"

if command -v great_expectations >/dev/null 2>&1; then
  (cd "$GX_DIR" && great_expectations checkpoint run "$GX_CHECKPOINT") 2>&1 | tee -a "$LOG_FILE"
else
  log "[run_gx] great_expectations not found, using uv run"
  (cd "$GX_DIR" && uv run great_expectations checkpoint run "$GX_CHECKPOINT") 2>&1 | tee -a "$LOG_FILE"
fi

log "[run_gx] done"