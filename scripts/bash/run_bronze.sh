#!/bin/bash
# run_bronze.sh - run both bronze extraction jobs (Postgres OLTP, then MongoDB)
# Each job reads the watermark from the target Delta table itself and lands
# only new or updated rows, so reruns are idempotent.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/run_bronze_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

TARGET_SCHEMA="${TARGET_SCHEMA:-bronze}"

log "[run_bronze] start target_schema=$TARGET_SCHEMA"

cd "$REPO_ROOT"

log "[run_bronze] postgres extraction"
uv run python -m src.jobs.pg_extract_incremental --target-schema "$TARGET_SCHEMA" 2>&1 | tee -a "$LOG_FILE"

log "[run_bronze] mongodb extraction"
uv run python -m src.jobs.mongo_extract_incremental --target-schema "$TARGET_SCHEMA" 2>&1 | tee -a "$LOG_FILE"

log "[run_bronze] done"
