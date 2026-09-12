#!/bin/bash
# publish_mart.sh - publish the gold star schema to the Postgres mart
# Runs the Spark JDBC publish job. Gold tables are materialized as tables,
# so the publish is a full overwrite per table into the mart gold schema.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/publish_mart_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "[publish_mart] start"

cd "$REPO_ROOT"
uv run python -m src.jobs.publish_gold_to_postgres 2>&1 | tee -a "$LOG_FILE"

log "[publish_mart] done"
