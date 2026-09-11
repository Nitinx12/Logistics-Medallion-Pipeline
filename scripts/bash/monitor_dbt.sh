#!/bin/bash
set -e
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/monitor_dbt_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
log "[monitor_dbt] start"
cd "$REPO_ROOT/dbt"
if command -v dbt >/dev/null 2>&1; then
  dbt debug --profiles-dir . 2>&1 | tee -a "$LOG_FILE" || log "dbt debug FAIL"
  dbt ls --profiles-dir . 2>&1 | head -30 | tee -a "$LOG_FILE" || true
else
  log "dbt not found, try uv run dbt"
  uv run dbt debug --profiles-dir . 2>&1 | tee -a "$LOG_FILE" || true
fi
if [ -f target/run_results.json ]; then log "run_results rows $(wc -l < target/run_results.json)"; fi
log "[monitor_dbt] done"
