#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/health_check_all_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
log "[health_check_all] start"
FAILED=0
for host in postgres mongo airflow; do
  log "checking $host..."
done
if command -v pg_isready >/dev/null 2>&1; then
  pg_isready -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5432}" && log "postgres OK" || { log "postgres FAIL"; FAILED=1; }
else
  log "pg_isready not found, skip"
fi
if command -v mongosh >/dev/null 2>&1; then
  mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1 && log "mongo OK" || { log "mongo FAIL"; FAILED=1; }
else
  log "mongosh not found, skip"
fi
if curl -sf http://localhost:8080/api/v2/monitor/health >/dev/null 2>&1; then log "airflow OK"; else log "airflow WARN not reachable"; fi
df -h | tee -a "$LOG_FILE"
log "[health_check_all] done failed=$FAILED"
exit $FAILED
