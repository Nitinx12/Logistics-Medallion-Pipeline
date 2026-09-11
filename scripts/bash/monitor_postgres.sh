#!/bin/bash
set -e
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/monitor_postgres_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
log "[monitor_postgres] start host=${POSTGRES_HOST:-localhost}:${POSTGRES_PORT:-5432} db=${POSTGRES_DATABASE:-freight_lake}"
if ! command -v psql >/dev/null 2>&1; then log "psql not found"; exit 1; fi
export PGPASSWORD="${POSTGRES_PASSWORD:-changeme}"
psql -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USERNAME:-postgres}" -d "${POSTGRES_DATABASE:-freight_lake}" -c "SELECT 1" && log "postgres connection OK" || { log "postgres connection FAIL"; exit 1; }
psql -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USERNAME:-postgres}" -d "${POSTGRES_DATABASE:-freight_lake}" -c "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 5" | tee -a "$LOG_FILE"
log "[monitor_postgres] done"
