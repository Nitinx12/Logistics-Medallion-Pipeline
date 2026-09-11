#!/bin/bash
set -e
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/monitor_airflow_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
log "[monitor_airflow] start"
for svc in airflow-apiserver airflow-scheduler dag-processor; do
  if docker ps --format "{{.Names}}" 2>/dev/null | grep -q "$svc"; then log "$svc running"; else log "$svc not running WARN"; fi
done
if curl -sf http://localhost:8080/api/v2/monitor/health >/dev/null 2>&1; then log "airflow api OK"; else log "airflow api not reachable"; fi
if command -v airflow >/dev/null 2>&1; then airflow dags list 2>&1 | head -20 | tee -a "$LOG_FILE"; else log "airflow cli not found"; fi
log "[monitor_airflow] done"
