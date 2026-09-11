#!/bin/bash
set -e
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/monitor_mongo_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
log "[monitor_mongo] start uri=${MONGO_URI:-mongodb://localhost:27017} db=${MONGO_DB:-freight_lake}"
if ! command -v mongosh >/dev/null 2>&1; then log "mongosh not found"; exit 1; fi
mongosh "${MONGO_URI:-mongodb://localhost:27017}/${MONGO_DB:-freight_lake}" --eval "db.runCommand({ping:1})" | tee -a "$LOG_FILE" && log "mongo ping OK" || { log "mongo ping FAIL"; exit 1; }
mongosh "${MONGO_URI:-mongodb://localhost:27017}/${MONGO_DB:-freight_lake}" --eval "db.getCollectionNames().forEach(c=>print(c + ': ' + db.getCollection(c).countDocuments()))" | tee -a "$LOG_FILE"
log "[monitor_mongo] done"
