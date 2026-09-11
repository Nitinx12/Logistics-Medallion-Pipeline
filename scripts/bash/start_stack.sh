#!/bin/bash
# start_stack.sh - bring up the full FreightLake stack and wait for health
# Waits for postgres, mongo, and airflow api-server to become ready.
# On timeout, rolls back the stack to avoid a partial, dangling state.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/start_stack_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

MAX_WAIT="${MAX_WAIT:-180}"
POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8080}"

log "[start_stack] start max_wait=${MAX_WAIT}s"

cd "$REPO_ROOT/docker"

log "[start_stack] docker compose up -d"
docker compose up -d

elapsed=0
pg_ready=0
mongo_ready=0
af_ready=0

log "[start_stack] waiting for postgres"
while [ $elapsed -lt $MAX_WAIT ]; do
  if pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" >/dev/null 2>&1; then
    pg_ready=1
    log "[start_stack] postgres ready after ${elapsed}s"
    break
  fi
  sleep 3
  elapsed=$((elapsed + 3))
done
if [ "$pg_ready" -ne 1 ]; then
  log "[start_stack] ERROR postgres not ready after ${MAX_WAIT}s"
  docker compose down
  exit 1
fi

elapsed=0
log "[start_stack] waiting for mongo"
while [ $elapsed -lt $MAX_WAIT ]; do
  if mongosh "$MONGO_URI" --eval "db.adminCommand('ping')" >/dev/null 2>&1; then
    mongo_ready=1
    log "[start_stack] mongo ready after ${elapsed}s"
    break
  fi
  sleep 3
  elapsed=$((elapsed + 3))
done
if [ "$mongo_ready" -ne 1 ]; then
  log "[start_stack] ERROR mongo not ready after ${MAX_WAIT}s"
  docker compose down
  exit 1
fi

elapsed=0
log "[start_stack] waiting for airflow api-server"
while [ $elapsed -lt $MAX_WAIT ]; do
  if curl -sf "$AIRFLOW_URL/api/v2/monitor/health" >/dev/null 2>&1; then
    af_ready=1
    log "[start_stack] airflow api ready after ${elapsed}s"
    break
  fi
  sleep 5
  elapsed=$((elapsed + 5))
done
if [ "$af_ready" -ne 1 ]; then
  log "[start_stack] ERROR airflow api not ready after ${MAX_WAIT}s"
  docker compose down
  exit 1
fi

log "[start_stack] done postgres=$pg_ready mongo=$mongo_ready airflow=$af_ready"