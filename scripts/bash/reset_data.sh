#!/bin/bash
# reset_data.sh - destroy all data volumes and reseed the OLTP sources
# WARNING: this is destructive. Requires --yes to confirm.
# After reset, start_stack.sh must be run to reinitialize the stack.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/reset_data_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

CONFIRM="${CONFIRM:-}"
if [ "$CONFIRM" != "--yes" ]; then
  log "[reset_data] ERROR pass CONFIRM=--yes to destroy all volumes"
  exit 1
fi

log "[reset_data] WARNING destroying all volumes and reseeding"

cd "$REPO_ROOT/docker"
docker compose down -v

log "[reset_data] volumes destroyed, starting stack"
"$SCRIPT_DIR/start_stack.sh"

log "[reset_data] done"