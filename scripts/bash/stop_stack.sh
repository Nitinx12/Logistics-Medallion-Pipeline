#!/bin/bash
# stop_stack.sh - graceful shutdown of the FreightLake stack
# Preserves named volumes so data survives a restart. Use reset_data.sh to wipe.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/stop_stack_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "[stop_stack] start"

cd "$REPO_ROOT/docker"
docker compose down

log "[stop_stack] done"