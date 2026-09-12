#!/bin/bash
# local_runner.sh - full local pipeline through main.py, the single entry point
# All flags pass through: --skip-docker, --dry-run, --stage <name>.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/local_runner_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "[local_runner] start args=$*"

cd "$REPO_ROOT"
set +e
uv run python main.py "$@" 2>&1 | tee -a "$LOG_FILE"
status=${PIPESTATUS[0]}
set -e

log "[local_runner] done exit=$status"
exit $status
