#!/bin/bash
# run_gx.sh - run the Great Expectations validations for the medallion
# Uses gx/run_validations.py, the real entrypoint (the gx project does not
# use GX checkpoints). Exits non-zero on any failed expectation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/run_gx_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

GX_DIR="${GX_DIR:-$REPO_ROOT/gx}"
# Extra args pass through, e.g. GX_ARGS="--demo" scripts/bash/run_gx.sh
GX_ARGS="${GX_ARGS:-}"

log "[run_gx] start dir=$GX_DIR args=$GX_ARGS"

cd "$REPO_ROOT"
uv run python gx/run_validations.py --postgres --all $GX_ARGS 2>&1 | tee -a "$LOG_FILE"

log "[run_gx] done"
