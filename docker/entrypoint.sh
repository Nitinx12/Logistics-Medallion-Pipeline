#!/usr/bin/env bash
set -euo pipefail
# FreightLake Docker entrypoint — thin wrapper, logic lives in scripts/

echo "[freightlake] entrypoint $*"
exec "$@"
