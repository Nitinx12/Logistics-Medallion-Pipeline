#!/bin/bash
# bootstrap.sh - one-time local environment setup for FreightLake
# Installs the uv virtualenv, builds the Docker image, and pulls base images.
# Requires: uv, docker, docker compose plugin, .env present.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/bootstrap_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "[bootstrap] start repo=$REPO_ROOT"

if [ ! -f "$REPO_ROOT/.env" ]; then
  log "[bootstrap] ERROR .env missing, copy .env.example to .env first"
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  log "[bootstrap] ERROR uv not found, install uv first"
  exit 1
fi

log "[bootstrap] syncing uv environment"
cd "$REPO_ROOT"
uv sync --all-extras

log "[bootstrap] building docker image"
cd "$REPO_ROOT/docker"
docker compose build

log "[bootstrap] pulling base images"
docker compose pull

log "[bootstrap] done"