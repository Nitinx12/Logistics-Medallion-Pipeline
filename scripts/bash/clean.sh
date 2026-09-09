#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
PROJECT_ROOT="$(find_project_root "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"
if command -v docker >/dev/null 2>&1; then
  docker compose --env-file .env -f docker/compose.yml down -v || log_warn "docker down failed"
else
  log_warn "docker not found, skipping compose down"
fi
rm -rf logs/ spark-warehouse/ metastore_db/ derby.log delta/ watermarks.json 2>/dev/null || true
log_success "Clean done — removed logs, spark-warehouse, delta, watermarks"
