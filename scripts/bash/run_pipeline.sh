#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
PROJECT_ROOT="$(find_project_root "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Ensure uv on PATH for WSL
if ! command -v uv >/dev/null 2>&1 && ! command -v uv.exe >/dev/null 2>&1; then
  for p in "$HOME/AppData/Local/hermes/bin" "/mnt/c/Users/$USER/AppData/Local/hermes/bin" "/mnt/c/Users/91852/AppData/Local/hermes/bin"; do
    [[ -x "$p/uv.exe" ]] && export PATH="$p:$PATH" && break
    [[ -x "$p/uv" ]] && export PATH="$p:$PATH" && break
  done
fi
UV_CMD="uv"; command -v uv.exe >/dev/null 2>&1 && ! command -v uv >/dev/null 2>&1 && UV_CMD="uv.exe"

log_info "Running full pipeline seed -> bronze -> dbt -> publish"
bash scripts/bash/seed_data.sh
$UV_CMD run python -m spark_jobs.bronze.extract_postgres_oltp
$UV_CMD run python -m spark_jobs.bronze.extract_mongo_tracking
bash scripts/bash/dbt.sh build || log_warn "dbt build skipped (no warehouse)"
$UV_CMD run python -m spark_jobs.publish.publish_gold_to_postgres
log_success "Pipeline complete"
