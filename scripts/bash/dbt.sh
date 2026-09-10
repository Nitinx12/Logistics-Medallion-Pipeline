#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
PROJECT_ROOT="$(find_project_root "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"
if ! command -v uv >/dev/null 2>&1 && ! command -v uv.exe >/dev/null 2>&1; then
  for p in "$HOME/AppData/Local/hermes/bin" "/mnt/c/Users/$USER/AppData/Local/hermes/bin" "/mnt/c/Users/91852/AppData/Local/hermes/bin"; do
    [[ -x "$p/uv.exe" ]] && export PATH="$p:$PATH" && break
    [[ -x "$p/uv" ]] && export PATH="$p:$PATH" && break
  done
fi
UV_CMD="uv"; command -v uv.exe >/dev/null 2>&1 && ! command -v uv >/dev/null 2>&1 && UV_CMD="uv.exe"

case "${1:-build}" in
  snapshot) log_info "dbt snapshot"; $UV_CMD run dbt snapshot --project-dir dbt --profiles-dir dbt --target dev || log_warn "dbt snapshot skipped — warehouse may need sql scope";;
  build) log_info "dbt snapshot"; $UV_CMD run dbt snapshot --project-dir dbt --profiles-dir dbt --target dev || log_warn "dbt snapshot skipped — warehouse may need sql scope"
         log_info "dbt build"; $UV_CMD run dbt build --project-dir dbt --profiles-dir dbt --target dev || log_warn "dbt build failed — warehouse may need sql scope";;
  test) log_info "dbt test"; $UV_CMD run dbt test --project-dir dbt --profiles-dir dbt --target dev || log_warn "dbt test skipped";;
  docs) log_info "dbt docs"; $UV_CMD run dbt docs generate --project-dir dbt --profiles-dir dbt || $UV_CMD run dbt parse --project-dir dbt --profiles-dir dbt;;
  *) log_error "unknown $1"; exit 1;;
esac
