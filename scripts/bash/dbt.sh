#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
case "${1:-build}" in
  build) log_info "dbt build"; uv run dbt build --profiles-dir dbt --target dev || log_error "dbt not configured yet";;
  test) log_info "dbt test"; uv run dbt test --profiles-dir dbt || true;;
  docs) log_info "dbt docs"; uv run dbt docs generate --profiles-dir dbt || true;;
  *) log_error "unknown $1"; exit 1;;
esac
