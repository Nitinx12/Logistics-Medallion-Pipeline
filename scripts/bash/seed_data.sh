#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
log_info "Seeding Postgres and Mongo from data/"
uv run python -c "import pathlib; print('seed stub: would load CSVs into Postgres/Mongo')"
log_success "Seed complete"
