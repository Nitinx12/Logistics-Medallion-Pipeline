#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
log_info "Running full pipeline seed -> bronze -> dbt -> publish"
bash scripts/bash/seed_data.sh
uv run python spark_jobs/bronze/extract_postgres_oltp.py
uv run python spark_jobs/bronze/extract_mongo_tracking.py
bash scripts/bash/dbt.sh build || true
uv run python spark_jobs/publish/publish_gold_to_postgres.py
log_success "Pipeline complete"
