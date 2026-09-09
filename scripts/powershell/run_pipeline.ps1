Import-Module "$PSScriptRoot/lib/Common.psm1"
Write-Info "Running full pipeline seed -> bronze -> dbt -> publish"
& "$PSScriptRoot/seed_data.ps1"
uv run python spark_jobs/bronze/extract_postgres_oltp.py
uv run python spark_jobs/bronze/extract_mongo_tracking.py
bash scripts/bash/dbt.sh build
uv run python spark_jobs/publish/publish_gold_to_postgres.py
Write-Success "Pipeline complete"
