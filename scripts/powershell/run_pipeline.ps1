Import-Module "$PSScriptRoot/lib/Common.psm1"
$ProjectRoot = Find-ProjectRoot $PSScriptRoot
Set-Location $ProjectRoot
Write-Info "Running full pipeline seed -> bronze -> dbt -> publish"
& "$PSScriptRoot/seed_data.ps1"
uv run python -m spark_jobs.bronze.extract_postgres_oltp
uv run python -m spark_jobs.bronze.extract_mongo_tracking
& "$PSScriptRoot/dbt.ps1" build
uv run python -m spark_jobs.publish.publish_gold_to_postgres
Write-Success "Pipeline complete"
