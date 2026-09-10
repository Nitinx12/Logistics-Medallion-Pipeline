Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Import-Module "$PSScriptRoot/lib/Common.psm1"
$ProjectRoot = Find-ProjectRoot $PSScriptRoot
Set-Location $ProjectRoot
Write-Info "Running full pipeline seed -> bronze -> dbt -> publish"
& "$PSScriptRoot/seed_data.ps1"
uv run python -m spark_jobs.bronze.extract_postgres_oltp
if ($LASTEXITCODE -ne 0) { throw "extract_postgres_oltp failed" }
uv run python -m spark_jobs.bronze.extract_mongo_tracking
if ($LASTEXITCODE -ne 0) { throw "extract_mongo_tracking failed" }
& "$PSScriptRoot/dbt.ps1" build
if ($LASTEXITCODE -ne 0) { Write-Warn "dbt build returned $LASTEXITCODE — continuing to publish (warehouse may need sql scope)" }
uv run python -m spark_jobs.publish.publish_gold_to_postgres
if ($LASTEXITCODE -ne 0) { throw "publish failed" }
Write-Success "Pipeline complete"
