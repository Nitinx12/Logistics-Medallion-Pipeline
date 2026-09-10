Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Import-Module "$PSScriptRoot/lib/Common.psm1"
$ProjectRoot = Find-ProjectRoot $PSScriptRoot
Set-Location $ProjectRoot
Write-Info "Seeding Postgres OLTP and Mongo from data/"
uv run python scripts/seed.py
if ($LASTEXITCODE -ne 0) { throw "seed.py failed with exit $LASTEXITCODE" }
Write-Success "Seed complete — Postgres freight_lake + Mongo freight_lake"
