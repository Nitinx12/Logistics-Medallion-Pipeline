Import-Module "$PSScriptRoot/lib/Common.psm1"
$ProjectRoot = Find-ProjectRoot $PSScriptRoot
Set-Location $ProjectRoot
Write-Info "Seeding Postgres OLTP and Mongo from data/"
uv run python scripts/seed.py
Write-Success "Seed complete — Postgres freight_lake + Mongo freight_lake"
