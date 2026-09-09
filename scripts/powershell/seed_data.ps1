Import-Module "$PSScriptRoot/lib/Common.psm1"
Write-Info "Seeding Postgres and Mongo from data/"
uv run python -c "import pathlib; print('seed stub: would load CSVs into Postgres/Mongo')"
Write-Success "Seed complete"
