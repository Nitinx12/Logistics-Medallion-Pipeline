Import-Module "$PSScriptRoot/lib/Common.psm1"
$ProjectRoot = Find-ProjectRoot $PSScriptRoot
Set-Location $ProjectRoot
try { docker compose --env-file .env -f docker/compose.yml down -v } catch { Write-Warn "docker down failed" }
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue logs, spark-warehouse, metastore_db, derby.log, delta, watermarks.json
Write-Success "Clean done — removed logs, spark-warehouse, delta, watermarks"
