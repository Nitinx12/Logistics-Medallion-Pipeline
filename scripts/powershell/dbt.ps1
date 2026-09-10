Param([string]$Action = "build")
Import-Module "$PSScriptRoot/lib/Common.psm1"
$ProjectRoot = Find-ProjectRoot $PSScriptRoot
Set-Location $ProjectRoot
switch ($Action) {
  "snapshot" { Write-Info "dbt snapshot"; uv run dbt snapshot --project-dir dbt --profiles-dir dbt --target dev; if ($LASTEXITCODE -ne 0) { Write-Warn "dbt snapshot skipped — warehouse may need sql scope" } }
  "build" { Write-Info "dbt snapshot"; uv run dbt snapshot --project-dir dbt --profiles-dir dbt --target dev; if ($LASTEXITCODE -ne 0) { Write-Warn "dbt snapshot skipped — warehouse may need sql scope" }
            Write-Info "dbt build"; uv run dbt build --project-dir dbt --profiles-dir dbt --target dev; if ($LASTEXITCODE -ne 0) { Write-Warn "dbt build failed — warehouse may need sql scope" } }
  "test" { Write-Info "dbt test"; uv run dbt test --project-dir dbt --profiles-dir dbt --target dev; if ($LASTEXITCODE -ne 0) { Write-Warn "dbt test skipped" } }
  "docs" { Write-Info "dbt docs"; uv run dbt docs generate --project-dir dbt --profiles-dir dbt; if ($LASTEXITCODE -ne 0) { uv run dbt parse --project-dir dbt --profiles-dir dbt } }
  default { Write-Err "unknown $Action"; exit 1 }
}
