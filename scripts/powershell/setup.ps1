Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Import-Module "$PSScriptRoot/lib/Common.psm1"

$ProjectRoot = Find-ProjectRoot $PSScriptRoot
Set-Location $ProjectRoot

Write-Info "Checking prerequisites"
# Mirror bash setup.sh uv fallback search (hermes / cargo / Py313)
$uvFound = $false
foreach ($p in @("$HOME/AppData/Local/hermes/bin", "$HOME/.cargo/bin", "/mnt/c/Users/$USER/AppData/Local/hermes/bin", "/mnt/c/Users/91852/AppData/Local/hermes/bin", "$HOME/AppData/Local/Programs/Python/Python313/Scripts", "/mnt/c/Users/91852/AppData/Local/Programs/Python/Python313/Scripts")) {
  if (Test-Path "$p/uv.exe") { $env:PATH = "$p;$env:PATH"; $uvFound = $true; break }
  if (Test-Path "$p/uv") { $env:PATH = "$p;$env:PATH"; $uvFound = $true; break }
}
Assert-Command uv
Write-Info "uv $(uv --version)"

$expected = (Get-Content ".python-version" -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
$actual = (uv run python --version 2>&1).ToString().Split()[1]
if ($expected -and $actual -notlike "$expected*") {
  Write-Warn "Python $actual does not match pinned $expected in .python-version — uv will handle it"
} else {
  Write-Info "Python $actual matches pinned $expected"
}

try { Assert-Command docker; Write-Info "docker $(docker --version 2>&1 | Select-Object -First 1)" } catch { Write-Warn "docker not found — docker-up will fail until Docker Desktop is installed" }

if (-not (Test-Path ".env")) {
  Write-Warn ".env not found — copying from .env.example (fill Databricks values)"
  Copy-Item ".env.example" ".env"
}

Write-Info "uv sync (one venv via uv)"
uv sync

Write-Success "Setup complete — run 'make lint' and 'make test' to verify"
