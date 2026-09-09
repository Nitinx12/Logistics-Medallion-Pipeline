Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Import-Module "$PSScriptRoot/lib/Common.psm1"

Assert-Command uv
Assert-Command docker

Write-Info "uv sync"
uv sync
Write-Success "Setup complete"
