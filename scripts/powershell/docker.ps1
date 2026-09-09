Param([string]$Action = "up")
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Import-Module "$PSScriptRoot/lib/Common.psm1"
$ProjectRoot = Find-ProjectRoot $PSScriptRoot
Set-Location $ProjectRoot

# Port handling — default 5434 to avoid collision with host 5432
if (-not $env:POSTGRES_DOCKER_PORT) { $env:POSTGRES_DOCKER_PORT = "5434" }
try {
  $tcp = Test-NetConnection -ComputerName localhost -Port 5432 -WarningAction SilentlyContinue
  if ($tcp.TcpTestSucceeded) { Write-Warn "Host 5432 in use — using docker host port $env:POSTGRES_DOCKER_PORT" }
} catch {}

switch ($Action) {
  "up" {
    Write-Info "Starting FreightLake containers (postgres:$env:POSTGRES_DOCKER_PORT->5432, mongo:27017, airflow:8090)"
    docker compose --env-file .env -f docker/compose.yml up -d --build
    Write-Success "Containers starting — run 'docker ps'"
  }
  "down" {
    Write-Info "Stopping FreightLake containers"
    docker compose --env-file .env -f docker/compose.yml down -v
    Write-Success "Containers stopped"
  }
  default { Write-Err "usage: docker.ps1 [up|down]"; exit 1 }
}
