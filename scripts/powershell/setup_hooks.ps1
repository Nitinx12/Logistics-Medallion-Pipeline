# setup_hooks.ps1 - install git hooks for FreightLake (PowerShell counterpart)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..\..")

Set-Location $RepoRoot

& git config core.hooksPath .githooks

# Ensure the hook is executable on systems that respect it
$hookPath = Join-Path $RepoRoot ".githooks\pre-commit"
if (Test-Path $hookPath) {
    # no chmod needed on Windows, but keep file as is for WSL
    Write-Host "[setup_hooks] git hooks installed from .githooks (core.hooksPath=.githooks)"
    Write-Host "[setup_hooks] pre-commit will run ruff check and secret scan on every commit"
} else {
    Write-Error "Hook file not found at $hookPath"
    exit 1
}
