# local_runner.ps1 - PowerShell counterpart of scripts/bash/local_runner.sh
# Full local pipeline through main.py, the single entry point. All flags
# pass through: --skip-docker, --dry-run, --stage <name>.

Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$LogFile = Join-Path $RepoRoot "logs\local_runner_$(Get-Date -Format 'yyyy-MM-dd').log"
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $LogFile) | Out-Null

function Log([string]$Message) {
    $Line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    Write-Output $Line
    Add-Content -Path $LogFile -Value $Line
}

Log "[local_runner] start args=$($args -join ' ')"
Set-Location $RepoRoot

& uv @('run', 'python', 'main.py') @args 2>&1 | Tee-Object -FilePath $LogFile -Append
$ExitCode = $LASTEXITCODE

Log "[local_runner] done exit=$ExitCode"
exit $ExitCode
