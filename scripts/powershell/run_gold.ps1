# run_gold.ps1 - PowerShell counterpart of scripts/bash/run_gold.sh
# Builds and tests the gold dbt models; same behavior and log output.

Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$LogFile = Join-Path $RepoRoot "logs\run_gold_$(Get-Date -Format 'yyyy-MM-dd').log"
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $LogFile) | Out-Null

function Log([string]$Message) {
    $Line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    Write-Output $Line
    Add-Content -Path $LogFile -Value $Line
}

function Invoke-Logged([string]$FileName, [string[]]$Arguments) {
    # Stream the command output to console and log, fail on a non-zero exit.
    & $FileName @Arguments 2>&1 | Tee-Object -FilePath $LogFile -Append
    if ($LASTEXITCODE -ne 0) {
        Log "[run_gold] ERROR exit=$LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

$DbtProfilesDir = if ($env:DBT_PROFILES_DIR) { $env:DBT_PROFILES_DIR } else { Join-Path $RepoRoot 'dbt' }
$DbtSelect = if ($env:DBT_SELECT) { $env:DBT_SELECT } else { 'tag:gold' }

Log "[run_gold] start profiles=$DbtProfilesDir select=$DbtSelect"
Set-Location (Join-Path $RepoRoot 'dbt')

Invoke-Logged 'uv' @('run', 'dbt', 'build', '--profiles-dir', $DbtProfilesDir, '--select', $DbtSelect)

Log "[run_gold] done"
