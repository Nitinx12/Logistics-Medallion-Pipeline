# publish_mart.ps1 - PowerShell counterpart of scripts/bash/publish_mart.sh
# Publishes the gold star schema to the Postgres mart; same behavior and
# log output.

Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$LogFile = Join-Path $RepoRoot "logs\publish_mart_$(Get-Date -Format 'yyyy-MM-dd').log"
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
        Log "[publish_mart] ERROR exit=$LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

Log "[publish_mart] start"
Set-Location $RepoRoot

Invoke-Logged 'uv' @('run', 'python', '-m', 'src.jobs.publish_gold_to_postgres')

Log "[publish_mart] done"
