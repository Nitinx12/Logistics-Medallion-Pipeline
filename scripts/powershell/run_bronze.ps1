# run_bronze.ps1 - PowerShell counterpart of scripts/bash/run_bronze.sh
# Runs both bronze extraction jobs; same behavior and log output.

Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$LogFile = Join-Path $RepoRoot "logs\run_bronze_$(Get-Date -Format 'yyyy-MM-dd').log"
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
        Log "[run_bronze] ERROR exit=$LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

$TargetSchema = if ($env:TARGET_SCHEMA) { $env:TARGET_SCHEMA } else { 'bronze' }

Log "[run_bronze] start target_schema=$TargetSchema"
Set-Location $RepoRoot

Log "[run_bronze] postgres extraction"
Invoke-Logged 'uv' @('run', 'python', '-m', 'src.jobs.pg_extract_incremental', '--target-schema', $TargetSchema)

Log "[run_bronze] mongodb extraction"
Invoke-Logged 'uv' @('run', 'python', '-m', 'src.jobs.mongo_extract_incremental', '--target-schema', $TargetSchema)

Log "[run_bronze] done"
