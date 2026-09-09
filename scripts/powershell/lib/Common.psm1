function Write-Info($m){ Write-Host "[INFO] $m" -ForegroundColor Blue }
function Write-Success($m){ Write-Host "[OK] $m" -ForegroundColor Green }
function Write-Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Write-Err($m){ Write-Host "[ERR] $m" -ForegroundColor Red }
function Assert-Command($n){ if(-not (Get-Command $n -ErrorAction SilentlyContinue)){ throw "Missing $n — install it and ensure it is on PATH" } }
function Find-ProjectRoot {
  param([string]$StartDir = $PSScriptRoot)
  $dir = $StartDir
  while ($dir -and $dir -ne "" -and (Split-Path $dir) -ne $dir) {
    if ((Test-Path (Join-Path $dir "pyproject.toml")) -and (Test-Path (Join-Path $dir ".env.example"))) { return $dir }
    $dir = Split-Path $dir
  }
  return (Resolve-Path (Join-Path $PSScriptRoot "../..")).Path
}
Export-ModuleMember -Function Write-Info,Write-Success,Write-Warn,Write-Err,Assert-Command,Find-ProjectRoot
