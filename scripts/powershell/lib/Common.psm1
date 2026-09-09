function Write-Info($m){ Write-Host "[INFO] $m" -ForegroundColor Blue }
function Write-Success($m){ Write-Host "[OK] $m" -ForegroundColor Green }
function Write-Err($m){ Write-Host "[ERR] $m" -ForegroundColor Red }
function Assert-Command($n){ if(-not (Get-Command $n -ErrorAction SilentlyContinue)){ throw "Missing $n" } }
Export-ModuleMember -Function Write-Info,Write-Success,Write-Err,Assert-Command
