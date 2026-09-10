# FreightLake pipeline health check — PowerShell equivalent of pipeline_health.sh
$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Import-Module (Join-Path $ScriptDir "lib/Common.psm1") -Force
$ProjectRoot = Find-ProjectRoot $ScriptDir
Set-Location $ProjectRoot
$LogDir = Join-Path $ProjectRoot "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$LogFile = Join-Path $LogDir "health.log"

function Mask($s){ return ($s -replace '://[^@]*@','://***:***@' -replace 'token=[^&]*','token=***' -replace 'dapi[^ ]*','***') }

$uvCmd = "uv"
if (Get-Command "uv.exe" -ErrorAction SilentlyContinue -and -not (Get-Command "uv" -ErrorAction SilentlyContinue)) { $uvCmd = "uv.exe" }
# Also try hermes path
if (-not (Get-Command $uvCmd -ErrorAction SilentlyContinue)) {
  foreach($p in @("$env:USERPROFILE\AppData\Local\hermes\bin","C:\Users\91852\AppData\Local\hermes\bin")) {
    if (Test-Path (Join-Path $p "uv.exe")) { $env:PATH = "$p;$env:PATH"; $uvCmd = "uv.exe"; break }
    if (Test-Path (Join-Path $p "uv")) { $env:PATH = "$p;$env:PATH"; $uvCmd = "uv"; break }
  }
}

$global:Pass=0; $global:Fail=0; $global:Warn=0; $global:Total=0

function Check($name, [scriptblock]$sb){
  $global:Total++
  try { & $sb; Write-Success "$name PASS"; $global:Pass++ } catch { Write-Err "$name FAIL $_"; $global:Fail++ }
}
function CheckWarn($name, [scriptblock]$sb){
  $global:Total++
  try { & $sb; Write-Success "$name PASS"; $global:Pass++ } catch { Write-Warn "$name WARN $_"; $global:Warn++ }
}

Write-Info "FreightLake pipeline health check — $(Get-Date -Format o)"
Write-Info "Project root $ProjectRoot"

# 1. Env
Write-Info "--- 1. Environment ---"
Check "env file exists" { if (-not (Test-Path (Join-Path $ProjectRoot ".env"))) { throw "missing .env" } }
if (Test-Path (Join-Path $ProjectRoot ".env")) {
  Get-Content (Join-Path $ProjectRoot ".env") | ForEach-Object {
    if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
    $kv = $_ -split '=',2
    if ($kv.Count -eq 2) { Set-Item -Path "env:$($kv[0].Trim())" -Value $kv[1].Trim() }
  }
}
foreach($var in @("POSTGRES_OLTP_URL","POSTGRES_MART_URL","MONGO_URI","DATABRICKS_HOST","DATABRICKS_TOKEN","DATABRICKS_HTTP_PATH")){
  $val = [Environment]::GetEnvironmentVariable($var)
  $global:Total++
  if ($val) { Write-Success "env $var=$(Mask $val)"; $global:Pass++ }
  elseif (Select-String -Path (Join-Path $ProjectRoot ".env.example") -Pattern "^$var=" -Quiet) { Write-Warn "env $var missing — placeholder"; $global:Warn++ }
  else { Write-Err "env $var missing"; $global:Fail++ }
}
foreach($var in @("DATABRICKS_CATALOG","DATABRICKS_SCHEMA_BRONZE","DATABRICKS_SCHEMA_SILVER","DATABRICKS_SCHEMA_GOLD")){
  Check "env $var set" { if (-not [Environment]::GetEnvironmentVariable($var)) { throw "missing" } }
}

# 2-4. DB connectivity (warn not fail if docker down)
Write-Info "--- 2. Postgres OLTP ---"
CheckWarn "postgres oltp connectivity" {
  & $uvCmd run python --% -c "import os,sys; from dotenv import load_dotenv; load_dotenv(); url=os.getenv('POSTGRES_OLTP_URL',''); 
import psycopg2; c=psycopg2.connect(url, connect_timeout=5); cur=c.cursor(); cur.execute('SELECT 1'); cur.close(); c.close()"
}
Write-Info "--- 3. Postgres mart ---"
CheckWarn "postgres mart connectivity" {
  & $uvCmd run python --% -c "import os,sys; from dotenv import load_dotenv; load_dotenv(); url=os.getenv('POSTGRES_MART_URL',''); 
import psycopg2; c=psycopg2.connect(url, connect_timeout=5); cur=c.cursor(); cur.execute('SELECT 1'); cur.close(); c.close()"
}
Write-Info "--- 4. MongoDB ---"
CheckWarn "mongo connectivity" {
  & $uvCmd run python --% -c "import os; from dotenv import load_dotenv; load_dotenv(); uri=os.getenv('MONGO_URI',''); 
from pymongo import MongoClient; c=MongoClient(uri, serverSelectionTimeoutMS=5000); c.admin.command('ping'); c.close()"
}

# 5. Databricks
Write-Info "--- 5. Databricks ---"
CheckWarn "databricks config present" { if (-not $env:DATABRICKS_HOST -or -not $env:DATABRICKS_TOKEN -or -not $env:DATABRICKS_HTTP_PATH) { throw "missing" } }

# 6. Docker
Write-Info "--- 6. Docker ---"
if (Get-Command docker -ErrorAction SilentlyContinue) {
  CheckWarn "docker compose config valid" { docker compose -f docker/compose.yml config | Out-Null }
  Check "compose dag-processor service" { if (-not (Select-String -Path docker/compose.yml -Pattern "dag-processor" -Quiet)) { throw "missing" } }
} else { Write-Warn "docker not found"; $global:Warn++; $global:Total++ }

# 7. dbt
Write-Info "--- 7. dbt ---"
Check "dbt compile local" { & $uvCmd run dbt compile --project-dir dbt --profiles-dir dbt --target local | Out-Null }
Check "dbt test local" { & $uvCmd run dbt test --project-dir dbt --profiles-dir dbt --target local | Out-Null }
CheckWarn "dbt docs generate" { & $uvCmd run dbt docs generate --project-dir dbt --profiles-dir dbt --target local | Out-Null }

# 8. GX
Write-Info "--- 8. Great Expectations ---"
Check "gx suite json valid" { Get-Content great_expectations/expectations/silver_suite.json | ConvertFrom-Json | Out-Null }
Check "gx suite has expectations" { $j=Get-Content great_expectations/expectations/silver_suite.json | ConvertFrom-Json; if ($j.expectations.Count -lt 20){ throw "too few" } }
Check "gx checkpoint exists" { if (-not (Test-Path great_expectations/checkpoints/silver_checkpoint.yml)) { throw "missing" } }

# 9. Airflow
Write-Info "--- 9. Airflow DAGs ---"
Check "bronze dag compile" { python -m py_compile airflow/dags/freightlake_bronze_dag.py }
Check "silver_gold dag compile" { python -m py_compile airflow/dags/freightlake_silver_gold_dag.py }
Check "publish dag compile" { python -m py_compile airflow/dags/freightlake_publish_dag.py }
Check "dag bronze uses python -m" { if (-not (Select-String -Path airflow/dags/freightlake_bronze_dag.py -Pattern "python -m spark_jobs.bronze" -Quiet)){ throw "missing" } }
Check "dag publish uses python -m" { if (-not (Select-String -Path airflow/dags/freightlake_publish_dag.py -Pattern "python -m spark_jobs.publish" -Quiet)){ throw "missing" } }

# 10. Lint
Write-Info "--- 10. Lint and unit tests ---"
Check "ruff check" { & $uvCmd run ruff check spark_jobs scripts tests main.py | Out-Null }
Check "ruff format check" { & $uvCmd run ruff format --check spark_jobs scripts tests main.py | Out-Null }
Check "mypy" { & $uvCmd run mypy spark_jobs --ignore-missing-imports | Out-Null }
Check "pytest" { & $uvCmd run pytest tests -q | Out-Null }
Check "sqlfluff gold" { & $uvCmd run sqlfluff lint dbt/models --dialect databricks | Out-Null }

# 11. Gold modelling
Write-Info "--- 11. Gold modelling ---"
Check "gold dim_driver lower" { if (-not (Select-String -Path dbt/models/gold/dim_driver.sql -Pattern "lower\(trim\(employment_status\)" -Quiet)){ throw "missing" } }
Check "gold dim_vehicle lower+model_year" { if (-not (Select-String -Path dbt/models/gold/dim_vehicle.sql -Pattern "LOWER\(TRIM\(make\)" -Quiet)){ throw "missing" }; if (-not (Select-String -Path dbt/models/gold/dim_vehicle.sql -Pattern "model_year" -Quiet)){ throw "missing" } }
Check "gold fct date_id" { if (-not (Select-String -Path dbt/models/gold/fct_orders.sql -Pattern "date_id" -Quiet)){ throw "missing" } }
Check "gold truck_id" { if ((Select-String -Path dbt/models/gold/fct_shipments.sql -Pattern "as vehicle_id" -Quiet)){ throw "should be truck_id" } }
Check "mart DDL BIGINT" { if (-not (Select-String -Path sql/serving_mart/01_mart_schema.sql -Pattern "pieces BIGINT" -Quiet)){ throw "missing" } }

Write-Info "--- Summary ---"
Write-Info "PASS $global:Pass / $global:Total  FAIL $global:Fail  WARN $global:Warn"
if ($global:Fail -eq 0) { Write-Success "Pipeline health OK"; exit 0 } else { Write-Err "Pipeline health FAILED"; exit 1 }
