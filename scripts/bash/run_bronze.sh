#!/bin/bash
# run_bronze.sh - execute the bronze medallion Spark job
# Reads watermark table, filters new/updated records, upserts to bronze delta.
# Retries once on transient Spark errors.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_FILE="$REPO_ROOT/logs/run_bronze_$(date +%Y-%m-%d).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

SPARK_MASTER="${SPARK_MASTER:-local[*]}"
WATERMARK_TABLE="${WATERMARK_TABLE:-freightlake_oltp.bronze_watermark}"
BRONZE_JOB="${BRONZE_JOB:-$REPO_ROOT/spark_jobs/bronze_job.py}"
MAX_RETRIES="${MAX_RETRIES:-1}"

log "[run_bronze] start master=$SPARK_MASTER watermark=$WATERMARK_TABLE"

attempt=0
while [ $attempt -le $MAX_RETRIES ]; do
  attempt=$((attempt + 1))
  log "[run_bronze] attempt $attempt"
  if spark-submit \
    --master "$SPARK_MASTER" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:4.2.0 \
    "$BRONZE_JOB" \
    --watermark-table "$WATERMARK_TABLE" 2>&1 | tee -a "$LOG_FILE"; then
    log "[run_bronze] success on attempt $attempt"
    exit 0
  fi
  log "[run_bronze] WARN attempt $attempt failed"
  if [ $attempt -le $MAX_RETRIES ]; then
    log "[run_bronze] retrying"
    sleep 5
  fi
done

log "[run_bronze] ERROR failed after $attempt attempts"
exit 1