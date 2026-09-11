#!/bin/bash
set -e

# docker/entrypoint.sh - FreightLake Airflow 3.x entrypoint
# Used by docker/Dockerfile, handles postgres/mongo wait, airflow init, and role based exec per AGENTS.md 8

ROLE="${1:-api-server}"

wait_for_postgres() {
  echo "[entrypoint] waiting for postgres at $POSTGRES_HOST:${POSTGRES_PORT:-5432}..."
  for i in {1..30}; do
    if pg_isready -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_SUPERUSER:-postgres}" >/dev/null 2>&1; then
      echo "[entrypoint] postgres ready"
      return 0
    fi
    sleep 2
  done
  echo "[entrypoint] postgres not ready after 60s"
  return 1
}

wait_for_postgres

case "$ROLE" in
  api-server)
    echo "[entrypoint] airflow db migrate"
    airflow db migrate || true
    echo "[entrypoint] creating airflow admin user if missing"
    airflow users create --username airflow --password airflow --firstname Airflow --lastname Admin --role Admin --email airflow@example.com || true
    exec airflow api-server
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  dag-processor)
    # Airflow 3.x requires dag-processor per AGENTS.md 8, do not remove
    exec airflow dag-processor
    ;;
  worker)
    exec airflow celery worker
    ;;
  bash|shell)
    exec /bin/bash
    ;;
  *)
    # Pass through any other airflow command
    exec airflow "$@"
    ;;
esac
