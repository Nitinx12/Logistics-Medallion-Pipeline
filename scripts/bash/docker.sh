#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"
PROJECT_ROOT="$(find_project_root "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Detect docker compose plugin vs standalone
COMPOSE="docker compose"
if ! docker compose version >/dev/null 2>&1; then
  if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
  else
    log_error "docker compose not found"
    exit 1
  fi
fi

# Port collision handling — host 5432 vs docker 5433
# If host postgres is listening on 5432, use 5433 for docker to avoid collision
if command -v ss >/dev/null 2>&1; then
  if ss -tln 2>/dev/null | grep -q ":5432 "; then
    export POSTGRES_DOCKER_PORT="${POSTGRES_DOCKER_PORT-5434}"
    log_warn "Host 5432 in use (local Postgres) — using docker host port $POSTGRES_DOCKER_PORT to avoid collision"
  fi
elif command -v netstat >/dev/null 2>&1; then
  if netstat -tln 2>/dev/null | grep -q ":5432 "; then
    export POSTGRES_DOCKER_PORT="${POSTGRES_DOCKER_PORT-5434}"
    log_warn "Host 5432 in use — using docker host port $POSTGRES_DOCKER_PORT"
  fi
fi

# Prefer 5433 by default to avoid collision with local dev (AGENTS.md:158)
export POSTGRES_DOCKER_PORT="${POSTGRES_DOCKER_PORT-5434}"

case "${1:-up}" in
  up)
    log_info "Starting FreightLake containers (postgres:$POSTGRES_DOCKER_PORT->5432, mongo:27017, airflow:8090)"
    $COMPOSE --env-file .env -f docker/compose.yml up -d --build
    log_success "Containers starting — run 'docker ps' and 'docker compose -f docker/compose.yml --env-file .env ps'"
    ;;
  down)
    log_info "Stopping FreightLake containers"
    $COMPOSE --env-file .env -f docker/compose.yml down -v
    log_success "Containers stopped"
    ;;
  *) log_error "usage: $0 [up|down]"; exit 1;;
esac
