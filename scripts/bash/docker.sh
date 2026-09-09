#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
case "${1:-up}" in
  up) docker compose -f docker/compose.yml up -d --build ;;
  down) docker compose -f docker/compose.yml down -v ;;
  *) log_error "usage: $0 [up|down]"; exit 1;;
esac
