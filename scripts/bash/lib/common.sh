#!/usr/bin/env bash
# FreightLake common helpers — shared logging and project root finder
# Mirrors bike-store-pipeline pattern.

log_info() { echo -e "\033[1;34m[INFO]\033[0m $*"; }
log_success() { echo -e "\033[1;32m[OK]\033[0m $*"; }
log_warn() { echo -e "\033[1;33m[WARN]\033[0m $*"; }
log_error() { echo -e "\033[1;31m[ERR]\033[0m $*" >&2; }

require_cmd() { command -v "$1" >/dev/null 2>&1 || { log_error "Missing $1 — install it and ensure it is on PATH"; exit 1; }; }

find_project_root() {
  local dir="$1"
  while [[ "$dir" != "/" && "$dir" != "." ]]; do
    if [[ -f "$dir/pyproject.toml" && -f "$dir/.env.example" ]]; then
      echo "$dir"
      return 0
    fi
    dir="$(dirname "$dir")"
  done
  # fallback to script location
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  echo "$script_dir"
}
