#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/common.sh
source "$SCRIPT_DIR/lib/common.sh"

PROJECT_ROOT="$(find_project_root "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Ensure uv is on PATH — handle PowerShell, git bash, and WSL (different mount prefixes)
if ! command -v uv >/dev/null 2>&1 && ! command -v uv.exe >/dev/null 2>&1; then
  for p in \
    "$HOME/AppData/Local/hermes/bin" \
    "$HOME/.cargo/bin" \
    "/c/Users/$USER/AppData/Local/hermes/bin" \
    "/mnt/c/Users/$USER/AppData/Local/hermes/bin" \
    "/mnt/c/Users/91852/AppData/Local/hermes/bin" \
    "$HOME/AppData/Local/Programs/Python/Python313/Scripts" \
    "/mnt/c/Users/91852/AppData/Local/Programs/Python/Python313/Scripts"
  do
    if [[ -x "$p/uv.exe" ]] || [[ -x "$p/uv" ]]; then
      export PATH="$p:$PATH"
      break
    fi
  done
fi

# Normalize uv command for WSL vs Windows — try uv, then uv.exe
UV_CMD="uv"
if ! command -v uv >/dev/null 2>&1; then
  if command -v uv.exe >/dev/null 2>&1; then
    UV_CMD="uv.exe"
  fi
fi

log_info "Checking prerequisites"
if ! command -v "$UV_CMD" >/dev/null 2>&1; then
  # last resort: try known absolute paths
  for p in "/mnt/c/Users/91852/AppData/Local/hermes/bin/uv.exe" "/c/Users/91852/AppData/Local/hermes/bin/uv.exe" "$HOME/AppData/Local/hermes/bin/uv.exe"; do
    if [[ -x "$p" ]]; then
      UV_CMD="$p"
      break
    fi
  done
fi
require_cmd "$UV_CMD"
log_info "uv $($UV_CMD --version)"

# Python pin check — .python-version 3.13
if [[ -f ".python-version" ]]; then
  expected="$(cat .python-version | tr -d '[:space:]')"
  actual="$($UV_CMD run python --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)"
  # allow 3.13.x matches 3.13
  if [[ "$actual" != "${expected%.*}"* && "$actual" != "$expected"* ]]; then
    log_warn "Python $actual does not match pinned $expected in .python-version — uv will handle it"
  else
    log_info "Python $actual matches pinned $expected"
  fi
fi

if command -v docker >/dev/null 2>&1; then
  log_info "docker $(docker --version 2>&1 | head -n1)"
else
  log_warn "docker not found — docker-up will fail until Docker Desktop is installed"
fi

if [[ ! -f ".env" ]]; then
  log_warn ".env not found — copying from .env.example (fill Databricks values)"
  cp .env.example .env
fi

log_info "uv sync (one venv via uv)"
$UV_CMD sync

log_success "Setup complete — run 'make lint' and 'make test' to verify"
