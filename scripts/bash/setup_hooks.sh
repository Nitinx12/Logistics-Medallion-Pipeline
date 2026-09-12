#!/bin/bash
# setup_hooks.sh - install git hooks for FreightLake
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

git config core.hooksPath .githooks
chmod +x .githooks/pre-commit
echo "[setup_hooks] git hooks installed from .githooks (core.hooksPath=.githooks)"
echo "[setup_hooks] pre-commit will run ruff check and secret scan on every commit"
