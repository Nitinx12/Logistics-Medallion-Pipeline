#!/usr/bin/env bash
log_info() { echo -e "\033[1;34m[INFO]\033[0m $*"; }
log_success() { echo -e "\033[1;32m[OK]\033[0m $*"; }
log_error() { echo -e "\033[1;31m[ERR]\033[0m $*" >&2; }
require_cmd() { command -v "$1" >/dev/null 2>&1 || { log_error "Missing $1"; exit 1; }; }
