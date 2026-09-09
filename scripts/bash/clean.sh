#!/usr/bin/env bash
set -euo pipefail
docker compose -f docker/compose.yml down -v || true
rm -rf logs/ spark-warehouse/ metastore_db/ derby.log
echo "Clean done"
