#!/bin/bash
set -e
# Apply OLTP schema to freightlake_oltp DB after DB creation
# Runs only on first init when volume is empty

echo "[freightlake] applying OLTP schema to freightlake_oltp"
for f in /tmp/oltp_schema/*.sql; do
  # skip README
  if [[ "$f" == *README.md ]]; then continue; fi
  echo "  -> $f"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_SUPERUSER" --dbname "freightlake_oltp" -f "$f"
done
echo "[freightlake] OLTP schema done"
