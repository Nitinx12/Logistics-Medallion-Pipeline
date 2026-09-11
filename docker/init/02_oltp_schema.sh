#!/bin/bash
set -e
# Apply OLTP schema to freightlake_oltp DB after DB creation
# Runs only on first init when volume is empty (docker-entrypoint-initdb.d)

# Defaults for local compose where .env may not define these
POSTGRES_SUPERUSER="${POSTGRES_SUPERUSER:-postgres}"
POSTGRES_OLTP_USER="${POSTGRES_OLTP_USER:-freightlake_oltp_user}"

# Guard: if no DDL files present, skip gracefully
if [ ! -d /tmp/oltp_schema ] || [ -z "$(ls -A /tmp/oltp_schema/*.sql 2>/dev/null)" ]; then
  echo "[freightlake] no oltp_schema/*.sql found, skipping OLTP schema apply"
  exit 0
fi

echo "[freightlake] applying OLTP schema to freightlake_oltp as $POSTGRES_SUPERUSER"
for f in /tmp/oltp_schema/*.sql; do
  if [[ "$f" == *README.md ]]; then continue; fi
  # skip if glob did not expand (no files)
  [ -e "$f" ] || continue
  echo "  -> $f"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_SUPERUSER" --dbname "freightlake_oltp" -f "$f"
done
# Grant privileges so seed.py and bronze jobs can read/write as oltp_user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_SUPERUSER" --dbname "freightlake_oltp" <<EOSQL
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "$POSTGRES_OLTP_USER";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "$POSTGRES_OLTP_USER";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "$POSTGRES_OLTP_USER";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "$POSTGRES_OLTP_USER";
EOSQL
echo "[freightlake] OLTP schema done with grants to $POSTGRES_OLTP_USER"