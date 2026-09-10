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
# Grant ownership/privileges so seed.py and bronze jobs can truncate/copy as oltp_user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_SUPERUSER" --dbname "freightlake_oltp" <<EOSQL
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "$POSTGRES_OLTP_USER";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "$POSTGRES_OLTP_USER";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "$POSTGRES_OLTP_USER";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "$POSTGRES_OLTP_USER";
EOSQL
echo "[freightlake] OLTP schema done with grants to $POSTGRES_OLTP_USER"
