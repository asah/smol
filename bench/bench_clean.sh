#!/usr/bin/env bash
set -euo pipefail

PSQL=${PSQL:-/usr/local/pgsql/bin/psql}
DB=${DBNAME:-postgres}

cat <<'SQL' | "$PSQL" -X -v ON_ERROR_STOP=1 -d "$DB" -q
WITH q AS (
  SELECT format('DROP TABLE IF EXISTS %I.%I CASCADE;', n.nspname, c.relname) AS cmd
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE c.relkind = 'r' AND c.relname LIKE 'bm\_%'
)
SELECT cmd FROM q \gexec
SQL

echo "Bench tables dropped (bm_%)."

