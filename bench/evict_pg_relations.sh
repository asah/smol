#!/bin/bash

psql=/usr/local/pgsql/bin/psql

# Return relfilenode paths for table + index
get_relation_files() {
  local DB="$1" TB="$2"
  set -e
  DATA=/home/postgres/pgdata/base
  DB_OID=$($psql -U postgres -Atc "SELECT oid FROM pg_database WHERE datname='$DB';")
  readarray -t OIDS < <($psql -U postgres -d "$DB" -Atc "
      SELECT c.relfilenode
      FROM pg_class c
      WHERE c.relname IN ('"$TB"', '"$TB"_k_idx')")
  for O in "${OIDS[@]}"; do
      find "$DATA/$DB_OID" -type f -name "${O}*" -print
  done
}

# Evict OS page cache for our relation files (heap + index)
evict_os_cache_for_relations() {
  local DB="$1" TB="$2"
  echo "==> Evicting OS cache for ${DB}.${TB} (heap + index) via vmtouch..."
  # Enumerate files and evict each one
  mapfile -t FILES < <(get_relation_files "$DB" "$TB")
  if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "!! No relation files found to evict" ; return 1
  fi
  for F in "${FILES[@]}"; do
    echo "    vmtouch -e $F"
    vmtouch -q -e "$F" || true
  done
  echo "==> Eviction requests issued."
}

# (Optional) Verify residency after eviction; expect 0 pages if fully evicted
check_os_residency() {
  local DB="$1" TB="$2"
  echo "==> Checking OS cache residency with vmtouch..."
  mapfile -t FILES < <(get_relation_files "$DB" "$TB")
  for F in "${FILES[@]}"; do
    vmtouch "$F" | sed 's/^/    /'
  done
}

evict_os_cache_for_relations postgres $1
