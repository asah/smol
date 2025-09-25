#!/usr/bin/env bash
set -euo pipefail

# Gap-maximizing scenario: single-key int2, COUNT(*) with predicate on key,
# compare BTREE(b) vs SMOL(b). Reports size and chosen scan/index.

if [ "$(id -un)" != "postgres" ]; then
  echo "# ERROR: run as postgres (use make bench-gap-scenario)" >&2; exit 2
fi

ROWS=${ROWS:-10000000}
SELECTIVITY=${SELECTIVITY:-0.5}
WORKERS=${WORKERS:-5}
TIMEOUT_SEC=${TIMEOUT_SEC:-60}
KILL_AFTER=${KILL_AFTER:-5}
DBNAME=${DBNAME:-postgres}
TBL=${TBL:-gap_bench}

# Inherit SMOL profiling across all psql sessions by setting PGOPTIONS
if [ "${SMOL_PROFILE:-0}" = "1" ]; then export PGOPTIONS="-c smol.profile=on"; fi
PSQL="/usr/local/pgsql/bin/psql -X -v ON_ERROR_STOP=1 -d ${DBNAME} -qAt"
PSQL_TMO="timeout -k ${KILL_AFTER}s ${TIMEOUT_SEC}s ${PSQL}"

# Session GUCs
${PSQL_TMO} -c "SET client_min_messages=warning" >/dev/null
if [ "${SMOL_PROFILE:-0}" = "1" ]; then
  ${PSQL_TMO} -c "SET smol.profile=on" >/dev/null
fi
if [ -n "${SMOL_CLAIM:-}" ]; then
  ${PSQL_TMO} -c "SET smol.parallel_claim_batch=${SMOL_CLAIM}" >/dev/null
fi
${PSQL_TMO} -c "SET max_parallel_workers_per_gather=${WORKERS}; SET max_parallel_workers=${WORKERS}; \
                SET parallel_setup_cost=0; SET parallel_tuple_cost=0; \
                SET min_parallel_index_scan_size=0; SET min_parallel_table_scan_size=0;" >/dev/null

echo "Index,Type,Build_ms,Size_MB,Query_ms,Plan"

# Validate selectivity range [0.0 .. 1.0]
case "$SELECTIVITY" in
  0|0.*|1|1.*) : ;; # we'll validate strictly below
esac
if [[ ! "$SELECTIVITY" =~ ^(0(\.[0-9]+)?|1(\.0+)?)$ ]]; then
  echo "# ERROR: SELECTIVITY out of bounds: '$SELECTIVITY' (expected 0.0..1.0)" >&2
  exit 2
fi

# Prepare schema
${PSQL_TMO} <<SQL
CREATE EXTENSION IF NOT EXISTS smol;
DROP TABLE IF EXISTS ${TBL} CASCADE;
CREATE UNLOGGED TABLE ${TBL}(b int2);
ALTER TABLE ${TBL} SET (autovacuum_enabled = off);
SQL

# Load data: two windows around 0 for selectivity control
W=16000
total=${ROWS}
exact=1
sel=${SELECTIVITY}
match=$(awk -v s="$sel" -v n="$total" 'BEGIN{printf "%d", s*n}')
if [ "$match" -lt 0 ]; then match=0; fi
match_rand=$(( match > exact ? match - exact : 0 ))
nonmatch=$(( total - exact - match_rand ))

insert_range() {
  local nrows=$1; local base=$2; local span=$3
  if [ "$nrows" -le 0 ]; then return; fi
  local cur=1
  local BATCH=${BATCH:-250000}
  while [ "$cur" -le "$nrows" ]; do
    local end=$(( cur + BATCH - 1 )); if [ $end -gt $nrows ]; then end=$nrows; fi
    local cnt=$(( end - cur + 1 ))
    ${PSQL_TMO} -c "INSERT INTO ${TBL} SELECT (${base} + floor(random()*${span}))::int2 FROM generate_series(1, ${cnt});" >/dev/null
    cur=$(( end + 1 ))
  done
}

# non-matching below 0: [-W, 0)
insert_range "$nonmatch" "-16000" "$W"
# matching above 0: (0, W]
insert_range "$match_rand" "1" "$W"
# exact match at 0
${PSQL_TMO} -c "INSERT INTO ${TBL} VALUES (0)" >/dev/null
${PSQL_TMO} -c "ANALYZE ${TBL}" >/dev/null

# Helper to print one line CSV for an index
run_one() {
  local idxname=$1
  local create_stmt=$2
  local label=$3
  local t0=$(date +%s%3N)
  ${PSQL_TMO} -c "${create_stmt}" >/dev/null || true
  local t1=$(date +%s%3N)
  local build_ms=$((t1 - t0))
  local size_mb=$(${PSQL_TMO} -c "SELECT round(pg_relation_size('${idxname}')/1024.0/1024.0,2)")
  # COUNT(*) IOS query on b > -1
  local q="SELECT count(*) FROM ${TBL} WHERE b > -1"
  local plan_all exec_ms chosen
  plan_all=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, TIMING ON, BUFFERS OFF) ${q}")
  exec_ms=$(echo "$plan_all" | grep -E 'Execution Time:' | awk '{print $(NF-1)}')
  chosen=$(echo "$plan_all" | grep -Em1 "Index Only Scan using |Index Scan using |Bitmap Index Scan on |Bitmap Heap Scan on |Seq Scan on " | sed 's/^\s\+//')
  if [ -z "$chosen" ]; then chosen=$(echo "$plan_all" | head -n1 | sed 's/^\s\+//'); fi
  chosen=$(echo "$chosen" | tr '\n' ' ' | sed 's/\s\+/ /g')
  echo "${label},${label},${build_ms},${size_mb},${exec_ms},${chosen}"
}

# Build and run BTREE IOS
run_one "${TBL}_b_btree" "CREATE INDEX ${TBL}_b_btree ON ${TBL} USING btree(b)" "btree"
# Build and run SMOL IOS
run_one "${TBL}_b_smol" "CREATE INDEX ${TBL}_b_smol ON ${TBL} USING smol(b)" "smol"

echo "Done"
