#!/usr/bin/env bash
set -euo pipefail

# Hard timeout wrapper for all psql calls to avoid hangs.
# TIMEOUT_SEC: max seconds to allow a single psql invocation (default 30)
# KILL_AFTER : grace before SIGKILL after timeout (seconds, default 5)
TIMEOUT_SEC=${TIMEOUT_SEC:-30}
KILL_AFTER=${KILL_AFTER:-5}
# Enforce hard caps per project policy
if [ "$TIMEOUT_SEC" -gt 30 ]; then TIMEOUT_SEC=30; fi
if [ "$KILL_AFTER" -gt 5 ]; then KILL_AFTER=5; fi
command -v timeout >/dev/null 2>&1 || { echo "# ERROR: 'timeout' not found; please install coreutils" >&2; exit 1; }

# Fair IOS benchmark for SMOL vs BTREE on 5M rows (int2,int2)
# - Measures index size and query speed for a representative range query
# - Enforces apples-to-apples IOS:
#   * SINGLECOL=0 (default): BTREE (b) INCLUDE (a) vs SMOL (b,a); query sum(a)
#   * SINGLECOL=1          : BTREE (b) vs SMOL (b); query sum(b)
# - Assumes PostgreSQL 18 is installed at /usr/local/pgsql and running in container

ROWS=${ROWS:-5000000}
THRESH=${THRESH:-15000}
DBNAME=${DBNAME:-postgres}
PSQL="/usr/local/pgsql/bin/psql -X -v ON_ERROR_STOP=1 -d ${DBNAME} -qAt"
PSQL_TMO="timeout -k ${KILL_AFTER}s ${TIMEOUT_SEC}s ${PSQL}"

# Use unique table/index names per run to avoid drop/locks on leftovers
TS=$(date +%s)
RAND=$RANDOM
TBL=${TBL:-mm_${TS}_${RAND}}

echo "# Preparing schema and data (rows=${ROWS}) using table ${TBL}" >&2
${PSQL_TMO} <<SQL
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;
CREATE UNLOGGED TABLE ${TBL}(a int2, b int2);
ALTER TABLE ${TBL} SET (autovacuum_enabled = off);
SQL

# Batched load to respect per-psql timeouts
BATCH=${BATCH:-250000}
CHUNK_MIN=${CHUNK_MIN:-50000}
cur=1
while [ "$cur" -le "$ROWS" ]; do
  # determine intended end of window
  end=$(( cur + BATCH - 1 ))
  if [ $end -gt $ROWS ]; then end=$ROWS; fi
  # adaptive chunk: shrink on timeout until success
  chunk=$(( end - cur + 1 ))
  while :; do
    echo "# Loading rows ${cur}..$((cur + chunk - 1)) (chunk=${chunk})" >&2
    if ${PSQL_TMO} -c "INSERT INTO ${TBL} SELECT (random()*32767)::int2, (random()*32767)::int2 FROM generate_series(${cur}, $((cur + chunk - 1)));"; then
      cur=$(( cur + chunk ))
      break
    else
      rc=$?
      if [ "$rc" -eq 124 ]; then
        # timed out: reduce chunk size
        if [ "$chunk" -le "$CHUNK_MIN" ]; then
          echo "# ERROR: insert chunk timed out even at CHUNK_MIN=${CHUNK_MIN}" >&2
          exit 1
        fi
        chunk=$(( chunk / 2 ))
        if [ "$chunk" -lt "$CHUNK_MIN" ]; then chunk=$CHUNK_MIN; fi
        echo "# Timeout on insert; reducing chunk to ${chunk}" >&2
      else
        echo "# ERROR: insert failed with rc=${rc}" >&2
        exit ${rc}
      fi
    fi
  done
done
${PSQL_TMO} -c "ANALYZE ${TBL};"

run_bench_phase() {
  local idxname=$1
  local create_stmt=$2
  local is_smol=$3

  echo "# Building index ${idxname}" >&2
  local t0=$(date +%s%3N)
  # Protect server from lingering backends when client timeout kills psql.
  # Use server-side statement_timeout slightly below TIMEOUT_SEC.
  local stmt_ms=$(( (TIMEOUT_SEC>2 ? TIMEOUT_SEC-2 : TIMEOUT_SEC) * 1000 ))
  ${PSQL_TMO} -c "SET statement_timeout=${stmt_ms}; ${create_stmt}"
  # For BTREE, force all-visible so IOS is possible; harmless for SMOL
  echo "# checkpoint + freeze/analyze table (for IOS)" >&2
  if [ "${is_smol}" -eq 0 ]; then
    # For BTREE, ensure the base table is all-visible so IOS is possible
    ${PSQL_TMO} -c "CHECKPOINT";
    ${PSQL_TMO} -c "SET vacuum_freeze_min_age=0";
    ${PSQL_TMO} -c "SET vacuum_freeze_table_age=0";
    ${PSQL_TMO} -c "VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) ${TBL}";
  else
    # SMOL does not require VACUUM; analyze already done above
    :
  fi
  local t1=$(date +%s%3N)
  local build_ms=$((t1 - t0))

  # Size in MB
  local size_mb=$(${PSQL_TMO} -c "SELECT round(pg_relation_size('${idxname}')/1024.0/1024.0,2)")

  # Enforce IOS and disable other paths
  ${PSQL_TMO} -c "SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;"

  # Choose fair query shape
  local query
  if [ "${SINGLECOL:-0}" -eq 1 ]; then
    query="SELECT sum(b::bigint) FROM ${TBL} WHERE b > ${THRESH};"
  else
    query="SELECT sum(a::bigint) FROM ${TBL} WHERE b > ${THRESH};"
  fi

  # Warm-up run
  ${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${query}" >/dev/null

  # Capture plan node (first line with Index*Scan)
  local pline
  pline=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${query}" \
            | grep -E 'Index (Only )?Scan' | head -n1 | sed 's/^\s\+//')

  # Timed run: capture Execution Time (ms)
  local exec_ms
  exec_ms=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${query}" \
            | grep -E 'Execution Time:' | awk '{print $(NF-1)}')

  echo "${idxname}|${is_smol}|${build_ms}|${size_mb}|${exec_ms}|${pline}"
}

if [ "${SINGLECOL:-0}" -eq 1 ]; then
  echo "# Phase 1: BTREE (${TBL}_b_btree on (b))" >&2
  bt_row=$(run_bench_phase ${TBL}_b_btree "CREATE INDEX ${TBL}_b_btree ON ${TBL}(b);" 0)
  echo "# Cleaning up BTREE index" >&2
  ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_b_btree;"
  echo "# Phase 2: SMOL (${TBL}_b_smol on (b))" >&2
  sm_row=$(run_bench_phase ${TBL}_b_smol "CREATE INDEX ${TBL}_b_smol ON ${TBL} USING smol(b);" 1)
else
  echo "# Phase 1: BTREE (${TBL}_ba_btree on (b) INCLUDE (a))" >&2
  bt_row=$(run_bench_phase ${TBL}_ba_btree "CREATE INDEX ${TBL}_ba_btree ON ${TBL} USING btree(b) INCLUDE (a);" 0)
  echo "# Cleaning up BTREE index" >&2
  ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_ba_btree;"
  echo "# Phase 2: SMOL (${TBL}_ba_smol on (b,a))" >&2
  sm_row=$(run_bench_phase ${TBL}_ba_smol "CREATE INDEX ${TBL}_ba_smol ON ${TBL} USING smol(b,a);" 1)
fi

# Print side-by-side summary
echo
echo "Index,Type,Build_ms,Size_MB,Query_ms,Plan"
IFS='|' read -r n is b s q p <<<"${bt_row}"; echo "${n},btree,${b},${s},${q},${p}"
IFS='|' read -r n is b s q p <<<"${sm_row}"; echo "${n},smol,${b},${s},${q},${p}"
