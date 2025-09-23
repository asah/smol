#!/usr/bin/env bash
set -euo pipefail

# SMOL vs BTREE benchmark
# - Supports configurable table sizes and options via CLI flags or env vars
# - Enforces hard client timeout per psql invocation (<= 30s) and server-side
#   statement_timeout below that for CREATE INDEX to avoid lingering backends.
#
# Usage:
#   smol_vs_btree.sh [--rows N|-r N] [--singlecol 0|1|-s 0|1]
#                    [--thresh N|-t N] [--batch N|-B N]
#                    [--timeout SEC] [--kill-after SEC]
#                    [--dbname NAME]
#
# Defaults (can also be provided via env):
#   ROWS=5000000 THRESH=15000 BATCH=250000 CHUNK_MIN=50000 SINGLECOL=0 TIMEOUT_SEC=30 KILL_AFTER=5 DBNAME=postgres

ROWS=${ROWS:-5000000}
THRESH=${THRESH:-15000}
BATCH=${BATCH:-250000}
CHUNK_MIN=${CHUNK_MIN:-50000}
SINGLECOL=${SINGLECOL:-0}
TIMEOUT_SEC=${TIMEOUT_SEC:-30}
KILL_AFTER=${KILL_AFTER:-5}
DBNAME=${DBNAME:-postgres}

print_usage() {
  cat <<USAGE
Usage: $0 [options]
  -r, --rows N         Number of rows to generate (default: ${ROWS})
  -s, --singlecol 0|1  Compare single-col indexes on (b) (1) or (b,a) (0) (default: ${SINGLECOL})
  -t, --thresh N       Filter threshold for b in queries (default: ${THRESH})
  -B, --batch N        Insert batch size per psql invocation (default: ${BATCH})
      --timeout SEC    Per-psql timeout cap (<=30) (default: ${TIMEOUT_SEC})
      --kill-after SEC Grace before SIGKILL on timeout (<=5) (default: ${KILL_AFTER})
      --dbname NAME    Database name (default: ${DBNAME})
  -h, --help           Show this help
USAGE
}

# Parse CLI args
while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--rows) ROWS="$2"; shift 2;;
    -s|--singlecol) SINGLECOL="$2"; shift 2;;
    -t|--thresh|--threshold) THRESH="$2"; shift 2;;
    -B|--batch) BATCH="$2"; shift 2;;
    --timeout) TIMEOUT_SEC="$2"; shift 2;;
    --kill-after) KILL_AFTER="$2"; shift 2;;
    -d|--dbname|--db) DBNAME="$2"; shift 2;;
    -h|--help) print_usage; exit 0;;
    *) echo "# ERROR: Unknown option: $1" >&2; print_usage; exit 2;;
  esac
done

# Clamp timeouts per project policy
if [ "$TIMEOUT_SEC" -gt 30 ]; then TIMEOUT_SEC=30; fi
if [ "$KILL_AFTER" -gt 5 ]; then KILL_AFTER=5; fi
command -v timeout >/dev/null 2>&1 || { echo "# ERROR: 'timeout' not found; please install coreutils" >&2; exit 1; }

PSQL="/usr/local/pgsql/bin/psql -X -v ON_ERROR_STOP=1 -d ${DBNAME} -qAt"
PSQL_TMO="timeout -k ${KILL_AFTER}s ${TIMEOUT_SEC}s ${PSQL}"

# Unique table name per run
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
cur=1
while [ "$cur" -le "$ROWS" ]; do
  end=$(( cur + BATCH - 1 ))
  if [ $end -gt $ROWS ]; then end=$ROWS; fi
  chunk=$(( end - cur + 1 ))
  while :; do
    echo "# Loading rows ${cur}..$((cur + chunk - 1)) (chunk=${chunk})" >&2
    if ${PSQL_TMO} -c "INSERT INTO ${TBL} SELECT (random()*32767)::int2, (random()*32767)::int2 FROM generate_series(${cur}, $((cur + chunk - 1)));"; then
      cur=$(( cur + chunk ))
      break
    else
      rc=$?
      if [ "$rc" -eq 124 ]; then
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
  local stmt_ms=$(( (TIMEOUT_SEC>2 ? TIMEOUT_SEC-2 : TIMEOUT_SEC) * 1000 ))
  if ${PSQL_TMO} -c "SET statement_timeout=${stmt_ms}; ${create_stmt}"; then
    :
  else
    local rc=$?
    local t1=$(date +%s%3N)
    local build_ms=$((t1 - t0))
    # Emit a CSV row with empty size/plan on failure
    echo "${idxname}|${is_smol}|${build_ms}|||" 
    return
  fi

  echo "# checkpoint + freeze/analyze table (for IOS)" >&2
  if [ "${is_smol}" -eq 0 ]; then
    ${PSQL_TMO} -c "CHECKPOINT";
    ${PSQL_TMO} -c "SET vacuum_freeze_min_age=0";
    ${PSQL_TMO} -c "SET vacuum_freeze_table_age=0";
    ${PSQL_TMO} -c "VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) ${TBL}";
  fi

  local t1=$(date +%s%3N)
  local build_ms=$((t1 - t0))
  local size_mb=$(${PSQL_TMO} -c "SELECT round(pg_relation_size('${idxname}')/1024.0/1024.0,2)")

  ${PSQL_TMO} -c "SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;"
  local query
  if [ "${SINGLECOL:-0}" -eq 1 ]; then
    query="SELECT sum(b::bigint) FROM ${TBL} WHERE b > ${THRESH};"
  else
    query="SELECT sum(a::bigint) FROM ${TBL} WHERE b > ${THRESH};"
  fi

  ${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${query}" >/dev/null || true
  local pline
  pline=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${query}" \
            | grep -E 'Index (Only )?Scan' | head -n1 | sed 's/^\s\+//')
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

echo
echo "Index,Type,Build_ms,Size_MB,Query_ms,Plan"
IFS='|' read -r n is b s q p <<<"${bt_row}"; echo "${n},btree,${b},${s},${q},${p}"
IFS='|' read -r n is b s q p <<<"${sm_row}"; echo "${n},smol,${b},${s},${q},${p}"

