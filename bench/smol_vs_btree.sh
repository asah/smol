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
#                    [--equal-on-a VAL] [--sweep PCTS] [--workers LIST]
#
# Defaults (can also be provided via env):
#   ROWS=5000000 THRESH=15000 BATCH=250000 CHUNK_MIN=50000 SINGLECOL=0 TIMEOUT_SEC=30 KILL_AFTER=5 DBNAME=postgres COLTYPE=int2
#   EQUAL_ON_A="" (unset) SWEEP="" (e.g., 10,50,90,99) WORKERS="5" (comma-list)

ROWS=${ROWS:-5000000}
THRESH=${THRESH:-15000}
BATCH=${BATCH:-250000}
CHUNK_MIN=${CHUNK_MIN:-50000}
SINGLECOL=${SINGLECOL:-0}
TIMEOUT_SEC=${TIMEOUT_SEC:-30}
KILL_AFTER=${KILL_AFTER:-5}
DBNAME=${DBNAME:-postgres}
COLTYPE=${COLTYPE:-int2}
EQUAL_ON_A=${EQUAL_ON_A:-}
SWEEP=${SWEEP:-}
WORKERS=${WORKERS:-5}

# Choose random range per type (can be overridden via RMAX_A/RMAX_B)
case "$COLTYPE" in
  int2) RMAX_A=${RMAX_A:-32767}; RMAX_B=${RMAX_B:-32767};;
  int4) RMAX_A=${RMAX_A:-1000000000}; RMAX_B=${RMAX_B:-1000000000};;
  int8) RMAX_A=${RMAX_A:-9000000000000000000}; RMAX_B=${RMAX_B:-9000000000000000000};;
  *) echo "# ERROR: unsupported COLTYPE=$COLTYPE" >&2; exit 2;;
esac

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
      --equal-on-a VAL Add "AND a = VAL" to two-column queries
      --sweep PCTS     Comma list of percentiles of b (e.g., 10,50,90,99) to run query sweeps
      --workers LIST   Comma list of worker counts (e.g., 0,1,2,3,5) for scaling
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
    --equal-on-a) EQUAL_ON_A="$2"; shift 2;;
    --sweep) SWEEP="$2"; shift 2;;
    --workers) WORKERS="$2"; shift 2;;
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

echo "# Preparing schema and data (rows=${ROWS}, coltype=${COLTYPE}) using table ${TBL}" >&2
${PSQL_TMO} <<SQL
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;
CREATE UNLOGGED TABLE ${TBL}(a ${COLTYPE}, b ${COLTYPE});
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
    if ${PSQL_TMO} -c "INSERT INTO ${TBL} SELECT (random()*${RMAX_A})::${COLTYPE}, (random()*${RMAX_B})::${COLTYPE} FROM generate_series(${cur}, $((cur + chunk - 1)));"; then
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
    ${PSQL_TMO} -c "SET statement_timeout=${stmt_ms}; CHECKPOINT";
    ${PSQL_TMO} -c "SET vacuum_freeze_min_age=0";
    ${PSQL_TMO} -c "SET vacuum_freeze_table_age=0";
    # Split VACUUM and ANALYZE to keep each under statement_timeout
    # VACUUM must run outside a transaction; do not bundle with SET in one -c
    ${PSQL_TMO} -c "VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) ${TBL}";
    ${PSQL_TMO} -c "SET statement_timeout=${stmt_ms}; ANALYZE ${TBL}";
  fi

  local t1=$(date +%s%3N)
  local build_ms=$((t1 - t0))
  local size_mb=$(${PSQL_TMO} -c "SELECT round(pg_relation_size('${idxname}')/1024.0/1024.0,2)")

  ${PSQL_TMO} -c "SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; \
                    SET max_parallel_workers_per_gather=5; SET max_parallel_workers=5; \
                    SET parallel_setup_cost=0; SET parallel_tuple_cost=0; \
                    SET min_parallel_index_scan_size=0; SET min_parallel_table_scan_size=0;"
  local query
  if [ "${SINGLECOL:-0}" -eq 1 ]; then
    query="SELECT sum(b::bigint) FROM ${TBL} WHERE b > ${THRESH};"
  else
    query="SELECT sum(a::bigint) FROM ${TBL} WHERE b > ${THRESH}"
    if [ -n "${EQUAL_ON_A}" ]; then
      query+=" AND a = ${EQUAL_ON_A}"
    fi
    query+=";"
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

# Compute percentile threshold of b using percentile_disc(pct)
percentile_thresh() {
  local pct=$1
  # Normalize to fraction in [0,1]
  local frac
  frac=$(awk -v p="${pct}" 'BEGIN{printf "%.6f", p/100.0}')
  ${PSQL_TMO} -c "SELECT percentile_disc(${frac}) WITHIN GROUP (ORDER BY b) FROM ${TBL};"
}

# Run a sweep of thresholds and worker counts for the current index
run_sweep() {
  local idx_type=$1   # btree|smol
  local wlist=$2      # comma-separated workers
  local plist=$3      # comma-separated percentiles
  local size_mb=$4    # index size to include in output (optional)
  local header_printed=${5:-0}
  if [ "${header_printed}" -eq 0 ]; then
    echo
    echo "SweepIndex,Type,Workers,Threshold,EqualOnA,Query_ms,Plan"
  fi
  IFS=',' read -ra WARR <<<"${wlist}"
  IFS=',' read -ra PARR <<<"${plist}"
  for w in "${WARR[@]}"; do
    ${PSQL_TMO} -c "SET max_parallel_workers_per_gather=${w}; SET max_parallel_workers=${w}; \
                     SET parallel_setup_cost=0; SET parallel_tuple_cost=0; \
                     SET min_parallel_index_scan_size=0; SET min_parallel_table_scan_size=0;"
    for p in "${PARR[@]}"; do
      thr=$(percentile_thresh "$p")
      # Build the query with the new threshold
      local q
      if [ "${SINGLECOL:-0}" -eq 1 ]; then
        q="SELECT sum(b::bigint) FROM ${TBL} WHERE b > ${thr};"
      else
        q="SELECT sum(a::bigint) FROM ${TBL} WHERE b > ${thr}"
        if [ -n "${EQUAL_ON_A}" ]; then q+=" AND a = ${EQUAL_ON_A}"; fi
        q+=";"
      fi
      # Capture plan line and execution time
      local pline exec_ms
      pline=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${q}" | grep -E 'Index (Only )?Scan' | head -n1 | sed 's/^\s\+//')
      exec_ms=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${q}" | grep -E 'Execution Time:' | awk '{print $(NF-1)}')
      echo "${idx_type},${idx_type},${w},${thr},${EQUAL_ON_A},${exec_ms},${pline}"
    done
  done
}

if [ "${SINGLECOL:-0}" -eq 1 ]; then
  echo "# Phase 1: BTREE (${TBL}_b_btree on (b))" >&2
  bt_row=$(run_bench_phase ${TBL}_b_btree "CREATE INDEX ${TBL}_b_btree ON ${TBL}(b);" 0)
  if [ -n "${SWEEP}" ]; then
    # Parse BTREE size for later reference
    bt_size=$(echo "${bt_row}" | awk -F'|' '{print $4}')
    run_sweep btree "${WORKERS}" "${SWEEP}" "${bt_size}" 1
  fi
  echo "# Cleaning up BTREE index" >&2
  ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_b_btree;"
  echo "# Phase 2: SMOL (${TBL}_b_smol on (b))" >&2
  sm_row=$(run_bench_phase ${TBL}_b_smol "CREATE INDEX ${TBL}_b_smol ON ${TBL} USING smol(b);" 1)
  if [ -n "${SWEEP}" ]; then
    sm_size=$(echo "${sm_row}" | awk -F'|' '{print $4}')
    run_sweep smol "${WORKERS}" "${SWEEP}" "${sm_size}" 1
  fi
else
  echo "# Phase 1: BTREE (${TBL}_ba_btree on (b) INCLUDE (a))" >&2
  bt_row=$(run_bench_phase ${TBL}_ba_btree "CREATE INDEX ${TBL}_ba_btree ON ${TBL} USING btree(b) INCLUDE (a);" 0)
  # Capture baseline sum/count for correctness before dropping BTREE
  base_sum=$(${PSQL_TMO} -c "SELECT sum(a::bigint) FROM ${TBL} WHERE b > ${THRESH}")
  base_cnt=$(${PSQL_TMO} -c "SELECT count(*)::bigint FROM ${TBL} WHERE b > ${THRESH}")
  if [ -n "${SWEEP}" ]; then
    bt_size=$(echo "${bt_row}" | awk -F'|' '{print $4}')
    run_sweep btree "${WORKERS}" "${SWEEP}" "${bt_size}" 1
  fi
  echo "# Cleaning up BTREE index" >&2
  ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_ba_btree;"
  echo "# Phase 2: SMOL (${TBL}_ba_smol on (b,a))" >&2
  sm_row=$(run_bench_phase ${TBL}_ba_smol "CREATE INDEX ${TBL}_ba_smol ON ${TBL} USING smol(b,a);" 1)
  # Stats for SMOL as well for fair selectivity
  ${PSQL_TMO} -c "ANALYZE ${TBL};"
  # Compute SMOL sum/count for correctness and print a line
  sm_sum=$(${PSQL_TMO} -c "SELECT sum(a::bigint) FROM ${TBL} WHERE b > ${THRESH}")
  sm_cnt=$(${PSQL_TMO} -c "SELECT count(*)::bigint FROM ${TBL} WHERE b > ${THRESH}")
  correct=false
  if [ "${base_sum}" = "${sm_sum}" ] && [ "${base_cnt}" = "${sm_cnt}" ]; then correct=true; fi
  echo "Correct,${correct},sum_bt=${base_sum},sum_smol=${sm_sum},cnt_bt=${base_cnt},cnt_smol=${sm_cnt}"
  if [ -n "${SWEEP}" ]; then
    sm_size=$(echo "${sm_row}" | awk -F'|' '{print $4}')
    run_sweep smol "${WORKERS}" "${SWEEP}" "${sm_size}" 1
  fi
fi

echo
echo "Index,Type,Build_ms,Size_MB,Query_ms,Plan"
IFS='|' read -r n is b s q p <<<"${bt_row}"; echo "${n},btree,${b},${s},${q},${p}"
IFS='|' read -r n is b s q p <<<"${sm_row}"; echo "${n},smol,${b},${s},${q},${p}"
