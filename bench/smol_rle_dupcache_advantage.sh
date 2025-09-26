#!/usr/bin/env bash
set -euo pipefail

# SMOL RLE + dupcache advantage benchmark
# Goal: maximize SMOL advantage vs BTREE by using:
#  - single-key int2 with extreme duplicates (99% same key),
#  - multiple INCLUDE columns with identical values within runs,
#  - index-only aggregation (COUNT and SUM over includes) to force tuple materialization,
#  - 5-way parallel IOS.

if [ "$(id -un)" != "postgres" ]; then
  echo "# ERROR: run as postgres user (use: make bench-rle-advantage)" >&2
  exit 2
fi

ROWS=${ROWS:-50000000}
WORKERS=${WORKERS:-5}
INC=${INC:-8}
COLTYPE=${COLTYPE:-int2}   # int2 recommended to maximize density
DUF=${DUP_FRAC:-0.99}
SWEEP=${SWEEP:-}
TIMEOUT_SEC=${TIMEOUT_SEC:-1800}
KILL_AFTER=${KILL_AFTER:-10}
REUSE=${REUSE:-1}
DB=${DB:-postgres}

PSQL="/usr/local/pgsql/bin/psql -X -v ON_ERROR_STOP=1 -d ${DB} -qAt"
PSQL_TMO="timeout -k ${KILL_AFTER}s ${TIMEOUT_SEC}s ${PSQL}"
export PGOPTIONS="${PGOPTIONS:-} -c client_min_messages=warning -c enable_seqscan=off -c enable_bitmapscan=off -c enable_indexscan=off -c enable_indexonlyscan=on -c max_parallel_workers_per_gather=${WORKERS} -c max_parallel_workers=${WORKERS} -c parallel_setup_cost=0 -c parallel_tuple_cost=0 -c min_parallel_index_scan_size=0 -c min_parallel_table_scan_size=0"

# Force parallel IOS and prefer it strongly
common_gucs() {
  ${PSQL_TMO} -c "SET client_min_messages=warning;" || true
  ${PSQL_TMO} -c "SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexscan=off; SET enable_indexonlyscan=on;" || true
  ${PSQL_TMO} -c "SET max_parallel_workers_per_gather=${WORKERS}; SET max_parallel_workers=${WORKERS};" || true
  ${PSQL_TMO} -c "SET parallel_setup_cost=0; SET parallel_tuple_cost=0;" || true
  ${PSQL_TMO} -c "SET min_parallel_index_scan_size=0; SET min_parallel_table_scan_size=0;" || true
}

tbl=adv_rle_dupc

make_table() {
  # Define columns: b plus INC include columns a1..aINC
  cols="b ${COLTYPE}"
  for ((i=1;i<=INC;i++)); do cols="$cols, a${i} ${COLTYPE}"; done
  ${PSQL_TMO} -c "DROP TABLE IF EXISTS ${tbl} CASCADE;"
  ${PSQL_TMO} -c "CREATE UNLOGGED TABLE ${tbl}(${cols});"
  ${PSQL_TMO} -c "ALTER TABLE ${tbl} SET (autovacuum_enabled = off);"
}

load_data() {
  # heavy duplicates with identical INCLUDE values, plus filler based on DUP_FRAC
  local heavy=$(awk -v r=${ROWS} -v f=${DUF} 'BEGIN{printf "%d", r*f}')
  local filler=$(( ROWS - heavy ))
  echo "# Loading: heavy=${heavy}, filler=${filler} (ROWS=${ROWS}, DUP_FRAC=${DUF})"
  # INSERT heavy duplicates in batches
  local B=${BATCH:-250000}
  local cur=0
  while [ $cur -lt $heavy ]; do
    local rem=$(( heavy - cur ))
    local cnt=$(( rem < B ? rem : B ))
    # Build SELECT list: constant key 42, constant INCLUDE values
    local list="42"
    for ((i=1;i<=INC;i++)); do list="$list, ${i}"; done
    ${PSQL_TMO} -c "INSERT INTO ${tbl} SELECT ${list} FROM generate_series(1, ${cnt});"
    cur=$(( cur + cnt ))
  done
  # INSERT filler with distinct keys around the ends of int2 domain
  if [ $filler -gt 0 ]; then
    local cur2=0
    while [ $cur2 -lt $filler ]; do
      local rem=$(( filler - cur2 ))
      local cnt=$(( rem < B ? rem : B ))
      local list_random="((CASE WHEN random()<0.5 THEN -32768 ELSE 32767 END))::${COLTYPE}"
      # includes constant as well to maximize dup-cache effectiveness when b==42; irrelevant for filler scans
      local incs=""
      for ((i=1;i<=INC;i++)); do incs="$incs, ${i}"; done
      ${PSQL_TMO} -c "INSERT INTO ${tbl} SELECT ${list_random}${incs} FROM generate_series(1, ${cnt});"
      cur2=$(( cur2 + cnt ))
    done
  fi
  ${PSQL_TMO} -c "ANALYZE ${tbl};" || true
}

drop_indexes() { ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${tbl}_bt; DROP INDEX IF EXISTS ${tbl}_sm;" || true; }

build_bt() {
  echo "# Building BTREE(b) INCLUDE (a1..aINC)"
  drop_indexes
  ${PSQL_TMO} -c "CREATE INDEX ${tbl}_bt ON ${tbl} USING btree(b) INCLUDE ("$(for ((i=1;i<=INC;i++)); do printf 'a%d%s' $i $([ $i -lt $INC ] && echo ',' ); done)");"
  ${PSQL_TMO} -c "CHECKPOINT;" || true
  ${PSQL_TMO} -c "SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;" || true
  # VACUUM cannot run inside a transaction block; run it standalone
  ${PSQL_TMO} -c "VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) ${tbl};" || true
}

build_smol() {
  echo "# Building SMOL(b) INCLUDE (...), RLE on"
  drop_indexes
  ${PSQL_TMO} -c "SET smol.rle=on;"
  ${PSQL_TMO} -c "CREATE INDEX ${tbl}_sm ON ${tbl} USING smol(b) INCLUDE ("$(for ((i=1;i<=INC;i++)); do printf 'a%d%s' $i $([ $i -lt $INC ] && echo ',' ); done)");"
}

report_sizes() {
  ${PSQL_TMO} -c "SELECT 'btree' AS idx, pg_size_pretty(pg_relation_size('${tbl}_bt')) AS size;"
  ${PSQL_TMO} -c "SELECT 'smol'  AS idx, pg_size_pretty(pg_relation_size('${tbl}_sm')) AS size;"
}

run_queries() {
  common_gucs
  echo "# COUNT(*), equality on b=42"
  ${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) SELECT count(*) FROM ${tbl} WHERE b = 42;"
  echo "# SUM over includes enforces tuple materialization"
  ${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) SELECT "$(for ((i=1;i<=INC;i++)); do printf 'sum(a%d)::bigint%s' $i $([ $i -lt $INC ] && echo ' + '); done)" FROM ${tbl} WHERE b = 42;"
}

RESULTS_DIR=${RESULTS_DIR:-/workspace/results}
emit_csv_header() {
  mkdir -p "${RESULTS_DIR}" || true
  local f=${RESULTS_DIR}/rle_adv.csv
  if [ ! -s "$f" ]; then
    echo "rows,workers,inc,dup_frac,index,query,size_bytes,exec_ms" > "$f"
  fi
}

extract_exec_ms() {
  # read EXPLAIN output from stdin and print the final Execution Time ms
  sed -n 's/.*Execution Time: \([0-9.]*\) ms.*/\1/p' | tail -n1
}

run_one() {
  DUF="$1"
  echo "\n# === RUN dup_frac=${DUF} ==="
  common_gucs
  make_table
  load_data
  echo "# Rows in table:"; ${PSQL_TMO} -c "SELECT count(*) FROM ${tbl};"

  echo "\n# --- BTREE pass ---"
  build_bt
  ${PSQL_TMO} -c "SET smol.profile=off;" >/dev/null 2>&1 || true
  bt_size=$(${PSQL_TMO} -c "SELECT pg_relation_size('${tbl}_bt');")
  # COUNT
  bt_cnt_ms=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) SELECT count(*) FROM ${tbl} WHERE b = 42;" | extract_exec_ms)
  # SUM
  sum_sql="SELECT "$(for ((i=1;i<=INC;i++)); do printf 'sum(a%d)::bigint%s' $i $([ $i -lt $INC ] && echo ' + '); done)" FROM ${tbl} WHERE b = 42;"
  bt_sum_ms=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${sum_sql}" | extract_exec_ms)

  echo "\n# --- SMOL pass (RLE+dupcache) ---"
  build_smol
  ${PSQL_TMO} -c "SET smol.profile=on; SET smol.skip_dup_copy=on;" >/dev/null 2>&1 || true
  sm_size=$(${PSQL_TMO} -c "SELECT pg_relation_size('${tbl}_sm');")
  sm_cnt_ms=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) SELECT count(*) FROM ${tbl} WHERE b = 42;" | extract_exec_ms)
  sm_sum_ms=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${sum_sql}" | extract_exec_ms)

  emit_csv_header
  echo "${ROWS},${WORKERS},${INC},${DUF},btree,count,${bt_size},${bt_cnt_ms}" >> "${RESULTS_DIR}/rle_adv.csv"
  echo "${ROWS},${WORKERS},${INC},${DUF},btree,sum,${bt_size},${bt_sum_ms}" >> "${RESULTS_DIR}/rle_adv.csv"
  echo "${ROWS},${WORKERS},${INC},${DUF},smol,count,${sm_size},${sm_cnt_ms}" >> "${RESULTS_DIR}/rle_adv.csv"
  echo "${ROWS},${WORKERS},${INC},${DUF},smol,sum,${sm_size},${sm_sum_ms}" >> "${RESULTS_DIR}/rle_adv.csv"
}

main() {
  if [ -n "$SWEEP" ]; then
    IFS=',' read -ra FRACS <<< "$SWEEP"
    for d in "${FRACS[@]}"; do
      run_one "$d"
    done
    echo "# Sweep complete. CSV at ${RESULTS_DIR}/rle_adv.csv"
  else
    run_one "$DUF"
    echo "# Finished. CSV at ${RESULTS_DIR}/rle_adv.csv"
  fi
}

main "$@"
