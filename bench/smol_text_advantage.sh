#!/usr/bin/env bash
set -euo pipefail

# SMOL text key advantage benchmark (short identifiers, <=32B, C collation)
# Compares BTREE vs SMOL for COUNT(*) WHERE b = :const across string lengths.

if [ "$(id -un)" != "postgres" ]; then
  echo "# ERROR: run as postgres user (use: make bench-text-advantage)" >&2
  exit 2
fi

ROWS=${ROWS:-5000000}
WORKERS=${WORKERS:-5}
INC=${INC:-0}
STRLEN=${STRLEN:-16}
SWEEP_STRLEN=${SWEEP_STRLEN:-}
UNIQVALS=${UNIQVALS:-0}
SWEEP_UNIQVALS=${SWEEP_UNIQVALS:-}
DISTRIBUTION=${DISTRIBUTION:-uniform}  # uniform|zipf
TIMEOUT_SEC=${TIMEOUT_SEC:-1800}
KILL_AFTER=${KILL_AFTER:-10}
DB=${DB:-postgres}

PSQL="/usr/local/pgsql/bin/psql -X -v ON_ERROR_STOP=1 -d ${DB} -qAt"
PSQL_TMO="timeout -k ${KILL_AFTER}s ${TIMEOUT_SEC}s ${PSQL}"

export PGOPTIONS="${PGOPTIONS:-} -c client_min_messages=warning -c enable_seqscan=off -c enable_bitmapscan=off -c enable_indexscan=off -c enable_indexonlyscan=on -c max_parallel_workers_per_gather=${WORKERS} -c max_parallel_workers=${WORKERS} -c parallel_setup_cost=0 -c parallel_tuple_cost=0 -c min_parallel_index_scan_size=0 -c min_parallel_table_scan_size=0"

tbl=adv_text
RESULTS_DIR=${RESULTS_DIR:-/workspace/results}
CSV=${CSV:-${RESULTS_DIR}/text_adv.csv}

emit_csv_header(){
  mkdir -p "${RESULTS_DIR}" || true
  if [ ! -s "${CSV}" ]; then echo "rows,workers,inc,strlen,uniqvals,distribution,index,size_bytes,count_ms" > "${CSV}"; fi
}

extract_ms(){ sed -n 's/.*Execution Time: \([0-9.]*\) ms.*/\1/p' | tail -n1; }

make_table(){
  local inc=$1
  ${PSQL_TMO} -c "DROP TABLE IF EXISTS ${tbl} CASCADE;"
  cols="b text COLLATE \"C\""
  if [ "$inc" -gt 0 ]; then
    for ((i=1;i<=inc;i++)); do cols="$cols, a${i} text COLLATE \"C\""; done
  fi
  ${PSQL_TMO} -c "CREATE UNLOGGED TABLE ${tbl}(${cols});"
  ${PSQL_TMO} -c "ALTER TABLE ${tbl} SET (autovacuum_enabled = off);"
}

parse_uniq(){
  local uv="$1"
  if [[ "$uv" == *% ]]; then
    local p=${uv%%%};
    # clamp 0..100
    if (( $(echo "$p < 0.0" | bc -l) )); then p=0.0; fi
    if (( $(echo "$p > 100.0" | bc -l) )); then p=100.0; fi
    # 0.0% -> 1 unique; 100% -> ROWS uniques
    local frac=$(echo "$p / 100.0" | bc -l)
    local val=$(printf '%.0f' "$(echo "$frac * ${ROWS}" | bc -l)")
    if [ "$val" -lt 1 ]; then val=1; fi
    echo "$val"
  else
    # absolute
    local val="$uv"; if [ -z "$val" ]; then val=0; fi
    if [ "$val" -gt "$ROWS" ]; then val=$ROWS; fi
    if [ "$val" -lt 1 ]; then val=1; fi
    echo "$val"
  fi
}

load_data(){
  local len=$1; local uniq=$2
  # Clamp batch size to avoid large executor memory; default varies by scale
  local defB=250000; if [ ${ROWS} -ge 50000000 ]; then defB=50000; fi
  local B=${BATCH:-$defB}
  echo "# Loading strings: len=${len}, uniq=${uniq}, dist=${DISTRIBUTION}"
  local total=$ROWS
  if [ $total -gt 0 ]; then
    local cur=0
    while [ $cur -lt $total ]; do
      local rem=$(( total - cur )); local cnt=$(( rem < B ? rem : B ))
      local val
      if [ "$DISTRIBUTION" = "zipf" ] && [ "$uniq" -gt 0 ]; then
        # deterministic pseudo-random u in [0,1); rank ~ pow(u, 0.2) for heavy head
        val="lpad( floor(${uniq} * pow( (mod(g*48271,2147483647)::float8)/2147483647.0, 0.2))::text, ${len}, '0')"
      elif [ "$uniq" -gt 0 ]; then
        val="lpad(((g-1) % ${uniq})::text, ${len}, '0')"
      else
        val="lpad((g-1)::text, ${len}, '0')"
      fi
      local list="$val"; if [ "$INC" -gt 0 ]; then for ((i=1;i<=INC;i++)); do list="$list, ${val}"; done; fi
      ${PSQL_TMO} -c "INSERT INTO ${tbl} SELECT ${list} FROM generate_series(1, ${cnt}) g;"
      cur=$(( cur + cnt ))
    done
  fi
  ${PSQL_TMO} -c "ANALYZE ${tbl};" >/dev/null 2>&1 || true
}

build_bt(){ ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${tbl}_bt;"; ${PSQL_TMO} -c "CREATE INDEX ${tbl}_bt ON ${tbl} USING btree(b COLLATE \"C\")"; }
build_sm(){ ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${tbl}_sm;"; ${PSQL_TMO} -c "SET smol.rle=on; CREATE INDEX ${tbl}_sm ON ${tbl} USING smol(b COLLATE \"C\")"; }

run_len(){
  local len=$1; local uniq=$2
  make_table "$INC"
  load_data "$len" "$uniq"
  echo "# Rows in table:"; ${PSQL_TMO} -c "SELECT count(*) FROM ${tbl};"
  build_bt
  local bt_size=$(${PSQL_TMO} -c "SELECT pg_relation_size('${tbl}_bt');")
  # pick the head value (rank 0) to stress duplicates
  local qval="lpad('0',${len},'0')"
  local count_q="SELECT count(*) FROM ${tbl} WHERE b = ${qval};"
  local bt_ms=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${count_q}" | extract_ms)
  build_sm
  local sm_size=$(${PSQL_TMO} -c "SELECT pg_relation_size('${tbl}_sm');")
  local sm_ms=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${count_q}" | extract_ms)
  emit_csv_header
  echo "${ROWS},${WORKERS},${INC},${len},${uniq},${DISTRIBUTION},btree,${bt_size},${bt_ms}" >> "${CSV}"
  echo "${ROWS},${WORKERS},${INC},${len},${uniq},${DISTRIBUTION},smol,${sm_size},${sm_ms}" >> "${CSV}"
}

main(){
  # Ensure latest SQL objects are present (opclasses). Safe if no deps yet.
  ${PSQL_TMO} -c "DROP EXTENSION IF EXISTS smol CASCADE; CREATE EXTENSION smol;" >/dev/null 2>&1 || true
  if [ -n "${SWEEP_STRLEN}" ]; then IFS=',' read -ra LENS <<< "${SWEEP_STRLEN}"; else LENS=("${STRLEN}"); fi
  if [ -n "${SWEEP_UNIQVALS}" ]; then IFS=',' read -ra UVS <<< "${SWEEP_UNIQVALS}"; else UVS=("${UNIQVALS}"); fi
  for u in "${UVS[@]}"; do uniq=$(parse_uniq "$u"); for l in "${LENS[@]}"; do run_len "$l" "$uniq"; done; done
  echo "# Done. CSV at ${CSV}"
}

main "$@"
