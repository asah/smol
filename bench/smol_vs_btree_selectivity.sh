#!/usr/bin/env bash
set -euo pipefail

# Must run as the postgres OS user to connect with psql without role errors.
# When invoked via Makefile, we already wrap with `su - postgres -c`.
if [ "$(id -un)" != "postgres" ]; then
  echo "# ERROR: please run this script as the 'postgres' user." >&2
  echo "# Hint: use 'make bench-smol-vs-btree' or 'make bench-smol-btree-5m'," >&2
  echo "#       or run: su - postgres -c \"env ROWS=... COLS=... bash $0\"" >&2
  exit 2
fi

# SMOL vs BTREE benchmark with selectivity-driven data generation and INCLUDE support.
# Env params:
#   ROWS (default 5000000)
#   COLS (default 2) -> 1 = key-only (b); >1 = b with INCLUDE a1..a{COLS-1}
#   COLTYPE (int2|int4|int8; default int2)
#   SELECTIVITY (comma list of s in [0..1]; default 0.5)
#   WORKERS (comma list; default 5)
#   TIMEOUT_SEC (<=30; default 30), KILL_AFTER (<=5; default 5)
#   DBNAME (default postgres), BATCH (default 250000), CHUNK_MIN (default 50000)

ROWS=${ROWS:-10000000}
COLS=${COLS:-2}
COLTYPE=${COLTYPE:-int2}
SELECTIVITY=${SELECTIVITY:-0.5}
WORKERS=${WORKERS:-5}
TIMEOUT_SEC=${TIMEOUT_SEC:-30}
KILL_AFTER=${KILL_AFTER:-5}
DBNAME=${DBNAME:-postgres}
BATCH=${BATCH:-250000}
CHUNK_MIN=${CHUNK_MIN:-50000}
COMPACT=${COMPACT:-0}
# Fast reuse mode: when set (REUSE=1), skip schema drop/recreate and reuse
# the same table by TRUNCATE; keep BTREE persistent across runs and only
# drop/recreate SMOL per selectivity to minimize overhead.
REUSE=${REUSE:-0}

# Inherit per-session options via PGOPTIONS for all psql sessions
PGOPTIONS="${PGOPTIONS:-} -c client_min_messages=warning"
if [ "${SMOL_PROFILE:-0}" = "1" ]; then PGOPTIONS="${PGOPTIONS} -c smol.profile=on"; fi
export PGOPTIONS
PSQL="/usr/local/pgsql/bin/psql -X -v ON_ERROR_STOP=1 -d ${DBNAME} -qAt"
PSQL_TMO="timeout -k ${KILL_AFTER}s ${TIMEOUT_SEC}s ${PSQL}"
# Quiet NOTICEs for ad-hoc statements
${PSQL_TMO} -c "SET client_min_messages=warning;" >/dev/null 2>&1 || true
# Optional: enable SMOL per-scan microprofile logging when SMOL_PROFILE=1
if [ "${SMOL_PROFILE:-0}" = "1" ]; then
  ${PSQL_TMO} -c "SET smol.profile = on;" >/dev/null 2>&1 || true
fi
if [ -n "${SMOL_CLAIM:-}" ]; then
  ${PSQL_TMO} -c "SET smol.parallel_claim_batch = ${SMOL_CLAIM};" >/dev/null 2>&1 || true
fi

case "$COLTYPE" in
  int2) RMAX=${RMAX:-32767};;
  int4) RMAX=${RMAX:-1000000000};;
  int8) RMAX=${RMAX:-9000000000000000000};;
  *) echo "# ERROR: unsupported COLTYPE=$COLTYPE" >&2; exit 2;;
esac

# Deterministic table naming to encourage reuse and avoid clutter.
# Format: bm_${COLTYPE}_c${COLS}_r{abbr} where abbr is 10m/500k/1b, etc.
abbr_rows() {
  local n=$1
  if [ "$n" -ge 1000000000 ]; then echo "$(( n/1000000000 ))b"; return; fi
  if [ "$n" -ge 1000000 ]; then echo "$(( n/1000000 ))m"; return; fi
  if [ "$n" -ge 1000 ]; then echo "$(( n/1000 ))k"; return; fi
  echo "$n"
}
if [ -z "${TBL:-}" ]; then
  base_name="bm_${COLTYPE}_c${COLS}_r$(abbr_rows "$ROWS")"
  # PostgreSQL limits identifiers to 63 bytes; truncate if necessary.
  TBL=${base_name:0:63}
fi

# Optional: skip regeneration when table already populated (REGEN=0 default).
REGEN=${REGEN:-0}

# (No index validity toggling; we observe planner choices and report them.)

prepare_table() {
  if [ "$REUSE" = "1" ]; then
    # Compute column list in bash, then create table if not exists
    if [ "$COLS" -le 1 ]; then
      cols_sql="b ${COLTYPE}"
    else
      cols_sql="b ${COLTYPE}"
      for ((i=1;i<${COLS};i++)); do cols_sql="$cols_sql, a${i} ${COLTYPE}"; done
    fi
    ${PSQL_TMO} -c "SET client_min_messages=warning; CREATE EXTENSION IF NOT EXISTS smol;"
    if [ "${SMOL_PROFILE:-0}" = "1" ]; then ${PSQL_TMO} -c "SET smol.profile=on;" >/dev/null 2>&1 || true; fi
    if [ -n "${SMOL_CLAIM:-}" ]; then ${PSQL_TMO} -c "SET smol.parallel_claim_batch=${SMOL_CLAIM};" >/dev/null 2>&1 || true; fi
    ${PSQL_TMO} -c "CREATE UNLOGGED TABLE IF NOT EXISTS ${TBL}(${cols_sql});"
    ${PSQL_TMO} -c "ALTER TABLE ${TBL} SET (autovacuum_enabled = off);"
    # Optional: apply SMOL planner GUCs if provided
    if [ -n "${SMOL_COST_PAGE:-}" ]; then ${PSQL_TMO} -c "SET smol.cost_page=${SMOL_COST_PAGE};"; fi
    if [ -n "${SMOL_COST_TUP:-}" ]; then ${PSQL_TMO} -c "SET smol.cost_tup=${SMOL_COST_TUP};"; fi
    if [ -n "${SMOL_SELEC_EQ:-}" ]; then ${PSQL_TMO} -c "SET smol.selec_eq=${SMOL_SELEC_EQ};"; fi
    if [ -n "${SMOL_SELEC_RANGE:-}" ]; then ${PSQL_TMO} -c "SET smol.selec_range=${SMOL_SELEC_RANGE};"; fi
  else
    # Apply optional SMOL claim batch (smol.profile comes via PGOPTIONS)
    if [ -n "${SMOL_CLAIM:-}" ]; then ${PSQL_TMO} -c "SET smol.parallel_claim_batch=${SMOL_CLAIM};" >/dev/null 2>&1 || true; fi
    ${PSQL_TMO} -c "SET client_min_messages = warning; CREATE EXTENSION IF NOT EXISTS smol;"
    ${PSQL_TMO} -c "DROP TABLE IF EXISTS ${TBL} CASCADE;"
    if [ "$COLS" -le 1 ]; then
      ${PSQL_TMO} -c "CREATE UNLOGGED TABLE ${TBL}(b ${COLTYPE});"
    else
      cols="b ${COLTYPE}";
      for ((i=1;i<${COLS};i++)); do cols="$cols, a${i} ${COLTYPE}"; done
      ${PSQL_TMO} -c "CREATE UNLOGGED TABLE ${TBL}(${cols});"
    fi
    ${PSQL_TMO} -c "ALTER TABLE ${TBL} SET (autovacuum_enabled = off);"
  fi
}

# Measure and cache memory bandwidth (GB/s) using a tiny C microbenchmark.
ensure_membw() {
  if [ -n "${MEMBW_GBPS:-}" ]; then return; fi
  mkdir -p results || true
  if [ -f results/membw.txt ]; then MEMBW_GBPS=$(cat results/membw.txt); export MEMBW_GBPS; return; fi
  cat > /tmp/membw.c <<'C'
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
static double now(){ struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec + ts.tv_nsec*1e-9; }
int main(){
  size_t sz = (size_t)256*1024*1024; /* 256 MiB */
  char *a = aligned_alloc(64, sz), *b = aligned_alloc(64, sz);
  if(!a||!b){ fprintf(stderr, "alloc failed\n"); return 1; }
  memset(a, 0xAA, sz); memset(b, 0x55, sz);
  double t0 = now();
  size_t it=0; double dur=0; while (dur < 0.25) { memcpy(b, a, sz); it++; dur = now()-t0; }
  double bytes = (double)sz * (double)it;
  double gbps = (bytes / dur) / 1073741824.0;
  printf("%.3f\n", gbps);
  return 0;
}

# Read or measure practical IOS max return throughput (GB/s) baseline.
# Prefer cached results/ios_max.txt; otherwise run a quick baseline.
ensure_iosmax() {
  if [ -n "${KNOWN_MAX_GBPS:-}" ]; then return; fi
  if [ -f results/ios_max.txt ]; then
    KNOWN_MAX_GBPS=$(cat results/ios_max.txt)
    export KNOWN_MAX_GBPS
    return
  fi
  # Fallback: run a small baseline to estimate max-known ret_gbps
  if [ -x bench/bench_ios_max.sh ]; then
    su - postgres -c "bash /workspace/bench/bench_ios_max.sh ROWS=${IOSMAX_ROWS:-10000000} WORKERS=${IOSMAX_WORKERS:-0,2,5}" >/dev/null 2>&1 || true
    if [ -f results/ios_max.txt ]; then KNOWN_MAX_GBPS=$(cat results/ios_max.txt); else KNOWN_MAX_GBPS=0; fi
  else
    KNOWN_MAX_GBPS=0
  fi
  export KNOWN_MAX_GBPS
}
C
  gcc -O3 -march=native -fno-omit-frame-pointer -o /tmp/membw /tmp/membw.c 2>/dev/null || true
  if [ -x /tmp/membw ]; then MEMBW_GBPS=$(/tmp/membw); else MEMBW_GBPS=0; fi
  echo "$MEMBW_GBPS" > results/membw.txt
  export MEMBW_GBPS
}

load_selective() {
  local s=$1
  local M=0
  # ~25% domain window around M for dispersion
  local W
  case "$COLTYPE" in
    int2) W=16000;;
    int4) W=250000000;;
    int8) W=1000000000000000000;;
  esac
  local total=${ROWS}
  local exact=1
  local match=$(( $(awk -v s="$s" -v n="$total" 'BEGIN{printf "%d", s*n}') ))
  if [ "$match" -lt 0 ]; then match=0; fi
  local match_rand=$(( match > exact ? match - exact : 0 ))
  local nonmatch=$(( total - exact - match_rand ))
  insert_range() {
    local nrows=$1; local base=$2; local span=$3
    if [ "$nrows" -le 0 ]; then return; fi
    local cur=1
    while [ "$cur" -le "$nrows" ]; do
      local end=$(( cur + BATCH - 1 )); if [ $end -gt $nrows ]; then end=$nrows; fi
      local cnt=$(( end - cur + 1 ))
      if [ "$COLS" -le 1 ]; then
        ${PSQL_TMO} -c "INSERT INTO ${TBL} SELECT (${base} + floor(random()*${span}))::${COLTYPE} FROM generate_series(1, ${cnt});";
      else
        # INCLUDE columns: fill a1.. with random
        local selb="(${base} + floor(random()*${span}))::${COLTYPE}"
        local sella="(random()*${RMAX})::${COLTYPE}"
        local list="${selb}"; for ((i=1;i<${COLS};i++)); do list="$list, ${sella}"; done
        ${PSQL_TMO} -c "INSERT INTO ${TBL} SELECT ${list} FROM generate_series(1, ${cnt});";
      fi
      cur=$(( end + 1 ))
    done
  }
  # If reusing, populate only when requested or when empty.
  if [ "$REUSE" = "1" ] && [ "$REGEN" = "0" ]; then
    # Use catalog estimate to avoid scanning large tables
    have_rows=$(${PSQL_TMO} -c "SELECT CASE WHEN to_regclass('${TBL}') IS NULL THEN 0 ELSE (SELECT (reltuples>0)::int FROM pg_class WHERE oid='${TBL}'::regclass) END")
    if [ "${have_rows}" = "1" ]; then
      # Already populated; just ANALYZE and return (works for any selectivity M)
      ${PSQL_TMO} -c "ANALYZE ${TBL};"
      return
    fi
  fi
  # Otherwise, (re)populate table
  if [ "$REUSE" = "1" ]; then ${PSQL_TMO} -c "TRUNCATE ${TBL};"; fi
  # non-matching below M: [M-W, M)
  insert_range "$nonmatch" "$(( M - W ))" "$W"
  # matching above M: (M, M+W]
  insert_range "$match_rand" "$(( M + 1 ))" "$W"
  # exact match at M
  if [ "$COLS" -le 1 ]; then
    ${PSQL_TMO} -c "INSERT INTO ${TBL} VALUES ((${M})::${COLTYPE});";
  else
    # Avoid noisy syntax error by directly inserting VALUES (list holds value expressions, not column names)
    local list="(${M})::${COLTYPE}"; for ((i=1;i<${COLS};i++)); do list="$list, 0::${COLTYPE}"; done
    ${PSQL_TMO} -c "INSERT INTO ${TBL} VALUES (${list});"
  fi
  ${PSQL_TMO} -c "ANALYZE ${TBL};"
}

run_phase() {
  local idxname=$1; local create_stmt=$2; local is_smol=$3
  local t0=$(date +%s%3N)
  local stmt_ms=$(( (TIMEOUT_SEC>2 ? TIMEOUT_SEC-2 : TIMEOUT_SEC) * 1000 ))
  if [ "$REUSE" = "1" ] && [ "$is_smol" -eq 0 ]; then
    # Ensure BTREE index exists; create if missing via IF NOT EXISTS
    local create_stmt_if="CREATE INDEX IF NOT EXISTS${create_stmt#CREATE INDEX}"
    ${PSQL_TMO} -c "SET statement_timeout=${stmt_ms}; ${create_stmt_if}" || true
  else
    # For SMOL (or non-reuse mode), (re)create index
    if [ "$REUSE" = "1" ] && [ "$is_smol" -eq 1 ]; then
      ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${idxname};"
    fi
    ${PSQL_TMO} -c "SET statement_timeout=${stmt_ms}; ${create_stmt}" || true
  fi
  if [ "$is_smol" -eq 0 ]; then
    ${PSQL_TMO} -c "CHECKPOINT";
    if [ "${SKIP_VAC:-0}" != "1" ]; then
      ${PSQL_TMO} -c "VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) ${TBL}";
    fi
    ${PSQL_TMO} -c "ANALYZE ${TBL}";
  fi
  local t1=$(date +%s%3N)
  local build_ms=$((t1 - t0))
  local size_mb=$(${PSQL_TMO} -c "SELECT round(pg_relation_size('${idxname}')/1024.0/1024.0,2)")
  echo "${idxname}|${is_smol}|${build_ms}|${size_mb}"
}

run_workers() {
  local idx_type=$1; local s=$2; local w=$3
  local M=0
  ${PSQL_TMO} -c "SET max_parallel_workers_per_gather=${w}; SET max_parallel_workers=${w}; \
                  SET parallel_setup_cost=0; SET parallel_tuple_cost=0; \
                  SET min_parallel_index_scan_size=0; SET min_parallel_table_scan_size=0;"
  if [ "${FORCE_PARALLEL:-0}" = "1" ]; then
    ${PSQL_TMO} -c "SET force_parallel_mode=on;" >/dev/null 2>&1 || true
  fi
  local q pline exec_ms
  if [ "$COLS" -le 1 ]; then q="SELECT count(*) FROM ${TBL} WHERE b > $((M-1));"; else q="SELECT sum(a1::bigint) FROM (SELECT a1 FROM ${TBL} WHERE b > $((M-1))) s"; fi
  # Always gather BUFFERS to extract Heap Fetches and rows
  plan_all=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) ${q}")
  exec_ms=$(echo "$plan_all" | grep -E 'Execution Time:' | awk '{print $(NF-1)}')
  # Extract chosen scan and index (if any); prefer index scans when present
  local chosen scan_type workers
  chosen=$(echo "$plan_all" | grep -Em1 "Index Only Scan using |Index Scan using |Bitmap Index Scan on |Bitmap Heap Scan on |Seq Scan on " | sed 's/^\s\+//')
  if [ -z "$chosen" ]; then
    chosen=$(echo "$plan_all" | head -n1 | sed 's/^\s\+//')
  fi
  scan_type=$(echo "$chosen" | sed -E 's/\s+using.*$//' | sed -E 's/\s+on.*$//')
  workers=$(echo "$plan_all" | grep -E "Workers Launched:" | awk '{print $3}' | tr -d ' ')
  if [ -z "$workers" ]; then workers=0; fi
  # Rows (per-scan-line) and loops (workers); compute total rows = rows * loops
  local rows loops heapf
  rows=$(echo "$plan_all" | grep -Em1 "Index Only Scan|Index Scan|Bitmap Heap Scan|Seq Scan" | sed -E 's/.*rows=([0-9]+).*/\1/')
  loops=$(echo "$plan_all" | grep -Em1 "Index Only Scan|Index Scan|Bitmap Heap Scan|Seq Scan" | sed -E 's/.*loops=([0-9]+).*/\1/')
  if [ -z "$loops" ]; then loops=1; fi
  heapf=$(echo "$plan_all" | grep -Em1 "Heap Fetches:" | awk '{print $3}')
  if [ -z "$heapf" ]; then heapf=0; fi
  local total_rows=$(( rows * loops ))
  # Compute per-row byte sizes
  local key_len=2; case "$COLTYPE" in int2) key_len=2;; int4) key_len=4;; int8) key_len=8;; esac
  local inc_len=0; if [ "$COLS" -gt 1 ]; then inc_len=$key_len; fi
  local touched_bytes=$(( total_rows * (key_len + inc_len) ))
  local ret_bytes=0; if [ "$COLS" -gt 1 ]; then ret_bytes=$(( total_rows * inc_len )); fi
  # Memory bandwidth (GB/s) measured/cached
  ensure_membw
  ensure_iosmax
  local scan_gbps ret_gbps pct_scan pct_ret
  if [ "$exec_ms" = "0.000" ] || [ -z "$exec_ms" ]; then
    scan_gbps=0; ret_gbps=0; pct_scan=0; pct_ret=0
  else
    scan_gbps=$(awk -v b="$touched_bytes" -v ms="$exec_ms" 'BEGIN{printf "%.3f", (b/1073741824.0)/(ms/1000.0)}')
    ret_gbps=$(awk -v b="$ret_bytes" -v ms="$exec_ms" 'BEGIN{printf "%.3f", (b/1073741824.0)/(ms/1000.0)}')
    pct_scan=$(awk -v g="$scan_gbps" -v m="$MEMBW_GBPS" 'BEGIN{if(m>0) printf "%.1f", 100.0*g/m; else print 0}')
    pct_ret=$(awk -v g="$ret_gbps" -v m="$MEMBW_GBPS" 'BEGIN{if(m>0) printf "%.1f", 100.0*g/m; else print 0}')
  fi
  # Percentage of practical max-known IOS ret throughput
  local pct_ret_known
  pct_ret_known=$(awk -v g="$ret_gbps" -v m="$KNOWN_MAX_GBPS" 'BEGIN{if(m>0) printf "%.1f", 100.0*g/m; else print 0}')
  # Emit: time|plan|rows|heapf|bw
  echo "${exec_ms}|${chosen}, ${workers} workers|rows=${rows} loops=${loops} total_rows=${total_rows}|heap_fetches=${heapf}|scan_gbps=${scan_gbps} (${pct_scan}% memcpy)|ret_gbps=${ret_gbps} (${pct_ret}% memcpy, ${pct_ret_known}% ios_max)"
}

echo "Index,Type,Build_ms,Size_MB,Query_ms,Plan"

prepare_table
IFS=',' read -ra SARR <<<"${SELECTIVITY}"
IFS=',' read -ra WARR <<<"${WORKERS}"

# Validate selectivity values are within [0.0 .. 1.0]
validate_selectivities() {
  for s in "${SARR[@]}"; do
    if [[ ! "$s" =~ ^(0(\.[0-9]+)?|1(\.0+)?)$ ]]; then
      echo "# ERROR: SELECTIVITY contains out-of-bounds or invalid value: '$s' (expected 0.0..1.0)" >&2
      exit 2
    fi
  done
}
validate_selectivities
IFS=',' read -ra WARR <<<"${WORKERS}"

# Build INCLUDE list once when needed
if [ "$COLS" -gt 1 ]; then
  inc_list="a1"; for ((i=2;i<${COLS};i++)); do inc_list="$inc_list, a${i}"; done
fi

# Phase 1: run all BTREE tests across selectivities; keep BTREE persistent in REUSE mode
for s in "${SARR[@]}"; do
  load_selective "$s"
  if [ "$COLS" -le 1 ]; then
    ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_b_smol;" >/dev/null 2>&1 || true
    IFS='|' read -r _ _ build_ms size_mb <<<"$(run_phase ${TBL}_b_btree "CREATE INDEX ${TBL}_b_btree ON ${TBL}(b);" 0)"
    for w in "${WARR[@]}"; do
      IFS='|' read -r exec_ms planok <<<"$(run_workers btree "$s" "$w")"
      echo "btree,btree,${build_ms},${size_mb},${exec_ms},${planok}"
    done
    if [ "$REUSE" != "1" ]; then ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_b_btree;"; fi
  else
    ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_b_smol;" >/dev/null 2>&1 || true
    IFS='|' read -r _ _ build_ms size_mb <<<"$(run_phase ${TBL}_b_btree "CREATE INDEX ${TBL}_b_btree ON ${TBL} USING btree(b) INCLUDE (${inc_list});" 0)"
    base_sum=$(${PSQL_TMO} -c "SELECT sum(a1::bigint) FROM ${TBL} WHERE b > -1"); base_cnt=$(${PSQL_TMO} -c "SELECT count(*) FROM ${TBL} WHERE b > -1")
    for w in "${WARR[@]}"; do
      IFS='|' read -r exec_ms planok <<<"$(run_workers btree "$s" "$w")"
      echo "btree,btree,${build_ms},${size_mb},${exec_ms},${planok}"
    done
    if [ "$REUSE" != "1" ]; then ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_b_btree;"; fi
  fi
done

# Phase 2: run all SMOL tests across selectivities; drop/recreate SMOL each time (read-only index)
for s in "${SARR[@]}"; do
  load_selective "$s"
  if [ "$COLS" -le 1 ]; then
    ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_b_btree;" >/dev/null 2>&1 || true
    IFS='|' read -r _ _ build_ms size_mb <<<"$(run_phase ${TBL}_b_smol "CREATE INDEX ${TBL}_b_smol ON ${TBL} USING smol(b);" 1)"
    for w in "${WARR[@]}"; do
      IFS='|' read -r exec_ms planok <<<"$(run_workers smol "$s" "$w")"
      echo "smol,smol,${build_ms},${size_mb},${exec_ms},${planok}"
    done
  else
    ${PSQL_TMO} -c "DROP INDEX IF EXISTS ${TBL}_b_btree;" >/dev/null 2>&1 || true
    IFS='|' read -r _ _ build_ms size_mb <<<"$(run_phase ${TBL}_b_smol "CREATE INDEX ${TBL}_b_smol ON ${TBL} USING smol(b) INCLUDE (${inc_list});" 1)"
    base_sum=$(${PSQL_TMO} -c "SELECT sum(a1::bigint) FROM ${TBL} WHERE b > -1"); base_cnt=$(${PSQL_TMO} -c "SELECT count(*) FROM ${TBL} WHERE b > -1"); sm_sum=$(${PSQL_TMO} -c "SELECT sum(a1::bigint) FROM ${TBL} WHERE b > -1"); sm_cnt=$(${PSQL_TMO} -c "SELECT count(*) FROM ${TBL} WHERE b > -1")
    echo "Correct,$([ \"${base_sum}\" = \"${sm_sum}\" ] && [ \"${base_cnt}\" = \"${sm_cnt}\" ] && echo true || echo false),sum_bt=${base_sum},sum_smol=${sm_sum},cnt_bt=${base_cnt},cnt_smol=${sm_cnt}"
    for w in "${WARR[@]}"; do
      IFS='|' read -r exec_ms planok <<<"$(run_workers smol "$s" "$w")"
      echo "smol,smol,${build_ms},${size_mb},${exec_ms},${planok}"
    done
  fi
done
