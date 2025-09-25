#!/usr/bin/env bash
set -euo pipefail

# Estimate a practical "max-known" throughput for the PostgreSQL index AM API
# by using BTREE with a minimal index-only scan workload.
#
# - Table: UNLOGGED t(b int8, a int8)
# - Index: BTREE(b) INCLUDE (a)
# - Query: SELECT sum(a) WHERE b > -1  (forces IOS to return include attr)
# - Parallel-friendly GUCs to maximize throughput
# - Reports ret_gbps and scan_gbps per worker count and writes the max ret_gbps
#   to results/ios_max.txt for other benches to use as "known max".

if [ "$(id -un)" != "postgres" ]; then
  echo "# ERROR: run as postgres (use make dstart; then make dexec; then run as postgres)." >&2
  exit 2
fi

ROWS=${ROWS:-10000000}
WORKERS=${WORKERS:-0,2,5}
DBNAME=${DBNAME:-postgres}
KILL_AFTER=${KILL_AFTER:-5}
TIMEOUT_SEC=${TIMEOUT_SEC:-120}

PSQL="/usr/local/pgsql/bin/psql -X -v ON_ERROR_STOP=1 -d ${DBNAME} -qAt"
PSQL_TMO="timeout -k ${KILL_AFTER}s ${TIMEOUT_SEC}s ${PSQL}"

# Helper: measure and cache memcpy bandwidth
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
C
  gcc -O3 -march=native -fno-omit-frame-pointer -o /tmp/membw /tmp/membw.c 2>/dev/null || true
  if [ -x /tmp/membw ]; then MEMBW_GBPS=$(/tmp/membw); else MEMBW_GBPS=0; fi
  echo "$MEMBW_GBPS" > results/membw.txt
  export MEMBW_GBPS
}

ensure_membw

${PSQL_TMO} -c "SET client_min_messages=warning; CREATE EXTENSION IF NOT EXISTS smol;" >/dev/null 2>&1 || true
${PSQL_TMO} -c "DROP TABLE IF EXISTS t CASCADE;"
${PSQL_TMO} -c "CREATE UNLOGGED TABLE t(b int8, a int8);"
${PSQL_TMO} -c "ALTER TABLE t SET (autovacuum_enabled = off);"

echo "# loading ${ROWS} rows ..."
${PSQL_TMO} -c "INSERT INTO t SELECT i::int8, (i*7)::int8 FROM generate_series(1, ${ROWS}) i;"
${PSQL_TMO} -c "ANALYZE t;"

${PSQL_TMO} -c "DROP INDEX IF EXISTS t_b_btree;"
${PSQL_TMO} -c "CREATE INDEX t_b_btree ON t USING btree(b) INCLUDE (a);"
${PSQL_TMO} -c "CHECKPOINT; VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) t; ANALYZE t;" >/dev/null 2>&1 || true

echo "Index,Workers,Rows,Exec_ms,Scan_GBps(%memcpy),Ret_GBps(%memcpy)"

IFS=',' read -ra WARR <<<"${WORKERS}"
vals=()
for w in "${WARR[@]}"; do
  ${PSQL_TMO} -c "SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexscan=on; SET enable_indexonlyscan=on; SET max_parallel_workers_per_gather=${w}; SET max_parallel_workers=${w}; SET min_parallel_index_scan_size=0; SET parallel_setup_cost=0; SET parallel_tuple_cost=0;"
  plan=$(${PSQL_TMO} -c "EXPLAIN (ANALYZE, BUFFERS, TIMING ON) SELECT sum(a::bigint) FROM t WHERE b > -1")
  ms=$(echo "$plan" | grep -E 'Execution Time:' | awk '{print $(NF-1)}')
  line=$(echo "$plan" | grep -E "Index Only Scan using |Index Scan using |Seq Scan on" | tail -n1)
  r=$(echo "$line" | sed -E 's/.*rows=([0-9]+).*/\1/')
  loops=$(echo "$line" | sed -E 's/.*loops=([0-9]+).*/\1/')
  if [ -z "$loops" ]; then loops=1; fi
  total=$(( r * loops ))
  # key=8 bytes, include=8 bytes; IOS returns include
  scan_b=$(( total * 16 ))
  ret_b=$(( total * 8 ))
  scan_gbps=$(awk -v b="$scan_b" -v ms="$ms" 'BEGIN{printf "%.3f", (b/1073741824.0)/(ms/1000.0)}')
  ret_gbps=$(awk -v b="$ret_b" -v ms="$ms" 'BEGIN{printf "%.3f", (b/1073741824.0)/(ms/1000.0)}')
  pct_s=$(awk -v g="$scan_gbps" -v m="$MEMBW_GBPS" 'BEGIN{if(m>0) printf "%.1f", 100.0*g/m; else print 0}')
  pct_r=$(awk -v g="$ret_gbps" -v m="$MEMBW_GBPS" 'BEGIN{if(m>0) printf "%.1f", 100.0*g/m; else print 0}')
  echo "btree,${w},${total},${ms},${scan_gbps}(${pct_s}%),${ret_gbps}(${pct_r}%)"
  vals+=("$ret_gbps")
done

# Record max return throughput as the "known max" for IOS ret path
max=0
for v in "${vals[@]:-0}"; do awk -v x="$v" -v m="$max" 'BEGIN{ if (x>m) print x; else print m; }' > /tmp/.mx; max=$(cat /tmp/.mx); done
mkdir -p results
echo "$max" > results/ios_max.txt
echo "# known max ret_gbps: $max (cached at results/ios_max.txt)"
