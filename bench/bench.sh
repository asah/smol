#!/usr/bin/env bash
set -euo pipefail

# SMOL benchmarking harness (quick/full).
# - Runs inside the smol Docker container (preferred).
# - Uses psql as the postgres user; collects metrics via EXPLAIN JSON.
# - Writes CSV and a minimal Markdown report to results/.

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS_DIR="$ROOT/results"
PSQL="/usr/local/pgsql/bin/psql -v ON_ERROR_STOP=1 -X -q -t -A -d postgres"
HAVE_JQ=0
if command -v jq >/dev/null 2>&1; then HAVE_JQ=1; fi

MODE="quick"
REPEATS=3
WARM_ONLY=1

usage() {
  echo "Usage: $0 [--quick|--full] [--repeats N] [--cold]";
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick) MODE=quick; ;;
    --full) MODE=full; ;;
    --repeats) REPEATS=${2:-7}; shift ;;
    --cold) WARM_ONLY=0 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
  shift
done

mkdir -p "$RESULTS_DIR"
TS="$(date +%Y%m%d-%H%M%S)"
CSV_OUT="$RESULTS_DIR/${MODE}-${TS}.csv"
MD_OUT="$RESULTS_DIR/${MODE}-${TS}.md"
CASES_OUT="$RESULTS_DIR/${MODE}-${TS}.cases.sql"

as_pg() {
  # Run command as postgres user under bash for process substitution compatibility
  su - postgres -c "bash -lc '$*'"
}

psql_q() {
  local sql="$1"
  local tf
  tf=$(mktemp /tmp/benchsql.XXXXXX)
  printf "\\set QUIET on\nSET client_min_messages=error;\n%s\n" "$sql" > "$tf"
  chmod 644 "$tf"
  # Filter harmless NOTICE lines from output
  as_pg "$PSQL -f $tf 2>&1 | sed '/NOTICE:/d'"
  rm -f "$tf"
}

psql_exec() {
  local sql="$1"
  local tf
  tf=$(mktemp /tmp/benchsql.XXXXXX)
  printf "SET client_min_messages=error;\n%s\n" "$sql" > "$tf"
  chmod 644 "$tf"
  as_pg "$PSQL -f $tf >/dev/null 2>&1"
  rm -f "$tf"
}

ensure_extension() {
  psql_exec "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='smol') THEN CREATE EXTENSION smol; END IF; END \$\$;"
}

restart_pg() {
  # Cold-cache reset: restart PG and drop OS caches if permitted
  if as_pg "/usr/local/pgsql/bin/pg_ctl -D /var/lib/postgresql/data -m fast -w restart"; then
    :
  fi
  if [[ -w /proc/sys/vm/drop_caches ]]; then
    sync; echo 3 > /proc/sys/vm/drop_caches || true
  fi
}

table_prep_common() {
  local tbl="$1"
  psql_exec "ALTER TABLE ${tbl} SET (autovacuum_enabled = off)" || true
  psql_exec "ANALYZE ${tbl}"
  # Freeze for stable visibility
  psql_exec "CHECKPOINT"
  psql_exec "SET vacuum_freeze_min_age=0"
  psql_exec "SET vacuum_freeze_table_age=0"
  psql_exec "VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) ${tbl}"
}

btree_build() {
  local idx="$1" tbl="$2" spec="$3"
  local t0 t1
  t0=$(date +%s%3N)
  psql_exec "DROP INDEX IF EXISTS ${idx}; CREATE INDEX ${idx} ON ${tbl} USING btree ${spec};"
  t1=$(date +%s%3N)
  local ms=$((t1 - t0))
  local sz=$(psql_q "SELECT pg_relation_size('${idx}')")
  echo "$ms" "$sz"
}

smol_build() {
  local idx="$1" tbl="$2" spec="$3"
  local t0 t1
  t0=$(date +%s%3N)
  psql_exec "DROP INDEX IF EXISTS ${idx}; CREATE INDEX ${idx} ON ${tbl} USING smol ${spec};"
  t1=$(date +%s%3N)
  local ms=$((t1 - t0))
  local sz=$(psql_q "SELECT pg_relation_size('${idx}')")
  echo "$ms" "$sz"
}

force_ios_gucs() {
  psql_q "SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexscan=off; SET enable_indexonlyscan=on;"
}

encourage_parallel_gucs() {
  local workers="$1"
  psql_q "SET max_parallel_workers_per_gather=${workers}; SET max_parallel_workers=${workers}; SET parallel_setup_cost=0; SET parallel_tuple_cost=0; SET min_parallel_index_scan_size=0; SET min_parallel_table_scan_size=0;"
}

parse_explain() {
  # stdin: EXPLAIN output; stdout: plan_ms exec_ms rows_out node_type index_name relname workers_planned workers_launched
  if [[ $HAVE_JQ -eq 1 ]]; then
    jq -r '
      def scans(p): [ p | .. | objects | select(((."Node Type") // "") | test("Index Only Scan|Index Scan|Seq Scan|Bitmap Heap Scan|Bitmap Index Scan")) ];
      .[0] as $r | (scans($r.Plan) | .[0]) as $s |
      [ $r["Planning Time"], $r["Execution Time"], ($s["Actual Rows"] // 0), ($s["Node Type"] // ""), ($s["Index Name"] // ""), ($s["Relation Name"] // ""), ($r.Plan["Workers Planned"] // 0), ($r.Plan["Workers Launched"] // 0) ] | @tsv'
  else
    awk '
      /Planning Time:/ {plan=$3}
      /Execution Time:/ {exec=$3}
      /Index Only Scan using/ {node="Index Only Scan"; idx=""; rel=""; for(i=1;i<=NF;i++){ if($i=="using") idx=$(i+1); if($i=="on") { rel=$(i+1); break } } }
      /Index Scan using/ {node="Index Scan"; idx=""; rel=""; for(i=1;i<=NF;i++){ if($i=="using") idx=$(i+1); if($i=="on") { rel=$(i+1); break } } }
      /Seq Scan on/ {node="Seq Scan"; for(i=1;i<=NF;i++){ if($i=="on") { rel=$(i+1); break } } }
      /Bitmap Heap Scan on/ {node="Bitmap Heap Scan"; for(i=1;i<=NF;i++){ if($i=="on") { rel=$(i+1); break } } }
      /rows=[0-9]+/ { if (rows=="") { if (match($0, /rows=[0-9]+/)) rows=substr($0,RSTART+5,RLENGTH-5) } }
      /Workers Planned:/ {wp=$3}
      /Workers Launched:/ {wl=$3}
      END { if (plan=="") plan=0; if (exec=="") exec=0; if (rows=="") rows=0; if (wp=="") wp=0; if (wl=="") wl=0; if (node=="") node=""; if (idx=="") idx=""; if (rel=="") rel=""; printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", plan, exec, rows, node, idx, rel, wp, wl }
    '
  fi
}

run_explain() {
  local sql="$1"
  local tf
  tf=$(mktemp /tmp/benchsql.XXXXXX)
  if [[ $HAVE_JQ -eq 1 ]]; then
    printf "\\set QUIET on\nSET client_min_messages=error;\nEXPLAIN (ANALYZE, TIMING ON, BUFFERS OFF, FORMAT JSON) %s;\n" "$sql" > "$tf"
  else
    printf "\\set QUIET on\nSET client_min_messages=error;\nEXPLAIN (ANALYZE, TIMING ON, BUFFERS OFF) %s;\n" "$sql" > "$tf"
  fi
  chmod 644 "$tf"
  as_pg "$PSQL -f $tf"
  rm -f "$tf"
}

# Compute statistics without GNU awk extensions
median() {
  # reads numbers on stdin; prints median
  sort -n | awk '{a[NR]=$1} END { if (NR==0) {print 0; exit} if (NR%2){print a[(NR+1)/2]} else {printf "%.6f\n", (a[NR/2]+a[NR/2+1])/2} }'
}
p95() {
  sort -n | awk '{a[NR]=$1} END { if (NR==0){print 0; exit} idx=int(0.95*NR); if (idx<1) idx=1; if (idx>NR) idx=NR; print a[idx] }'
}

measure_query() {
  local sql="$1" repeats="$2" warm="$3" workers="$4"
  force_ios_gucs
  encourage_parallel_gucs "$workers"
  # warm-up run if requested
  if [[ "$warm" -eq 1 ]]; then
    run_explain "$sql" >/dev/null 2>&1 || true
  fi
  local tmp_pl=/tmp/pl.$$
  local tmp_ex=/tmp/ex.$$
  : > "$tmp_pl"; : > "$tmp_ex"
  local rows_out=0 node="" idx="" rel="" wp=0 wl=0
  for ((i=1;i<=repeats;i++)); do
    local out
    out=$(run_explain "$sql" | parse_explain)
    local pl ex rows n ixd rn wpp wll
    pl="$(echo "$out" | cut -f1)"; ex="$(echo "$out" | cut -f2)"; rows="$(echo "$out" | cut -f3)"; n="$(echo "$out" | cut -f4)"; ixd="$(echo "$out" | cut -f5)"; rn="$(echo "$out" | cut -f6)"; wpp="$(echo "$out" | cut -f7)"; wll="$(echo "$out" | cut -f8)"
    echo "$pl" >> "$tmp_pl"; echo "$ex" >> "$tmp_ex"; rows_out="$rows"; node="$n"; idx="$ixd"; rel="$rn"; wp="$wpp"; wl="$wll"
  done
  local pl_med ex_med ex_p95
  pl_med=$(cat "$tmp_pl" | median)
  ex_med=$(cat "$tmp_ex" | median)
  ex_p95=$(cat "$tmp_ex" | p95)
  rm -f "$tmp_pl" "$tmp_ex"
  echo "$pl_med" "$ex_med" "$ex_p95" "$rows_out" "$node" "$idx" "$rel" "$wp" "$wl"
}

csv_header() {
  echo "case_id,engine,key_type,cols,includes,duplicates,rows,selectivity,direction,workers_req,warm,build_ms,idx_size_bytes,plan_ms_med,exec_ms_med,exec_ms_p95,rows_out,node_type,index_name,relname,workers_planned,workers_launched,timestamp" > "$CSV_OUT"
}

csv_row() {
  local IFS=","; echo "$*" >> "$CSV_OUT"
}

# ---------------- Quick suite -----------------

bench_int4_unique_range() {
  local case_id="INT4_RANGE_UNIQUE_1M"
  local rows=1000000
  local tbl="q1_t"; local q="SELECT count(*) FROM ${tbl} WHERE a >= ${rows}/2"; psql_exec "DROP TABLE IF EXISTS ${tbl} CASCADE; CREATE UNLOGGED TABLE ${tbl}(a int4); INSERT INTO ${tbl} SELECT i FROM generate_series(1, ${rows}) i;"; printf "%s\n%s;\n" "-- $case_id" "$q" >> "$CASES_OUT"
  table_prep_common "$tbl"
  # BTREE
  read -r bt_build_ms bt_sz < <(btree_build q1_bt $tbl "(a)")
  force_ios_gucs
  encourage_parallel_gucs 0
  read -r pl ex ex95 rows_out node idx rel wp wl < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","btree","int4","k1",0,"unique",$rows,0.5,"ASC",0,1,$bt_build_ms,$bt_sz,$pl,$ex,$ex95,$rows_out,$node,$idx,$rel,$wp,$wl,$(date +%s)
  # SMOL
  read -r sm_build_ms sm_sz < <(smol_build q1_sm $tbl "(a)")
  read -r pl2 ex2 ex95_2 rows_out2 node2 idx2 rel2 wp2 wl2 < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","smol","int4","k1",0,"unique",$rows,0.5,"ASC",0,1,$sm_build_ms,$sm_sz,$pl2,$ex2,$ex95_2,$rows_out2,$node2,$idx2,$rel2,$wp2,$wl2,$(date +%s)
}

bench_int4_hotkey_include_eq() {
  local case_id="INT4_HOT_INCLUDE_EQ_1M"; local rows=1000000
  local tbl="q2_t"; psql_exec "DROP TABLE IF EXISTS ${tbl} CASCADE; CREATE UNLOGGED TABLE ${tbl}(a int4, x1 int4, x2 int4);"
  # Heavy duplicate hot key 42 (80% rows as 42), rest sequential
  psql_exec "INSERT INTO ${tbl} SELECT 42, 111, 222 FROM generate_series(1, ${rows}*8/10); INSERT INTO ${tbl} SELECT i, 111, 222 FROM generate_series(1, ${rows}-${rows}*8/10) i;"; local q="SELECT sum(x1)+sum(x2) FROM ${tbl} WHERE a = 42"; printf "%s\n%s;\n" "-- $case_id" "$q" >> "$CASES_OUT"
  table_prep_common "$tbl"
  # BTREE INCLUDE
  read -r bt_build_ms bt_sz < <(btree_build q2_bt $tbl "(a) INCLUDE (x1,x2)")
  read -r pl ex ex95 rows_out node idx rel wp wl < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","btree","int4","k1",2,"hot1",$rows,"eq","ASC",0,1,$bt_build_ms,$bt_sz,$pl,$ex,$ex95,$rows_out,$node,$idx,$rel,$wp,$wl,$(date +%s)
  # SMOL
  read -r sm_build_ms sm_sz < <(smol_build q2_sm $tbl "(a) INCLUDE (x1,x2)")
  read -r pl2 ex2 ex95_2 rows_out2 node2 idx2 rel2 wp2 wl2 < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","smol","int4","k1",2,"hot1",$rows,"eq","ASC",0,1,$sm_build_ms,$sm_sz,$pl2,$ex2,$ex95_2,$rows_out2,$node2,$idx2,$rel2,$wp2,$wl2,$(date +%s)
}

bench_date_int4_k2eq_k1range() {
  local case_id="DATE_INT4_K2EQ_K1RANGE_1M"; local rows=1000000
  local tbl="q3_t"; psql_exec "DROP TABLE IF EXISTS ${tbl} CASCADE; CREATE UNLOGGED TABLE ${tbl}(d date, b int4); INSERT INTO ${tbl} SELECT DATE '2020-01-01' + (i-1), (i % 10) FROM generate_series(1, ${rows}) i;"
  table_prep_common "$tbl"
  local d0
  d0=$(psql_q "WITH m AS (SELECT d AS md FROM ${tbl} ORDER BY d LIMIT 1 OFFSET ${rows}/10) SELECT md FROM m")
  read -r bt_build_ms bt_sz < <(btree_build q3_bt $tbl "(d,b)")
  local q="SELECT count(*) FROM ${tbl} WHERE d >= '${d0}' AND b = 5"; printf "%s\n%s;\n" "-- $case_id" "$q" >> "$CASES_OUT"
  read -r pl ex ex95 rows_out node idx rel wp wl < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","btree","date","k2",0,"mod10",$rows,">=10%","ASC",0,1,$bt_build_ms,$bt_sz,$pl,$ex,$ex95,$rows_out,$node,$idx,$rel,$wp,$wl,$(date +%s)
  read -r sm_build_ms sm_sz < <(smol_build q3_sm $tbl "(d,b)")
  read -r pl2 ex2 ex95_2 rows_out2 node2 idx2 rel2 wp2 wl2 < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","smol","date","k2",0,"mod10",$rows,">=10%","ASC",0,1,$sm_build_ms,$sm_sz,$pl2,$ex2,$ex95_2,$rows_out2,$node2,$idx2,$rel2,$wp2,$wl2,$(date +%s)
}

bench_text16_c_range() {
  local case_id="TEXT16C_RANGE_100K"; local rows=100000
  local tbl="q4_t"; psql_exec "DROP TABLE IF EXISTS ${tbl} CASCADE; CREATE UNLOGGED TABLE ${tbl}(b text COLLATE pg_catalog.\"C\");"
  # 16B text keys via base36 counter padded to length
  psql_exec "INSERT INTO ${tbl} SELECT lpad(to_char(i, '9999999999999999'), 16, '0') FROM generate_series(1, ${rows}) i;"
  table_prep_common "$tbl"
  read -r bt_build_ms bt_sz < <(btree_build q4_bt $tbl "(b COLLATE pg_catalog.\"C\")")
  local q="SELECT count(*) FROM ${tbl} WHERE b >= 'm'"; printf "%s\n%s;\n" "-- $case_id" "$q" >> "$CASES_OUT"
  read -r pl ex ex95 rows_out node idx rel wp wl < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","btree","text16C","k1",0,"unique",$rows,">=m","ASC",0,1,$bt_build_ms,$bt_sz,$pl,$ex,$ex95,$rows_out,$node,$idx,$rel,$wp,$wl,$(date +%s)
  read -r sm_build_ms sm_sz < <(smol_build q4_sm $tbl "(b COLLATE pg_catalog.\"C\")")
  read -r pl2 ex2 ex95_2 rows_out2 node2 idx2 rel2 wp2 wl2 < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","smol","text16C","k1",0,"unique",$rows,">=m","ASC",0,1,$sm_build_ms,$sm_sz,$pl2,$ex2,$ex95_2,$rows_out2,$node2,$idx2,$rel2,$wp2,$wl2,$(date +%s)
}

bench_int4_moddup_parallel() {
  local case_id="INT4_MODDUP_PAR_5M"; local rows=5000000
  local tbl="q5_t"; psql_exec "DROP TABLE IF EXISTS ${tbl} CASCADE; CREATE UNLOGGED TABLE ${tbl}(a int4); INSERT INTO ${tbl} SELECT (i % 100000)::int4 FROM generate_series(1, ${rows}) i;"
  table_prep_common "$tbl"
  read -r bt_build_ms bt_sz < <(btree_build q5_bt $tbl "(a)")
  # workers 0 and 4
  local q="SELECT count(*) FROM ${tbl} WHERE a >= 90000"; printf "%s\n%s;\n" "-- $case_id" "$q" >> "$CASES_OUT"
  read -r pl ex ex95 rows_out node idx rel wp wl < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","btree","int4","k1",0,"dup_mod100k",$rows,">=90k","ASC",0,1,$bt_build_ms,$bt_sz,$pl,$ex,$ex95,$rows_out,$node,$idx,$rel,$wp,$wl,$(date +%s)
  read -r plp exp ex95p rows_outp nodep idxp relp wpp wlp < <(measure_query "$q" "$REPEATS" 1 4)
  csv_row "$case_id","btree","int4","k1",0,"dup_mod100k",$rows,">=90k","ASC",4,1,$bt_build_ms,$bt_sz,$plp,$exp,$ex95p,$rows_outp,$nodep,$idxp,$relp,$wpp,$wlp,$(date +%s)
  read -r sm_build_ms sm_sz < <(smol_build q5_sm $tbl "(a)")
  read -r pl2 ex2 ex95_2 rows_out2 node2 idx2 rel2 wp2 wl2 < <(measure_query "$q" "$REPEATS" 1 0)
  csv_row "$case_id","smol","int4","k1",0,"dup_mod100k",$rows,">=90k","ASC",0,1,$sm_build_ms,$sm_sz,$pl2,$ex2,$ex95_2,$rows_out2,$node2,$idx2,$rel2,$wp2,$wl2,$(date +%s)
  read -r pl2p ex2p ex952p rows_out2p node2p idx2p rel2p wp2p wl2p < <(measure_query "$q" "$REPEATS" 1 4)
  csv_row "$case_id","smol","int4","k1",0,"dup_mod100k",$rows,">=90k","ASC",4,1,$sm_build_ms,$sm_sz,$pl2p,$ex2p,$ex952p,$rows_out2p,$node2p,$idx2p,$rel2p,$wp2p,$wl2p,$(date +%s)
}

bench_quick() {
  ensure_extension
  csv_header
  bench_int4_unique_range
  bench_int4_hotkey_include_eq
  bench_date_int4_k2eq_k1range
  bench_text16_c_range
  bench_int4_moddup_parallel
}

# ---------------- Full suite (scaffold) -----------------
bench_full() {
  ensure_extension
  csv_header
  # Modest grid (can be expanded): key types, sizes, workers, distributions and includes
  local key_types=(int2 int4 int8 uuid date)
  local sizes=(100000 1000000)
  local workers=(0 4)
  local dists=(unique hot1)
  local includes=(0 2)
  for kt in "${key_types[@]}"; do
    for n in "${sizes[@]}"; do
      for dist in "${dists[@]}"; do
        for inc in "${includes[@]}"; do
          local tbl="f_${kt}_${n}_${dist}_inc${inc}"
          # Build table
          psql_exec "DROP TABLE IF EXISTS ${tbl} CASCADE; CREATE UNLOGGED TABLE ${tbl}(a ${kt}`[ $inc -gt 0 ] && printf ', ' || true``[ $inc -gt 0 ] && printf 'x1 int4, x2 int4' || true`);"
          if [[ "$dist" == "unique" ]]; then
            psql_exec "INSERT INTO ${tbl}(a`[ $inc -gt 0 ] && printf ', x1, x2' || true`) SELECT i::${kt}`[ $inc -gt 0 ] && printf ', (i%1000), (i%10000)' || true` FROM generate_series(1, ${n}) i;"
          else
            # hot1: heavy duplicate on key 42
            psql_exec "INSERT INTO ${tbl}(a`[ $inc -gt 0 ] && printf ', x1, x2' || true`) SELECT 42::${kt}`[ $inc -gt 0 ] && printf ', 111, 222' || true` FROM generate_series(1, ${n}*7/10); INSERT INTO ${tbl}(a`[ $inc -gt 0 ] && printf ', x1, x2' || true`) SELECT i::${kt}`[ $inc -gt 0 ] && printf ', 111, 222' || true` FROM generate_series(1, ${n}-${n}*7/10) i;"
          fi
          table_prep_common "$tbl"
          # Index specs
          local spec="(a)"; local spec_btree="(a)"; local cols="k1"
          if [[ $inc -gt 0 ]]; then spec="(a) INCLUDE (x1,x2)"; spec_btree="(a) INCLUDE (x1,x2)"; fi
          read -r bt_build_ms bt_sz < <(btree_build f_bt_${kt}_${n}_${dist}_inc${inc} $tbl "$spec_btree")
          # Range threshold ~50%
          local q="SELECT count(*) FROM ${tbl} WHERE a >= (${n}/2)::${kt}"
          for w in "${workers[@]}"; do
            read -r pl ex ex95 rows_out node idx rel wp wl < <(measure_query "$q" "$REPEATS" 1 "$w")
            csv_row "FULL","btree","$kt","$cols",$inc,"$dist",$n,0.5,"ASC",$w,1,$bt_build_ms,$bt_sz,$pl,$ex,$ex95,$rows_out,$node,$idx,$rel,$wp,$wl,$(date +%s)
          done
          read -r sm_build_ms sm_sz < <(smol_build f_sm_${kt}_${n}_${dist}_inc${inc} $tbl "$spec")
          for w in "${workers[@]}"; do
            read -r pl2 ex2 ex95_2 rows_out2 node2 idx2 rel2 wp2 wl2 < <(measure_query "$q" "$REPEATS" 1 "$w")
            csv_row "FULL","smol","$kt","$cols",$inc,"$dist",$n,0.5,"ASC",$w,1,$sm_build_ms,$sm_sz,$pl2,$ex2,$ex95_2,$rows_out2,$node2,$idx2,$rel2,$wp2,$wl2,$(date +%s)
          done
        done
      done
    done
  done
}

write_md() {
  {
    echo "# SMOL Bench ${MODE}"
    echo
    echo "- Timestamp: $(date -u)"
    echo "- Repeats: ${REPEATS}"
    echo "- Warm-only: ${WARM_ONLY}"
    echo "- jq: $([[ $HAVE_JQ -eq 1 ]] && echo present || echo absent)"
    echo "- CSV: $(basename \"$CSV_OUT\")"
    echo "- Cases: $(basename \"$CASES_OUT\")"
    echo
    echo "## Raw Results (exec_ms_med)"
    echo
    echo "case_id | workers | engine | exec_ms_med | plan_ms_med | rows_out | node | index | rel | workers(planned/launched)"
    echo "---|---:|---|---:|---:|---:|---|---|---|---:"
  } > "$MD_OUT"

  awk -F, 'NR>1 { printf "%s | %s | %s | %s | %s | %s | %s | %s | %s | %s/%s\n", $1, $10, $2, $15, $14, $17, $18, $19, $20, $21, $22 }' "$CSV_OUT" >> "$MD_OUT"

  {
    echo
    echo "## Speedup vs BTREE (lower is faster)"
    echo
    echo "case_id | workers | btree_ms | smol_ms | speedup (btree/smol)"
    echo "---|---:|---:|---:|---:"
  } >> "$MD_OUT"

  awk -F, '
    NR==1 {next}
    { key=$1"|"$10; if($2=="btree"){b[key]=$15} else if($2=="smol"){s[key]=$15} ks[key]=1 }
    END {
      for (k in ks) { bm=b[k]; sm=s[k]; if (bm=="" || sm=="" || sm==0) continue; split(k,a,"|"); printf "%s | %s | %.3f | %.3f | %.2f\n", a[1], a[2], bm+0, sm+0, (bm+0)/(sm+0) }
    }
  ' "$CSV_OUT" >> "$MD_OUT"

  awk -F, '
    NR==1 {next}
    ($10=="0" && $2=="btree") { key=$1"|"$10; b[key]=$15; ks[key]=1 }
    ($10=="0" && $2=="smol")  { key=$1"|"$10; s[key]=$15; ks[key]=1 }
    END { c=0; sum=0; for (k in ks) if (b[k]!="" && s[k]!="" && s[k]>0) { sum += (b[k]/s[k]); c++ } if (c>0) printf "\nAverage speedup (workers=0): %.2f (n=%d)\n", sum/c, c; }
  ' "$CSV_OUT" >> "$MD_OUT"

  if command -v codex >/dev/null 2>&1 && [[ -n "${OPENAI_API_KEY:-}" ]] && [[ "${ENABLE_CODEX_ANALYSIS:-0}" == "1" ]]; then
    mkdir -p /root/.codex
    printf '{"OPENAI_API_KEY":"%s"}\n' "$OPENAI_API_KEY" > /root/.codex/auth.json
    ANALYSIS_OUT="$(mktemp /tmp/analysis.XXXXXX)"
    {
      echo "You are a benchmarking analyst. Given the CSV below (SMOL vs BTREE), write a concise qualitative assessment: where SMOL wins/loses, sensitivity to selectivity/duplicates/INCLUDE, and parallel scaling. Offer 2–3 actionable tuning ideas."
      echo
      cat "$CSV_OUT"
    } > "$ANALYSIS_OUT"
    echo "\n## Qualitative Assessment (Codex)\n" >> "$MD_OUT"
    codex -a never --sandbox danger-full-access "$(cat "$ANALYSIS_OUT")" >> "$MD_OUT" || echo "(Codex analysis failed)" >> "$MD_OUT"
    rm -f "$ANALYSIS_OUT"
  else
    echo "\nNote: Skipped AI analysis (Codex/OPENAI_API_KEY unavailable)." >> "$MD_OUT"
  fi
}

main() {
  if [[ "$MODE" == "quick" ]]; then
    bench_quick
  else
    bench_full
  fi
  write_md
  echo "Wrote: $CSV_OUT"
  echo "Wrote: $MD_OUT"
}

main "$@"
#!/usr/bin/env bash
set -euo pipefail

# SMOL benchmarking harness (quick/full).
# - Runs inside the smol Docker container (preferred).
# - Uses psql as the postgres user; collects metrics via EXPLAIN JSON.
# - Writes CSV and a minimal Markdown report to results/.

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
RESULTS_DIR="$ROOT/results"
PSQL="/usr/local/pgsql/bin/psql -v ON_ERROR_STOP=1 -X -q -t -A -d postgres"
HAVE_JQ=0
if command -v jq >/dev/null 2>&1; then HAVE_JQ=1; fi

MODE="quick"
REPEATS=7
WARM_ONLY=1

usage() {
  echo "Usage: $0 [--quick|--full] [--repeats N] [--cold]";
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick) MODE=quick; ;;
    --full) MODE=full; ;;
    --repeats) REPEATS=${2:-7}; shift ;;
    --cold) WARM_ONLY=0 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
  shift
done

mkdir -p "$RESULTS_DIR"
CSV_OUT="$RESULTS_DIR/${MODE}-$(date +%Y%m%d-%H%M%S).csv"
MD_OUT="$RESULTS_DIR/${MODE}-$(date +%Y%m%d-%H%M%S).md"

as_pg() {
  # Run command as postgres user
  su - postgres -c "$*"
}

psql_q() {
  local sql="$1"
  local tf
  tf=$(mktemp /tmp/benchsql.XXXXXX)
  printf "%s\n" "$sql" > "$tf"
  chmod 644 "$tf"
  as_pg "$PSQL -f $tf"
  rm -f "$tf"
}

ensure_extension() {
  psql_q "CREATE EXTENSION IF NOT EXISTS smol;"
}

restart_pg() {
  # Cold-cache reset: restart PG and drop OS caches if permitted
  if as_pg "/usr/local/pgsql/bin/pg_ctl -D /var/lib/postgresql/data -m fast -w restart"; then
    :
  fi
  if [[ -w /proc/sys/vm/drop_caches ]]; then
    sync; echo 3 > /proc/sys/vm/drop_caches || true
  fi
}

table_prep_common() {
  local tbl="$1"
  psql_q "ALTER TABLE ${tbl} SET (autovacuum_enabled = off)" || true
  psql_q "ANALYZE ${tbl}"
  # Freeze for stable visibility
  psql_q "CHECKPOINT"
  psql_q "SET vacuum_freeze_min_age=0"
  psql_q "SET vacuum_freeze_table_age=0"
  psql_q "VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) ${tbl}"
}

btree_build() {
  local idx="$1" tbl="$2" spec="$3"
  local t0 t1
  t0=$(date +%s%3N)
  psql_q "DROP INDEX IF EXISTS ${idx}; CREATE INDEX ${idx} ON ${tbl} USING btree ${spec};"
  t1=$(date +%s%3N)
  local ms=$((t1 - t0))
  local sz=$(psql_q "SELECT pg_relation_size('${idx}')")
  echo "$ms" "$sz"
}

smol_build() {
  local idx="$1" tbl="$2" spec="$3" rle="$4"
  local t0 t1
  psql_q "SET smol.rle = ${rle};"
  t0=$(date +%s%3N)
  psql_q "DROP INDEX IF EXISTS ${idx}; CREATE INDEX ${idx} ON ${tbl} USING smol ${spec};"
  t1=$(date +%s%3N)
  local ms=$((t1 - t0))
  local sz=$(psql_q "SELECT pg_relation_size('${idx}')")
  echo "$ms" "$sz"
}

force_ios_gucs() {
  psql_q "SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexscan=off; SET enable_indexonlyscan=on;"
}

encourage_parallel_gucs() {
  local workers="$1"
  psql_q "SET max_parallel_workers_per_gather=${workers}; SET max_parallel_workers=${workers}; SET parallel_setup_cost=0; SET parallel_tuple_cost=0; SET min_parallel_index_scan_size=0; SET min_parallel_table_scan_size=0;"
}

parse_explain() {
  # stdin: EXPLAIN output; stdout: plan_ms exec_ms rows_out
  if [[ $HAVE_JQ -eq 1 ]]; then
    jq -r '.[0] | [ .["Planning Time"], .["Execution Time"], (.Plan["Actual Rows"] // 0) ] | @tsv'
  else
    # Parse plain text EXPLAIN ANALYZE output
    awk '
      /Planning Time:/ {plan=$3}
      /Execution Time:/ {exec=$3}
      {
        if (match($0, /rows=[0-9]+/)) { rows = substr($0, RSTART+5, RLENGTH-5) }
      }
      END {
        if (plan=="") plan=0; if (exec=="") exec=0; if (rows=="") rows=0;
        print plan"\t"exec"\t"rows
      }'
  fi
}

run_explain() {
  local sql="$1"
  if [[ $HAVE_JQ -eq 1 ]]; then
    as_pg "$PSQL -c \"EXPLAIN (ANALYZE, TIMING ON, BUFFERS OFF, FORMAT JSON) ${sql}\""
  else
    as_pg "$PSQL -c \"EXPLAIN (ANALYZE, TIMING ON, BUFFERS OFF) ${sql}\""
  fi
}

# Compute statistics without GNU awk extensions
median() {
  # reads numbers on stdin; prints median
  sort -n | awk '{a[NR]=$1} END { if (NR==0) {print 0; exit} if (NR%2){print a[(NR+1)/2]} else {printf "%.6f\n", (a[NR/2]+a[NR/2+1])/2} }'
}
p95() {
  sort -n | awk '{a[NR]=$1} END { if (NR==0){print 0; exit} idx=int(0.95*NR); if (idx<1) idx=1; if (idx>NR) idx=NR; print a[idx] }'
}

measure_query() {
  local sql="$1" repeats="$2" warm="$3" workers="$4"
  force_ios_gucs
  encourage_parallel_gucs "$workers"
  # warm-up run if requested
  if [[ "$warm" -eq 1 ]]; then
    run_explain "$sql" >/dev/null 2>&1 || true
  fi
  local tmp_pl=/tmp/pl.$$
  local tmp_ex=/tmp/ex.$$
  : > "$tmp_pl"; : > "$tmp_ex"
  local rows_out=0
  for ((i=1;i<=repeats;i++)); do
    local out
    out=$(run_explain "$sql" | parse_explain)
    local pl ex rows
    pl="$(echo "$out" | cut -f1)"; ex="$(echo "$out" | cut -f2)"; rows="$(echo "$out" | cut -f3)"
    echo "$pl" >> "$tmp_pl"; echo "$ex" >> "$tmp_ex"; rows_out="$rows"
  done
  local pl_med ex_med ex_p95
  pl_med=$(cat "$tmp_pl" | median)
  ex_med=$(cat "$tmp_ex" | median)
  ex_p95=$(cat "$tmp_ex" | p95)
  rm -f "$tmp_pl" "$tmp_ex"
  echo "$pl_med" "$ex_med" "$ex_p95" "$rows_out"
}
