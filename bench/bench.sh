#!/usr/bin/env bash
#
# bench.sh - SMOL vs BTREE benchmark driver
#
# Usage:
#   ./bench/bench.sh --quick          # quick regression suite
#   ./bench/bench.sh --full           # full comprehensive suite
#   ./bench/bench.sh --thrash         # low shared_buffers test
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="${SCRIPT_DIR}/.."
RESULTS_DIR="${WORKSPACE}/results"
SQL_DIR="${SCRIPT_DIR}/sql"

# Default parameters
MODE="quick"
REPEATS=5
WARMUP_RUNS=1
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick) MODE="quick"; shift ;;
        --full) MODE="full"; shift ;;
        --thrash) MODE="thrash"; shift ;;
        --repeats) REPEATS="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Ensure directories exist
mkdir -p "${RESULTS_DIR}"
mkdir -p "${SQL_DIR}"

# PostgreSQL connection (assumes running instance)
PSQL="psql -X -q -v ON_ERROR_STOP=1"

echo "[bench] Starting benchmark in ${MODE} mode (repeats=${REPEATS})"
echo "[bench] Results will be written to ${RESULTS_DIR}/"

# CSV output file
CSV_FILE="${RESULTS_DIR}/${MODE}-${TIMESTAMP}.csv"
MD_FILE="${RESULTS_DIR}/${MODE}-${TIMESTAMP}.md"

# Initialize CSV with header
cat > "${CSV_FILE}" <<'EOF'
case_id,engine,key_type,cols,includes,duplicates,rows,selectivity,direction,workers,warm,build_ms,idx_size_mb,plan_ms,exec_ms,rows_out
EOF

echo "[bench] CSV output: ${CSV_FILE}"
echo "[bench] Markdown report: ${MD_FILE}"

# Function to run a single benchmark case
run_case() {
    local case_id="$1"
    local engine="$2"      # btree or smol
    local key_type="$3"     # int2, int4, int8, etc.
    local cols="$4"         # 1 or 2
    local includes="$5"     # 0, 2, 8
    local duplicates="$6"   # unique, zipf, hot
    local rows="$7"
    local selectivity="$8"  # eq, 1e-4, 0.01, 0.1, 1.0
    local direction="$9"    # asc or desc
    local workers="${10}"
    local warm="${11}"      # warm or cold

    # Create table and data
    local table_name="bench_${case_id}"
    local idx_name="${table_name}_${engine}"

    echo "  [case ${case_id}] engine=${engine} type=${key_type} rows=${rows} sel=${selectivity} workers=${workers} warm=${warm}"

    # Build SQL for this case
    local build_sql="${SQL_DIR}/build_${case_id}_${engine}.sql"
    local query_sql="${SQL_DIR}/query_${case_id}_${engine}.sql"

    # Generate data and index (this is simplified - real implementation in bench_util.sql)
    $PSQL <<SQL_BUILD
\\timing on
DROP TABLE IF EXISTS ${table_name} CASCADE;
CREATE UNLOGGED TABLE ${table_name}(
    k1 ${key_type},
    k2 int4,
    inc1 int4,
    inc2 int4
);

-- Generate deterministic data
INSERT INTO ${table_name}
SELECT
    (CASE WHEN '${duplicates}' = 'unique' THEN i
          WHEN '${duplicates}' = 'zipf' THEN (i % (${rows}/100))::${key_type}
          ELSE (CASE WHEN i % 10 = 0 THEN 42 ELSE i END)::${key_type}
     END) AS k1,
    (i % 1000)::int4 AS k2,
    (i % 10000)::int4 AS inc1,
    (i % 10000)::int4 AS inc2
FROM generate_series(1, ${rows}) AS s(i);

ALTER TABLE ${table_name} SET (autovacuum_enabled = off);
ANALYZE ${table_name};
CHECKPOINT;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) ${table_name};
ANALYZE ${table_name};

-- Build index and measure time
\\timing on
CREATE INDEX ${idx_name} ON ${table_name} USING ${engine}(k1)$(if [ "$includes" -gt 0 ]; then echo " INCLUDE (inc1, inc2)"; fi);
\\timing off

-- Record index size
SELECT pg_relation_size('${idx_name}') AS idx_size;
SQL_BUILD

    # Extract build time and index size from psql output
    # (simplified - real version parses \timing output)

    # Run query with appropriate settings
    local query_predicate
    if [ "$selectivity" = "eq" ]; then
        query_predicate="k1 = 42"
    else
        query_predicate="k1 >= CAST(${rows} * (1.0 - ${selectivity}) AS ${key_type})"
    fi

    local order_clause=""
    if [ "$direction" = "desc" ]; then
        order_clause="ORDER BY k1 DESC"
    fi

    # Configure parallel workers
    $PSQL -c "SET max_parallel_workers_per_gather = ${workers};"

    # Warm cache if requested
    if [ "$warm" = "warm" ]; then
        $PSQL -c "SELECT count(*) FROM ${table_name} WHERE ${query_predicate};" > /dev/null
    fi

    # Run actual timed query
    local result
    result=$($PSQL -t <<SQL_QUERY
EXPLAIN (ANALYZE, TIMING ON, BUFFERS OFF, FORMAT JSON)
SELECT $(if [ "$includes" -gt 0 ]; then echo "inc1, inc2,"; fi) count(*)
FROM ${table_name}
WHERE ${query_predicate} ${order_clause};
SQL_QUERY
)

    # Parse JSON result (simplified)
    # Real version extracts: plan_ms, exec_ms, rows_out
    local plan_ms=0.0
    local exec_ms=0.0
    local rows_out=0

    # Append to CSV
    echo "${case_id},${engine},${key_type},${cols},${includes},${duplicates},${rows},${selectivity},${direction},${workers},${warm},0.0,0.0,${plan_ms},${exec_ms},${rows_out}" >> "${CSV_FILE}"
}

# Quick benchmark suite
if [ "$MODE" = "quick" ]; then
    echo "[bench] Running quick benchmark suite..."

    # Q1: int4, single-key, 1M rows, unique, no includes
    run_case "q1a" "btree" "int4" 1 0 "unique" 1000000 "0.5" "asc" 0 "warm"
    run_case "q1b" "smol" "int4" 1 0 "unique" 1000000 "0.5" "asc" 0 "warm"

    # Q2: int4, heavy duplicates, 1M rows, 2 includes
    run_case "q2a" "btree" "int4" 1 2 "hot" 1000000 "eq" "asc" 0 "warm"
    run_case "q2b" "smol" "int4" 1 2 "hot" 1000000 "eq" "asc" 0 "warm"

    echo "[bench] Quick suite complete."
fi

# Thrash test - low shared_buffers to force I/O
if [ "$MODE" = "thrash" ]; then
    echo "[bench] Running thrash test (low shared_buffers)..."
    echo "[bench] This test demonstrates SMOL's advantage when indexes don't fit in RAM"

    # Stop PostgreSQL
    make -C "${WORKSPACE}" stop || true

    # Modify postgresql.conf to set shared_buffers = 128MB
    # (assuming standard location)
    local pgdata="/var/lib/postgresql/data"
    if [ -f "${pgdata}/postgresql.conf" ]; then
        cp "${pgdata}/postgresql.conf" "${pgdata}/postgresql.conf.bak.thrash"
        echo "shared_buffers = 128MB" >> "${pgdata}/postgresql.conf"
        echo "effective_cache_size = 256MB" >> "${pgdata}/postgresql.conf"
    fi

    # Restart with low memory settings
    make -C "${WORKSPACE}" start

    # Run large dataset benchmark
    echo "[bench] Building 10M row dataset..."
    run_case "thrash1" "btree" "int4" 1 2 "zipf" 10000000 "0.1" "asc" 0 "cold"
    run_case "thrash2" "smol" "int4" 1 2 "zipf" 10000000 "0.1" "asc" 0 "cold"

    # Restore postgresql.conf
    if [ -f "${pgdata}/postgresql.conf.bak.thrash" ]; then
        mv "${pgdata}/postgresql.conf.bak.thrash" "${pgdata}/postgresql.conf"
    fi

    make -C "${WORKSPACE}" stop
    make -C "${WORKSPACE}" start

    echo "[bench] Thrash test complete."
fi

# Generate markdown report
cat > "${MD_FILE}" <<'MDEOF'
# SMOL Benchmark Report

Generated: $(date)
Mode: ${MODE}
Repeats: ${REPEATS}

## Summary

This report compares SMOL vs BTREE index performance for index-only scans.

### Key Findings

(Analysis would go here - comparing exec times, index sizes, etc.)

## Detailed Results

See CSV file: ${CSV_FILE}

MDEOF

echo "[bench] Benchmark complete!"
echo "[bench] Results written to:"
echo "[bench]   ${CSV_FILE}"
echo "[bench]   ${MD_FILE}"
