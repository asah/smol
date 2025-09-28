# Benchmarking SMOL

Benchmarking is super important because the primary purpose of SMOL is performance.

SMOL is a tree structured index targeting simple, fixed-length single keys and narrow tables with highly optimized index-only scans (IOS). Therefore, its primary competitor is B-Tree, for which PostgreSQL has a high quality builtin index access method. Hash indexes compete in single key lookups but not range queries. BRIN indexes don't support index-only scans.

Query speed is the primary performance goal. Subtly, SMOL is almost smaller and therefore creates less RAM/buffer/cache pressure, but this is tricky to measure, and even moreso because indexes are already much smaller than base tables and therefore has limited impact on cache pressure.

SMOL is read-only at this time, so update performance is obviously not important to measure. SMOL indexes are faster to build than B-Tree indexes, but that's rarely a concern.

SMOL is only designed for IOS, so we can ignore table layout and instead focus on performance for covering indexes (INCLUDE).

SMOL is optimized for single keys and if multi-column key performance is lousy for some reason, users can synthesize more-complex single keys, relying on our benchmarks that involve longer single keys.

SMOL should be optimized for modern cloud computing environments with many cores, lots of RAM, network SSD, etc. 


# Benchmark and Performance Goals

SMOL should be faster than B-Tree by enough margin to justify users considering its adoption.

SMOL performance should be predictable enough for users to rely on it.



# Benchmark Philosophy

Thinking top-down, SMOL is designed for these types of data and workloads:
 - narrow JOIN and lookup tables with short, fixed-length keys and fields
 - narrow time series and log tables

Thinking bottom-up and white-box, SMOL performance is affected by the amount and type of data and query selectivity. Notably, it uses compression internally so repetitive data will probably behave differently.

We'd like two benchmark Makefile targets:
 - for development and CI, a quick benchmark which checks common cases for performance regression
 - for releases and publication, a thorough benchmark which examines the performance-curve across many axes

In both cases, we'd like to output raw CSV for later processing, human-readable markdown files containing AI-written analysis of the benchmark runs and appropriate charts/graphs in some portable easy to consume format, while keeping our toolchain simple and highly portable, for example raw HTML+SVG would be terrific, PNG if we can keep the toolchain simple.


# Benchmark Design

This section defines concrete datasets, queries, metrics, and a simple, portable
framework to run both quick and full benchmarks. It favors repeatability,
portability, and apples-to-apples comparison versus PostgreSQL btree.

Scope and non-goals
- Measure read-side performance only: index build time and index-only scans.
- Compare SMOL vs BTREE for identical schemas and queries.
- Avoid external profilers; rely on PostgreSQL timings and counters.
- Keep toolchain simple: psql + Make + bash; optional jq for JSON parsing.

Core metrics (per run)
- exec_ms: execution time in ms for the query (median over N repeats).
- plan_ms: planning time in ms (from EXPLAIN JSON).
- rows: actual rows returned.
- idx_size_bytes: size of the index relation.
- build_ms: time to CREATE INDEX (captured once per dataset+index).
- warm: warm or cold cache condition for the run.
- workers: number of parallel workers planned/used (forced via GUCs).
- extras (optional if pg_stat_statements enabled): total_time, calls.

Measurement method
- Use EXPLAIN (ANALYZE, TIMING ON, BUFFERS OFF, FORMAT JSON) for each query.
  Parse JSON to extract Planning Time, Execution Time, Actual Rows.
- Repeat each query R times; discard first iteration; record median and p95.
- Warm runs: issue once to warm, then measure. Cold runs: drop caches and
  restart PostgreSQL or echo 3 > /proc/sys/vm/drop_caches inside Docker.
- Record pg_relation_size(index) and CREATE INDEX wall-clock time.

Axes and datasets
- Key type: int2, int4, int8, uuid, date, text(<=32B, C collation).
- Columns: single-key; two-key (k1,k2) with equality on k2.
- INCLUDE payload: 0, 2, 8 fixed-length include columns (int4 unless noted).
- Duplicates: unique, moderate dup (Zipf s=1.1), heavy dup (few hot keys).
- Rows: 1e5 (small), 1e6 (medium), 1e7 (large, optional for full run).
- Selectivity (on leading key): equality; range with s ∈ {1e-6,1e-4,1e-2,0.1,1.0}.
- Order direction: ASC and DESC.
- Parallelism: workers ∈ {0,2,4,8} (encourage via planner GUCs).
- RLE: off/on for single-key SMOL when duplicates present.

Representative quick suite (bench-quick)
- Q1: int4, single-key, ROWS=1e6, unique, include=0.
  - Predicates: a >= median (s≈0.5), a >= 0.01p (s≈0.99), a = mode (miss)
  - Runs: warm only, R=7 repeats, workers=0.
- Q2: int4, single-key, ROWS=1e6, heavy duplicates (10 hot keys), include=2.
  - Predicates: a = hotkey, a >= hotkey, a >= cold threshold
  - RLE: off vs on; warm runs; workers=0.
- Q3: (date,int4), two-key, ROWS=1e6; b = 42 AND d >= d0; include=0.
  - Selectivity tuned so result ~1e4 rows; warm runs; workers=0.
- Q4: text(16B, C), single-key, ROWS=1e5, unique, include=0.
  - Predicates: b >= 'm', b = 'a'; warm runs; workers=0.
- Q5: int4, single-key, ROWS=5e6, dup=moderate, include=0.
  - Predicate: a >= threshold (s≈0.1). Parallel workers={0,4}. Warm runs.

Comprehensive suite (bench-full)
- Grid across:
  - Key type ∈ {int2,int4,int8,uuid,date,text16C}
  - Columns ∈ {k1, (k1,k2)}; includes ∈ {0,2,8} (includes only for k1)
  - Duplicates ∈ {unique,zipf1.1,hot10}
  - Rows ∈ {1e5,1e6,(1e7 optional)}
  - Selectivity ∈ {eq, 1e-4, 1e-2, 0.1, 1.0}
  - Direction ∈ {ASC,DESC}
  - Parallel workers ∈ {0,2,4,8}
- For each case:
  1) Create table UNLOGGED; insert deterministic data; ANALYZE; freeze.
  2) Build BTREE index (and BTREE INCLUDE when comparing INCLUDE); VACUUM FREEZE.
     Record build_ms and idx_size_bytes.
  3) Execute query set (warm and cold). Record metrics.
  4) Drop BTREE; build SMOL; repeat step 3. For SMOL, also RLE on/off when applicable.

Deterministic data generation
- Unique ints: sequential 1..N or shuffled with fixed seed.
- Zipf duplicates: generate via inverse transform with fixed seed; cap to UNIQVALS.
- Hot keys: pick K hot keys with high mass; rest uniform.
- Date sequence: start at DATE '2020-01-01' + i.
- text16C: built from base-36 counter to 16 bytes; enforce COLLATE "C".
- INCLUDE columns: small int payloads or 16B uuid, derived from key for stability.

Query set (by case type)
- Single-key equality: SELECT count/sum/projection WHERE k = const.
- Single-key range: WHERE k >= t (ASC and DESC, with ORDER BY k LIMIT X optional for top-k).
- Two-key with equality on k2: WHERE k1 > t AND k2 = v.
- INCLUDE projection: SUM over include columns to ensure payload access.
- For each query, force IOS path correctness: SET enable_indexscan=off; enable_indexonlyscan=on; enable_seqscan=off; enable_bitmapscan=off.

Parallel settings
- Encourage parallel with:
  SET max_parallel_workers_per_gather=8; SET max_parallel_workers=8;
  SET parallel_setup_cost=0; SET parallel_tuple_cost=0;
  SET min_parallel_index_scan_size=0; SET min_parallel_table_scan_size=0;
  Measure with workers ∈ {0,2,4,8} by toggling max_parallel_workers_per_gather.

Cache control
- Warm: run once to warm, then measure.
- Cold: restart PostgreSQL between runs; inside Docker, drop OS cache with
  echo 3 > /proc/sys/vm/drop_caches (root) to reset file cache.

Outputs
- Raw CSV per run with columns:
  case_id, engine, key_type, cols, includes, duplicates, rows, selectivity,
  direction, workers, warm, rle, build_ms, idx_size_bytes, plan_ms, exec_ms,
  rows_out, timestamp
- Aggregated CSV with: case_id, engine, metric=exec_ms, p50, p95, min, max, n.
- Human-readable report: Markdown with summary tables and embedded SVG charts
  (latency vs selectivity curves; throughput vs workers; index-size vs rows).

Framework design
- Makefile targets:
  - bench-quick: build image if needed, start PG, run quick suite, write CSV to results/quick.csv and a Markdown report to results/quick.md.
  - bench-full: idem, running the full grid; results/full-YYYYmmdd-HHMM.csv and .md.
- Driver: bash + psql orchestrator, no external deps beyond jq (optional).
  - Use EXPLAIN (FORMAT JSON) and jq to extract Planning/Execution times.
  - Without jq, parse plain EXPLAIN text for "Execution Time:" with awk/sed.
  - Use \copy to append rows to CSV from psql.
- SQL utilities (bench/sql):
  - bench_util.sql with helper functions: make_data(params), build_idx(kind),
    run_case(params) returning (plan_ms, exec_ms, rows).
  - Deterministic data generators using generate_series and SQL.
- Cold/warm harness: helper script bench/caches.sh to restart PG and drop caches when inside Docker; no-op fallback if not permitted.

Validation and safety
- For each dataset, sanity-check correctness parity (md5 over ordered results) between BTREE and SMOL for one representative query before timing.
- Ensure IOS path by GUCs; enforce text C collation for text cases.
- Use UNLOGGED tables and VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) for stable visibility.

Success criteria and analysis
- SMOL should outperform BTREE notably for index-only scans:
  - Low-to-moderate selectivity ranges (e.g., 1e-2 to 0.5): 1.2–2.0x target.
  - Equality on hot duplicates with RLE: higher gains expected.
  - INCLUDE projections: reduced per-row copy wins over BTREE IOS.
- Parallel scaling: demonstrate near-linear scaling up to 4–8 workers on range scans.
- Report includes: per-axis graphs and a narrative of observed patterns.

Reproducibility knobs
- Seed all random generators; record GUCs alongside results.
- Emit the exact SQL used for each case into results/cases.sql for auditing.

Next steps (optional implementation plan)
- Add bench/ directory with bash driver and SQL utilities.
- Add Makefile targets bench-quick and bench-full using the existing Docker helpers (start/stop, psql).
- Start with the quick suite and evolve toward the full grid.
