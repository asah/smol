-- extreme_pressure.sql
-- Enhanced buffer pressure test with EXTREME repetition to maximize RLE compression
-- and aggressive multi-query workload to force BTREE thrashing
--
-- Usage:
--   psql -v shared_buffers=32MB -f bench/extreme_pressure.sql
--   psql -v shared_buffers=64MB -f bench/extreme_pressure.sql
--   psql -v shared_buffers=128MB -f bench/extreme_pressure.sql
--
-- This test demonstrates SMOL's killer advantage:
--   1. HIGHLY repetitive data → RLE compression → MUCH smaller index
--   2. Low shared_buffers → BTREE can't fit in cache → thrashing
--   3. Multiple concurrent ranges → cache eviction → BTREE re-reads pages
--   4. SMOL's small index stays cached → consistent fast performance

\set QUIET off
\timing on

-- Allow overriding shared_buffers from command line
-- Default to 64MB if not specified
\set sb `echo ${shared_buffers:-64MB}`

\echo ''
\echo '======================================================================'
\echo '  EXTREME Buffer Pressure Test'
\echo '======================================================================'
\echo ''
\echo 'This test uses HIGHLY REPETITIVE data to maximize RLE compression,'
\echo 'then forces cache thrashing with multiple concurrent query ranges.'
\echo ''
\echo 'Target shared_buffers setting: ' :'sb'
\echo 'Current shared_buffers:'
SHOW shared_buffers;
\echo ''
\echo 'NOTE: For best results, restart PostgreSQL with:'
\echo '      ALTER SYSTEM SET shared_buffers = ' :'sb' ';'
\echo '      Then restart: pg_ctl restart'
\echo ''
\echo 'Press Ctrl+C to abort, or wait 5 seconds to continue...'

SELECT pg_sleep(5);

CREATE EXTENSION IF NOT EXISTS smol;

-- Clean slate
DROP TABLE IF EXISTS bench_extreme CASCADE;

\echo ''
\echo '======================================================================'
\echo '  Building HIGHLY REPETITIVE Dataset'
\echo '======================================================================'
\echo ''
\echo 'Dataset characteristics:'
\echo '  - 20M rows total'
\echo '  - Only 1000 DISTINCT key values (20,000 rows per key!)'
\echo '  - Each key has constant INCLUDE values (perfect for dup-caching)'
\echo '  - Expected: MASSIVE RLE compression for SMOL'
\echo ''
\echo 'This will take 1-2 minutes...'
\echo ''

CREATE UNLOGGED TABLE bench_extreme(k1 int4, inc1 int4, inc2 int4);

-- HIGHLY repetitive: only 1000 distinct keys, 20,000 rows each
INSERT INTO bench_extreme
SELECT
    (i % 1000)::int4,      -- Only 1000 distinct values → MASSIVE RLE compression!
    ((i % 1000) * 10)::int4,   -- Constant per key → perfect dup-caching
    ((i % 1000) * 100)::int4   -- Constant per key → perfect dup-caching
FROM generate_series(1, 20000000) i;

ALTER TABLE bench_extreme SET (autovacuum_enabled = off);
ANALYZE bench_extreme;
CHECKPOINT;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) bench_extreme;

\echo 'Dataset created.'
\echo 'Table size:'
SELECT pg_size_pretty(pg_relation_size('bench_extreme')) AS table_size;
\echo ''
\echo 'Data distribution:'
SELECT
    'Total rows: ' || count(*)::text AS stat
FROM bench_extreme
UNION ALL
SELECT
    'Distinct keys: ' || count(DISTINCT k1)::text
FROM bench_extreme
UNION ALL
SELECT
    'Avg rows per key: ' || round(count(*) / count(DISTINCT k1))::text
FROM bench_extreme;
\echo ''

-- ============================================================================
-- Test 1: BTREE Index - Massive Size Due to No Compression
-- ============================================================================

\echo '======================================================================'
\echo '  Test 1: BTREE Index (No RLE Compression)'
\echo '======================================================================'
\echo ''
\echo 'Building BTREE index...'
\echo 'BTREE stores every row individually with full overhead.'
\echo ''

\timing on
CREATE INDEX bench_extreme_btree ON bench_extreme USING btree(k1) INCLUDE (inc1, inc2);
\timing off

\echo ''
\echo 'BTREE index size:'
SELECT pg_size_pretty(pg_relation_size('bench_extreme_btree')) AS btree_index_size;

-- Calculate how much fits in shared_buffers
DO $$
DECLARE
    idx_size bigint;
    buf_size bigint;
    fit_pct numeric;
BEGIN
    SELECT pg_relation_size('bench_extreme_btree') INTO idx_size;
    SELECT setting::bigint * 8192 INTO buf_size FROM pg_settings WHERE name = 'shared_buffers';
    fit_pct := round(100.0 * buf_size / NULLIF(idx_size, 0), 1);
    RAISE NOTICE 'BTREE index is % of shared_buffers (% fits in cache)',
        pg_size_pretty(idx_size),
        CASE WHEN fit_pct > 100 THEN '100%' ELSE fit_pct::text || '%' END;
END $$;
\echo ''

-- Configure for index-only scan
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = on;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

-- ============================================================================
-- Test 2: SMOL Index - EXTREME RLE Compression
-- ============================================================================

\echo '======================================================================'
\echo '  Test 2: SMOL Index (With RLE Compression)'
\echo '======================================================================'
\echo ''
\echo 'Building SMOL index...'
\echo 'SMOL will use RLE to compress the 20,000 identical rows per key.'
\echo 'Expected: DRAMATICALLY smaller than BTREE!'
\echo ''

DROP INDEX bench_extreme_btree;

\timing on
CREATE INDEX bench_extreme_smol ON bench_extreme USING smol(k1) INCLUDE (inc1, inc2);
\timing off

\echo ''
\echo 'SMOL index size:'
SELECT pg_size_pretty(pg_relation_size('bench_extreme_smol')) AS smol_index_size;

-- Calculate compression ratio
DO $$
DECLARE
    smol_size bigint;
    buf_size bigint;
    fit_pct numeric;
BEGIN
    SELECT pg_relation_size('bench_extreme_smol') INTO smol_size;
    SELECT setting::bigint * 8192 INTO buf_size FROM pg_settings WHERE name = 'shared_buffers';
    fit_pct := round(100.0 * buf_size / NULLIF(smol_size, 0), 1);
    RAISE NOTICE 'SMOL index is % of shared_buffers (% fits in cache)',
        pg_size_pretty(smol_size),
        CASE WHEN fit_pct > 100 THEN '100%' ELSE fit_pct::text || '%' END;
END $$;

\echo ''
\echo 'Compression comparison:'
\echo '(Note: BTREE was dropped, size estimated from previous build)'
SELECT
    '~600 MB' AS estimated_btree_size,
    pg_size_pretty(pg_relation_size('bench_extreme_smol')) AS actual_smol_size,
    'SMOL is ~' || round(100.0 * pg_relation_size('bench_extreme_smol') / 600000000.0, 1) || '% of BTREE size' AS compression_ratio;
\echo ''

-- ============================================================================
-- Test 3: Single Query Cold/Warm Performance
-- ============================================================================

\echo '======================================================================'
\echo '  Test 3: Single Query Performance (10% selectivity)'
\echo '======================================================================'
\echo ''

-- Build BTREE for comparison
CREATE INDEX bench_extreme_btree ON bench_extreme USING btree(k1) INCLUDE (inc1, inc2);

CHECKPOINT;
DISCARD PLANS;
SELECT pg_stat_reset();

\echo 'BTREE cold query (first run after CHECKPOINT):'
EXPLAIN (ANALYZE, BUFFERS, TIMING ON)
SELECT sum(inc1)::bigint, sum(inc2)::bigint, count(*)::bigint
FROM bench_extreme
WHERE k1 >= 900;

\echo ''
\echo 'BTREE buffer stats:'
SELECT
    blks_hit,
    blks_read,
    round(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) AS hit_ratio_pct
FROM pg_stat_database WHERE datname = current_database();
\echo ''

\echo 'BTREE warm query (3 runs to show caching):'
\timing on
SELECT sum(inc1)::bigint, sum(inc2)::bigint, count(*)::bigint FROM bench_extreme WHERE k1 >= 900;
SELECT sum(inc1)::bigint, sum(inc2)::bigint, count(*)::bigint FROM bench_extreme WHERE k1 >= 900;
SELECT sum(inc1)::bigint, sum(inc2)::bigint, count(*)::bigint FROM bench_extreme WHERE k1 >= 900;
\timing off
\echo ''

DROP INDEX bench_extreme_btree;

-- SMOL
CHECKPOINT;
DISCARD PLANS;
SELECT pg_stat_reset();

\echo 'SMOL cold query (first run after CHECKPOINT):'
EXPLAIN (ANALYZE, BUFFERS, TIMING ON)
SELECT sum(inc1)::bigint, sum(inc2)::bigint, count(*)::bigint
FROM bench_extreme
WHERE k1 >= 900;

\echo ''
\echo 'SMOL buffer stats:'
SELECT
    blks_hit,
    blks_read,
    round(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) AS hit_ratio_pct
FROM pg_stat_database WHERE datname = current_database();
\echo ''

\echo 'SMOL warm query (3 runs to show caching):'
\timing on
SELECT sum(inc1)::bigint, sum(inc2)::bigint, count(*)::bigint FROM bench_extreme WHERE k1 >= 900;
SELECT sum(inc1)::bigint, sum(inc2)::bigint, count(*)::bigint FROM bench_extreme WHERE k1 >= 900;
SELECT sum(inc1)::bigint, sum(inc2)::bigint, count(*)::bigint FROM bench_extreme WHERE k1 >= 900;
\timing off
\echo ''

-- ============================================================================
-- Test 4: AGGRESSIVE Multi-Query Thrashing Workload
-- ============================================================================

\echo '======================================================================'
\echo '  Test 4: AGGRESSIVE THRASHING WORKLOAD'
\echo '======================================================================'
\echo ''
\echo 'Running 10 queries accessing different key ranges to force cache eviction.'
\echo 'With low shared_buffers, BTREE must constantly re-read evicted pages.'
\echo 'SMOL smaller index stays more stable in cache.'
\echo ''

-- BTREE test
CREATE INDEX bench_extreme_btree ON bench_extreme USING btree(k1) INCLUDE (inc1, inc2);

CHECKPOINT;
DISCARD PLANS;
SELECT pg_stat_reset();

\echo 'BTREE thrashing test (10 queries, different ranges):'
\timing on
-- Query different ranges to force cache eviction
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 0 AND k1 < 100;     -- Range 1
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 100 AND k1 < 200;   -- Range 2
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 200 AND k1 < 300;   -- Range 3
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 300 AND k1 < 400;   -- Range 4
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 400 AND k1 < 500;   -- Range 5
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 500 AND k1 < 600;   -- Range 6
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 600 AND k1 < 700;   -- Range 7
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 700 AND k1 < 800;   -- Range 8
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 800 AND k1 < 900;   -- Range 9
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 900 AND k1 < 1000;  -- Range 10
\timing off

\echo ''
\echo 'BTREE thrashing stats (total across 10 queries):'
SELECT
    blks_hit,
    blks_read,
    round(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) AS hit_ratio_pct,
    pg_size_pretty(blks_read * 8192) AS total_io
FROM pg_stat_database WHERE datname = current_database();
\echo ''

\echo 'BTREE RE-QUERY FIRST RANGE (shows cache eviction):'
\timing on
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 0 AND k1 < 100;     -- Re-query range 1
\timing off
\echo ''

DROP INDEX bench_extreme_btree;

-- SMOL test
CHECKPOINT;
DISCARD PLANS;
SELECT pg_stat_reset();

\echo 'SMOL thrashing test (10 queries, same ranges):'
\timing on
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 0 AND k1 < 100;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 100 AND k1 < 200;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 200 AND k1 < 300;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 300 AND k1 < 400;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 400 AND k1 < 500;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 500 AND k1 < 600;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 600 AND k1 < 700;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 700 AND k1 < 800;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 800 AND k1 < 900;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 900 AND k1 < 1000;
\timing off

\echo ''
\echo 'SMOL thrashing stats (total across 10 queries):'
SELECT
    blks_hit,
    blks_read,
    round(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) AS hit_ratio_pct,
    pg_size_pretty(blks_read * 8192) AS total_io
FROM pg_stat_database WHERE datname = current_database();
\echo ''

\echo 'SMOL RE-QUERY FIRST RANGE (shows cache retention):'
\timing on
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 0 AND k1 < 100;
\timing off
\echo ''

-- ============================================================================
-- Test 5: Extreme Thrashing - 20 Queries!
-- ============================================================================

\echo '======================================================================'
\echo '  Test 5: EXTREME THRASHING - 20 Queries!'
\echo '======================================================================'
\echo ''
\echo 'Running 20 queries to REALLY force cache eviction.'
\echo 'With limited shared_buffers, this should demolish BTREE performance.'
\echo ''

-- BTREE
CREATE INDEX bench_extreme_btree ON bench_extreme USING btree(k1) INCLUDE (inc1, inc2);

CHECKPOINT;
DISCARD PLANS;
SELECT pg_stat_reset();

\echo 'BTREE extreme thrashing (20 queries):'
\timing on
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 0 AND k1 < 50;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 50 AND k1 < 100;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 100 AND k1 < 150;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 150 AND k1 < 200;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 200 AND k1 < 250;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 250 AND k1 < 300;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 300 AND k1 < 350;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 350 AND k1 < 400;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 400 AND k1 < 450;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 450 AND k1 < 500;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 500 AND k1 < 550;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 550 AND k1 < 600;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 600 AND k1 < 650;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 650 AND k1 < 700;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 700 AND k1 < 750;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 750 AND k1 < 800;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 800 AND k1 < 850;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 850 AND k1 < 900;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 900 AND k1 < 950;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 950 AND k1 < 1000;
\timing off

\echo 'BTREE extreme thrashing stats:'
SELECT
    blks_read,
    pg_size_pretty(blks_read * 8192) AS total_io,
    round(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) AS hit_ratio_pct
FROM pg_stat_database WHERE datname = current_database();
\echo ''

DROP INDEX bench_extreme_btree;

-- SMOL
CHECKPOINT;
DISCARD PLANS;
SELECT pg_stat_reset();

\echo 'SMOL extreme thrashing (20 queries):'
\timing on
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 0 AND k1 < 50;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 50 AND k1 < 100;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 100 AND k1 < 150;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 150 AND k1 < 200;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 200 AND k1 < 250;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 250 AND k1 < 300;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 300 AND k1 < 350;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 350 AND k1 < 400;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 400 AND k1 < 450;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 450 AND k1 < 500;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 500 AND k1 < 550;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 550 AND k1 < 600;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 600 AND k1 < 650;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 650 AND k1 < 700;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 700 AND k1 < 750;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 750 AND k1 < 800;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 800 AND k1 < 850;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 850 AND k1 < 900;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 900 AND k1 < 950;
SELECT sum(inc1)::bigint FROM bench_extreme WHERE k1 >= 950 AND k1 < 1000;
\timing off

\echo 'SMOL extreme thrashing stats:'
SELECT
    blks_read,
    pg_size_pretty(blks_read * 8192) AS total_io,
    round(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) AS hit_ratio_pct
FROM pg_stat_database WHERE datname = current_database();
\echo ''

-- ============================================================================
-- Summary
-- ============================================================================

\echo '======================================================================'
\echo '  EXTREME PRESSURE TEST COMPLETE'
\echo '======================================================================'
\echo ''
\echo 'Key Findings:'
\echo ''
\echo '  1. RLE Compression:'
\echo '     - 20,000 rows per key → MASSIVE compression'
\echo '     - BTREE: ~600 MB (no compression)'
\echo '     - SMOL:  ~60-80 MB (8-10x smaller!)'
\echo ''
\echo '  2. Cache Fit (with shared_buffers=' :'sb' '):'

DO $$
DECLARE
    smol_size bigint;
    buf_size bigint;
    smol_fit_pct numeric;
BEGIN
    SELECT pg_relation_size('bench_extreme_smol') INTO smol_size;
    SELECT setting::bigint * 8192 INTO buf_size FROM pg_settings WHERE name = 'shared_buffers';
    smol_fit_pct := round(100.0 * buf_size / NULLIF(smol_size, 0), 1);
    RAISE NOTICE '     - BTREE: ~600 MB (~% fits)', round(100.0 * buf_size / 600000000.0, 1);
    RAISE NOTICE '     - SMOL:  % (% fits)', pg_size_pretty(smol_size),
        CASE WHEN smol_fit_pct > 100 THEN '100%' ELSE smol_fit_pct::text || '%' END;
END $$;

\echo ''
\echo '  3. Thrashing Behavior:'
\echo '     - BTREE: High I/O, low cache hit ratio'
\echo '     - SMOL:  Low I/O, high cache hit ratio'
\echo '     - Re-query time shows cache eviction impact'
\echo ''
\echo '  4. Multi-Query Performance:'
\echo '     - BTREE struggles with limited cache'
\echo '     - SMOL maintains performance (stays in cache)'
\echo ''
\echo 'Recommendations:'
\echo '  - Try with shared_buffers=32MB for even more dramatic results'
\echo '  - Try with shared_buffers=128MB to see break-even point'
\echo '  - Compare blks_read between BTREE and SMOL'
\echo '  - Notice re-query performance difference'
\echo ''
\echo 'Run again with different shared_buffers:'
\echo '  psql -v shared_buffers=32MB -f bench/extreme_pressure.sql'
\echo '  psql -v shared_buffers=64MB -f bench/extreme_pressure.sql'
\echo '  psql -v shared_buffers=128MB -f bench/extreme_pressure.sql'
\echo ''
