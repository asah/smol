-- ============================================================================
-- SIMPLE THRASHING TEST: Demonstrate SMOL's Cache Advantage
-- ============================================================================
--
-- MEASURED DATA (1M rows, 1000 distinct keys):
--   BTREE: 30 MB per 1M rows
--   SMOL:  11.5 MB per 1M rows (2.61x compression)
--
-- TARGET: 64MB shared_buffers
--   Dataset: 10M rows
--   BTREE: ~300 MB (4.7x larger than cache) → will thrash
--   SMOL:  ~115 MB (1.8x larger than cache) → less thrashing
--
-- EXPECTED RESULT:
--   BTREE: Increasing latency as cache fills, then stays high
--   SMOL:  Lower latency, more stable performance
--   Speedup: 2-3x on sustained queries
--
-- ============================================================================

\timing on
\set QUIET on

\echo ''
\echo '========================================='
\echo 'SIMPLE THRASHING TEST'
\echo '========================================='
\echo ''
\echo 'Configuration: shared_buffers should be 64MB'
\echo ''

SELECT current_setting('shared_buffers') AS shared_buffers;

-- ============================================================================
-- PHASE 1: Create 10M row dataset
-- ============================================================================

\echo ''
\echo 'Phase 1: Creating 10M row dataset...'
\echo ''

DROP TABLE IF EXISTS thrash CASCADE;

CREATE TABLE thrash (
    k1 int4,
    inc1 int4,
    inc2 int4
) WITH (autovacuum_enabled = false);

-- 10M rows with 1000 distinct keys = 10,000 rows per key
INSERT INTO thrash
SELECT
    (i % 1000)::int4,
    (i % 1000)::int4,
    (i % 1000)::int4
FROM generate_series(1, 10000000) i;

VACUUM ANALYZE thrash;

\echo 'Dataset created.'

-- ============================================================================
-- PHASE 2: Build indexes
-- ============================================================================

\echo ''
\echo 'Phase 2: Building BTREE index...'

DROP INDEX IF EXISTS thrash_btree;
CREATE INDEX thrash_btree ON thrash (k1) INCLUDE (inc1, inc2);
VACUUM ANALYZE thrash;

\echo 'Phase 2: Building SMOL index...'

DROP INDEX IF EXISTS thrash_smol;
CREATE INDEX thrash_smol ON thrash USING smol (k1) INCLUDE (inc1, inc2);
VACUUM ANALYZE thrash;

\echo ''
\echo 'Index Sizes:'
SELECT
    'BTREE' as index_type,
    pg_size_pretty(pg_relation_size('thrash_btree')) as size,
    pg_relation_size('thrash_btree') / (1024*1024) as size_mb
UNION ALL
SELECT
    'SMOL' as index_type,
    pg_size_pretty(pg_relation_size('thrash_smol')) as size,
    pg_relation_size('thrash_smol') / (1024*1024) as size_mb;

-- ============================================================================
-- PHASE 3: BTREE Thrashing Test (20 queries)
-- ============================================================================

\echo ''
\echo '========================================='
\echo 'Phase 3: BTREE Thrashing Test'
\echo '========================================='
\echo ''

-- Force index-only scan, disable parallel to see pure index performance
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = on;
SET enable_indexonlyscan = on;
SET max_parallel_workers_per_gather = 0;

-- Drop SMOL index to force BTREE usage
DROP INDEX thrash_smol;

CHECKPOINT;
SELECT pg_stat_reset();

\echo 'BTREE Query 1/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 0 AND k1 < 50;

\echo 'BTREE Query 2/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 50 AND k1 < 100;

\echo 'BTREE Query 3/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 100 AND k1 < 150;

\echo 'BTREE Query 4/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 150 AND k1 < 200;

\echo 'BTREE Query 5/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 200 AND k1 < 250;

\echo 'BTREE Query 6/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 250 AND k1 < 300;

\echo 'BTREE Query 7/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 300 AND k1 < 350;

\echo 'BTREE Query 8/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 350 AND k1 < 400;

\echo 'BTREE Query 9/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 400 AND k1 < 450;

\echo 'BTREE Query 10/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 450 AND k1 < 500;

\echo 'BTREE Query 11/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 500 AND k1 < 550;

\echo 'BTREE Query 12/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 550 AND k1 < 600;

\echo 'BTREE Query 13/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 600 AND k1 < 650;

\echo 'BTREE Query 14/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 650 AND k1 < 700;

\echo 'BTREE Query 15/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 700 AND k1 < 750;

\echo 'BTREE Query 16/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 750 AND k1 < 800;

\echo 'BTREE Query 17/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 800 AND k1 < 850;

\echo 'BTREE Query 18/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 850 AND k1 < 900;

\echo 'BTREE Query 19/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 900 AND k1 < 950;

\echo 'BTREE Query 20/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 950 AND k1 < 1000;

\echo ''
\echo 'BTREE Query 20 with EXPLAIN (should show high cache hits):'
EXPLAIN (ANALYZE, BUFFERS) SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 950 AND k1 < 1000;

\echo ''
\echo 'BTREE I/O Stats:'
SELECT
    blks_read,
    blks_hit,
    pg_size_pretty((blks_read + blks_hit) * 8192) as total_io,
    round(100.0 * blks_hit / NULLIF(blks_read + blks_hit, 0), 2) as hit_ratio_pct
FROM pg_stat_database
WHERE datname = current_database();

-- ============================================================================
-- PHASE 4: SMOL Thrashing Test (20 queries)
-- ============================================================================

\echo ''
\echo '========================================='
\echo 'Phase 4: SMOL Thrashing Test'
\echo '========================================='
\echo ''

-- Rebuild SMOL index and drop BTREE
DROP INDEX thrash_btree;
CREATE INDEX thrash_smol ON thrash USING smol (k1) INCLUDE (inc1, inc2);
VACUUM ANALYZE thrash;

CHECKPOINT;
SELECT pg_stat_reset();

\echo 'SMOL Query 1/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 0 AND k1 < 50;

\echo 'SMOL Query 2/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 50 AND k1 < 100;

\echo 'SMOL Query 3/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 100 AND k1 < 150;

\echo 'SMOL Query 4/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 150 AND k1 < 200;

\echo 'SMOL Query 5/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 200 AND k1 < 250;

\echo 'SMOL Query 6/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 250 AND k1 < 300;

\echo 'SMOL Query 7/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 300 AND k1 < 350;

\echo 'SMOL Query 8/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 350 AND k1 < 400;

\echo 'SMOL Query 9/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 400 AND k1 < 450;

\echo 'SMOL Query 10/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 450 AND k1 < 500;

\echo 'SMOL Query 11/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 500 AND k1 < 550;

\echo 'SMOL Query 12/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 550 AND k1 < 600;

\echo 'SMOL Query 13/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 600 AND k1 < 650;

\echo 'SMOL Query 14/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 650 AND k1 < 700;

\echo 'SMOL Query 15/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 700 AND k1 < 750;

\echo 'SMOL Query 16/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 750 AND k1 < 800;

\echo 'SMOL Query 17/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 800 AND k1 < 850;

\echo 'SMOL Query 18/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 850 AND k1 < 900;

\echo 'SMOL Query 19/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 900 AND k1 < 950;

\echo 'SMOL Query 20/20'
SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 950 AND k1 < 1000;

\echo ''
\echo 'SMOL Query 20 with EXPLAIN (should show very high cache hits):'
EXPLAIN (ANALYZE, BUFFERS) SELECT sum(inc1)::bigint FROM thrash WHERE k1 >= 950 AND k1 < 1000;

\echo ''
\echo 'SMOL I/O Stats:'
SELECT
    blks_read,
    blks_hit,
    pg_size_pretty((blks_read + blks_hit) * 8192) as total_io,
    round(100.0 * blks_hit / NULLIF(blks_read + blks_hit, 0), 2) as hit_ratio_pct
FROM pg_stat_database
WHERE datname = current_database();

-- ============================================================================
-- Summary
-- ============================================================================

\echo ''
\echo '========================================='
\echo 'ANALYSIS'
\echo '========================================='
\echo ''
\echo 'Compare the query times above:'
\echo '  - BTREE: Should show increasing latency as cache fills'
\echo '  - SMOL: Should show more stable, lower latency'
\echo ''
\echo 'To calculate speedup:'
\echo '  1. Average BTREE times for queries 10-20'
\echo '  2. Average SMOL times for queries 10-20'
\echo '  3. Speedup = BTREE_avg / SMOL_avg'
\echo ''
