-- thrash.sql - Low memory benchmark to demonstrate SMOL's space efficiency
-- This test intentionally sets low shared_buffers to force I/O and show
-- how SMOL's smaller index size provides an advantage when indexes don't fit in RAM

\set QUIET off
\timing on

CREATE EXTENSION IF NOT EXISTS smol;
CREATE EXTENSION IF NOT EXISTS pg_buffercache;

\echo '=== Thrash Test (Low Memory) ==='
\echo ''
\echo 'This test demonstrates SMOL advantage when indexes exceed shared_buffers'
\echo 'Current shared_buffers:'
SHOW shared_buffers;
\echo ''

-- Clean slate
DROP TABLE IF EXISTS bench_thrash CASCADE;

-- ============================================================================
-- Large dataset: 5M rows with moderate duplicates and INCLUDE columns
-- Goal: Create indexes larger than shared_buffers to force disk I/O
-- ============================================================================

\echo 'Building test table...'

CREATE TABLE bench_thrash(k1 bigint, inc1 bigint, val text collate "C", bigfield text);

INSERT INTO bench_thrash
SELECT
    (i % 1000)::bigint,
    (i % 1000)::bigint,
    repeat('1234567890', 2)
FROM generate_series(1, 20000000) i;

\echo 'Table size:'
SELECT pg_size_pretty(pg_relation_size('bench_thrash'));
\echo ''

-- ============================================================================
-- BTREE Index
-- ============================================================================

\echo 'Building BTREE index with INCLUDE...'
\timing on
CREATE INDEX bench_thrash_btree ON bench_thrash USING btree(k1) INCLUDE (inc1, val);
\timing off

\echo 'BTREE index size:'
SELECT pg_size_pretty(pg_relation_size('bench_thrash_btree')) AS btree_size;
\echo ''

-- Configure for IOS
SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_indexonlyscan = on;
SET enable_bitmapscan = on;
SET max_parallel_workers_per_gather = 0;

\echo 'BTREE query - multiple iterations to test caching...'

\timing off
SELECT * FROM pg_buffercache_evict_relation('bench_thrash_btree'::regclass);

\timing on
SELECT inc1, count(*) FROM bench_thrash WHERE k1 % 4 = 0 group by 1 order by 1 limit 1;
SELECT inc1, count(*) FROM bench_thrash WHERE k1 % 4 = 0 group by 1 order by 1 limit 1;
SELECT inc1, count(*) FROM bench_thrash WHERE k1 % 4 = 0 group by 1 order by 1 limit 1;

\timing off
explain (analyze, verbose, buffers) SELECT inc1, count(*) FROM bench_thrash WHERE k1 % 4 = 0 group by 1 order by 1 limit 1;
\echo ''

-- ============================================================================
-- SMOL Index
-- ============================================================================

\echo 'Building SMOL index with INCLUDE...'
\timing on
CREATE INDEX bench_thrash_smol ON bench_thrash USING smol(k1) INCLUDE (inc1, val);
\timing off

\echo 'Size comparison:'
SELECT
    pg_size_pretty(pg_relation_size('bench_thrash_btree')) AS btree_size,
    pg_size_pretty(pg_relation_size('bench_thrash_smol')) AS smol_size;
\echo ''

-- ensure btree isn't used
DROP INDEX bench_thrash_btree;


\echo 'SMOL query - multiple iterations to test caching...'

\timing off
SELECT * FROM pg_buffercache_evict_relation('bench_thrash_smol'::regclass);

\timing on
SELECT inc1, count(*) FROM bench_thrash WHERE k1 % 4 = 0 group by 1 order by 1 limit 1;
SELECT inc1, count(*) FROM bench_thrash WHERE k1 % 4 = 0 group by 1 order by 1 limit 1;
SELECT inc1, count(*) FROM bench_thrash WHERE k1 % 4 = 0 group by 1 order by 1 limit 1;

\timing off
explain (analyze, verbose, buffers) SELECT inc1, count(*) FROM bench_thrash WHERE k1 % 4 = 0 group by 1 order by 1 limit 1;
\echo ''

