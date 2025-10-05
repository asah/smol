-- Performance investigation test
-- Isolate the slow query pattern

\set QUIET off
\timing on

CREATE EXTENSION IF NOT EXISTS smol;

DROP TABLE IF EXISTS perf_test CASCADE;

\echo 'Creating test table: 1M rows with duplicates (200 rows per key)...'
CREATE UNLOGGED TABLE perf_test(k1 int4, inc1 int4, inc2 int4);

INSERT INTO perf_test
SELECT
    (i % 5000)::int4,  -- ~200 rows per distinct key
    (i % 10000)::int4,
    (i % 10000)::int4
FROM generate_series(1, 1000000) i;

ALTER TABLE perf_test SET (autovacuum_enabled = off);
ANALYZE perf_test;
CHECKPOINT;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) perf_test;

\echo ''
\echo 'Building SMOL index...'
CREATE INDEX perf_test_smol ON perf_test USING smol(k1) INCLUDE (inc1, inc2);

\echo 'Index size:'
SELECT pg_size_pretty(pg_relation_size('perf_test_smol'));

-- Configure for IOS
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = on;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

\echo ''
\echo 'Running query (10% selectivity, ~100K rows returned)...'
\echo 'Warmup run:'
SELECT sum(inc1)::bigint, count(*)::bigint FROM perf_test WHERE k1 >= 4500;

\echo ''
\echo 'Timed runs:'
\timing on
SELECT sum(inc1)::bigint, count(*)::bigint FROM perf_test WHERE k1 >= 4500;
SELECT sum(inc1)::bigint, count(*)::bigint FROM perf_test WHERE k1 >= 4500;
SELECT sum(inc1)::bigint, count(*)::bigint FROM perf_test WHERE k1 >= 4500;
\timing off
