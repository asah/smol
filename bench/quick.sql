-- quick.sql - Quick benchmark suite for SMOL vs BTREE
-- Runs a focused set of cases to check for performance regressions

\set QUIET off
\timing on

CREATE EXTENSION IF NOT EXISTS smol;

-- Clean slate
DROP TABLE IF EXISTS bench_q1 CASCADE;
DROP TABLE IF EXISTS bench_q2 CASCADE;
DROP TABLE IF EXISTS bench_q3 CASCADE;

\echo '=== Quick Benchmark Suite ==='
\echo ''

-- ============================================================================
-- Q1: int4, single-key, 1M rows, unique, no includes
-- Target: Basic range scan performance
-- ============================================================================
\echo 'Q1: int4 single-key, 1M unique rows, range scan (50% selectivity)'

CREATE UNLOGGED TABLE bench_q1(k1 int4);
INSERT INTO bench_q1 SELECT i FROM generate_series(1, 1000000) i;
ALTER TABLE bench_q1 SET (autovacuum_enabled = off);
ANALYZE bench_q1;
CHECKPOINT;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) bench_q1;

-- BTREE
\echo '  Building BTREE index...'
\timing on
CREATE INDEX bench_q1_btree ON bench_q1 USING btree(k1);
\timing off
\echo '  Index size:'
SELECT pg_size_pretty(pg_relation_size('bench_q1_btree'));

-- Configure for IOS
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = on;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

\echo '  BTREE query (warm):'
SELECT count(*) FROM bench_q1 WHERE k1 >= 500000;  -- warmup
\timing on
SELECT count(*) FROM bench_q1 WHERE k1 >= 500000;
SELECT count(*) FROM bench_q1 WHERE k1 >= 500000;
SELECT count(*) FROM bench_q1 WHERE k1 >= 500000;
\timing off

DROP INDEX bench_q1_btree;

-- SMOL
\echo '  Building SMOL index...'
\timing on
CREATE INDEX bench_q1_smol ON bench_q1 USING smol(k1);
\timing off
\echo '  Index size:'
SELECT pg_size_pretty(pg_relation_size('bench_q1_smol'));

\echo '  SMOL query (warm):'
SELECT count(*) FROM bench_q1 WHERE k1 >= 500000;  -- warmup
\timing on
SELECT count(*) FROM bench_q1 WHERE k1 >= 500000;
SELECT count(*) FROM bench_q1 WHERE k1 >= 500000;
SELECT count(*) FROM bench_q1 WHERE k1 >= 500000;
\timing off

\echo ''

-- ============================================================================
-- Q2: int4, single-key, 1M rows, heavy duplicates, 2 includes
-- Target: RLE compression + INCLUDE duplicate caching
-- ============================================================================
\echo 'Q2: int4 single-key, 1M rows with heavy duplicates, 2 INCLUDE columns'

CREATE UNLOGGED TABLE bench_q2(k1 int4, inc1 int4, inc2 int4);
-- Generate data: 10 hot keys (90% of data) + unique tail
INSERT INTO bench_q2
SELECT
    CASE WHEN i % 10 < 9 THEN (i % 10) ELSE 100 + i END,
    111,  -- constant INCLUDE for dup-caching benefit
    222
FROM generate_series(1, 1000000) i;
ALTER TABLE bench_q2 SET (autovacuum_enabled = off);
ANALYZE bench_q2;
CHECKPOINT;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) bench_q2;

-- BTREE
\echo '  Building BTREE index with INCLUDE...'
\timing on
CREATE INDEX bench_q2_btree ON bench_q2 USING btree(k1) INCLUDE (inc1, inc2);
\timing off
\echo '  Index size:'
SELECT pg_size_pretty(pg_relation_size('bench_q2_btree'));

\echo '  BTREE query (equality on hot key, projecting INCLUDEs):'
SELECT sum(inc1), sum(inc2) FROM bench_q2 WHERE k1 = 5;  -- warmup
\timing on
SELECT sum(inc1), sum(inc2) FROM bench_q2 WHERE k1 = 5;
SELECT sum(inc1), sum(inc2) FROM bench_q2 WHERE k1 = 5;
SELECT sum(inc1), sum(inc2) FROM bench_q2 WHERE k1 = 5;
\timing off

DROP INDEX bench_q2_btree;

-- SMOL
\echo '  Building SMOL index with INCLUDE...'
\timing on
CREATE INDEX bench_q2_smol ON bench_q2 USING smol(k1) INCLUDE (inc1, inc2);
\timing off
\echo '  Index size:'
SELECT pg_size_pretty(pg_relation_size('bench_q2_smol'));

\echo '  SMOL query (equality on hot key, projecting INCLUDEs):'
SELECT sum(inc1), sum(inc2) FROM bench_q2 WHERE k1 = 5;  -- warmup
\timing on
SELECT sum(inc1), sum(inc2) FROM bench_q2 WHERE k1 = 5;
SELECT sum(inc1), sum(inc2) FROM bench_q2 WHERE k1 = 5;
SELECT sum(inc1), sum(inc2) FROM bench_q2 WHERE k1 = 5;
\timing off

\echo ''

-- ============================================================================
-- Q3: Two-column (date, int4), 1M rows
-- Target: Two-column scan performance
-- ============================================================================
\echo 'Q3: Two-column (date, int4), 1M rows, range on leading key'

CREATE UNLOGGED TABLE bench_q3(k1 date, k2 int4);
INSERT INTO bench_q3
SELECT
    '2020-01-01'::date + (i % 3650),
    i % 1000
FROM generate_series(1, 1000000) i;
ALTER TABLE bench_q3 SET (autovacuum_enabled = off);
ANALYZE bench_q3;
CHECKPOINT;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) bench_q3;

-- BTREE (using INCLUDE to match SMOL behavior)
\echo '  Building BTREE index...'
\timing on
CREATE INDEX bench_q3_btree ON bench_q3 USING btree(k1) INCLUDE (k2);
\timing off
\echo '  Index size:'
SELECT pg_size_pretty(pg_relation_size('bench_q3_btree'));

\echo '  BTREE query (range + equality on second key):'
SELECT count(*) FROM bench_q3 WHERE k1 >= '2025-01-01' AND k2 = 42;  -- warmup
\timing on
SELECT count(*) FROM bench_q3 WHERE k1 >= '2025-01-01' AND k2 = 42;
SELECT count(*) FROM bench_q3 WHERE k1 >= '2025-01-01' AND k2 = 42;
SELECT count(*) FROM bench_q3 WHERE k1 >= '2025-01-01' AND k2 = 42;
\timing off

DROP INDEX bench_q3_btree;

-- SMOL
\echo '  Building SMOL index...'
\timing on
CREATE INDEX bench_q3_smol ON bench_q3 USING smol(k1, k2);
\timing off
\echo '  Index size:'
SELECT pg_size_pretty(pg_relation_size('bench_q3_smol'));

\echo '  SMOL query (range + equality on second key):'
SELECT count(*) FROM bench_q3 WHERE k1 >= '2025-01-01' AND k2 = 42;  -- warmup
\timing on
SELECT count(*) FROM bench_q3 WHERE k1 >= '2025-01-01' AND k2 = 42;
SELECT count(*) FROM bench_q3 WHERE k1 >= '2025-01-01' AND k2 = 42;
SELECT count(*) FROM bench_q3 WHERE k1 >= '2025-01-01' AND k2 = 42;
\timing off

\echo ''
\echo '=== Quick benchmark complete ==='
