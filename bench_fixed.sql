-- Combined fixed-width benchmarks: SMALLINT/INT4 × Uniprocessor/Parallel
-- Each sub-benchmark runs BTREE (IOS with VM) vs SMOL back-to-back

\echo '=== FIXED-WIDTH BENCHMARKS (COMBINED) ==='
-- Optional: pass row counts:
--   -v rows_si=NNN (default 1000000) for SMALLINT
--   -v rows_i4=NNN (default 1000000) for INT4
\if :{?rows_si}
\else
\set rows_si 1000000
\endif
\if :{?rows_i4}
\else
\set rows_i4 1000000
\endif
-- Optional: parallel worker counts
--   -v par_workers_si=K (default 2) for SMALLINT parallel
--   -v par_workers_i4=K (default 2) for INT4 parallel
\if :{?par_workers_si}
\else
\set par_workers_si 2
\endif
\if :{?par_workers_i4}
\else
\set par_workers_i4 2
\endif
SET client_min_messages = warning;
SET search_path = public;
CREATE EXTENSION IF NOT EXISTS smol;

-- Helper to prefer IOS
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;

-- 1) SMALLINT — Uniprocessor
\echo ''
\echo '--- SMALLINT (UNIPROCESSOR) ---'
SET max_parallel_workers_per_gather = 0;
DROP TABLE IF EXISTS fw_si CASCADE;
SET synchronous_commit = off;
SET maintenance_work_mem = '2GB';
SET work_mem = '256MB';

CREATE UNLOGGED TABLE fw_si (a int2, b int2);
INSERT INTO fw_si SELECT (random()*32767)::int2, (random()*32767)::int2 FROM generate_series(1, :rows_si);
ANALYZE fw_si;
ALTER TABLE fw_si SET (autovacuum_enabled = off);
-- BTREE (ensure VM all-visible)
CREATE INDEX fw_si_btree ON fw_si (b,a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) fw_si;
SELECT 'BTREE size (si uni)' AS metric, pg_size_pretty(pg_relation_size('fw_si_btree')) AS idx_size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM fw_si WHERE b>5000;
\timing off
DROP INDEX fw_si_btree;
-- SMOL
CREATE INDEX fw_si_smol ON fw_si USING smol (b,a);
SELECT 'SMOL size (si uni)' AS metric, pg_size_pretty(pg_relation_size('fw_si_smol')) AS idx_size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM fw_si WHERE b>5000;
\timing off
DROP TABLE fw_si CASCADE;

-- 2) SMALLINT — Parallel
\echo ''
\echo '--- SMALLINT (PARALLEL) ---'
SET max_parallel_workers_per_gather = :par_workers_si;
SET min_parallel_index_scan_size = 0; SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0;
DROP TABLE IF EXISTS fw_si_p CASCADE;
CREATE UNLOGGED TABLE fw_si_p (a int2, b int2);
INSERT INTO fw_si_p SELECT (random()*32767)::int2, (random()*32767)::int2 FROM generate_series(1, :rows_si);
ANALYZE fw_si_p;
ALTER TABLE fw_si_p SET (autovacuum_enabled = off);
-- BTREE (ensure VM all-visible)
CREATE INDEX fw_si_p_btree ON fw_si_p (b,a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) fw_si_p;
SELECT 'BTREE size (si par)' AS metric, pg_size_pretty(pg_relation_size('fw_si_p_btree')) AS idx_size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM fw_si_p WHERE b>5000;
\timing off
DROP INDEX fw_si_p_btree;
-- SMOL
CREATE INDEX fw_si_p_smol ON fw_si_p USING smol (b,a);
SELECT 'SMOL size (si par)' AS metric, pg_size_pretty(pg_relation_size('fw_si_p_smol')) AS idx_size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM fw_si_p WHERE b>5000;
\timing off
DROP TABLE fw_si_p CASCADE;

-- 3) INT4 — Uniprocessor
\echo ''
\echo '--- INT4 (UNIPROCESSOR) ---'
SET max_parallel_workers_per_gather = 0;
DROP TABLE IF EXISTS fw_i4 CASCADE;
CREATE UNLOGGED TABLE fw_i4 (a int4, b int4);
INSERT INTO fw_i4 SELECT (random()*1000000)::int, (random()*1000000)::int FROM generate_series(1, :rows_i4);
ANALYZE fw_i4;
ALTER TABLE fw_i4 SET (autovacuum_enabled = off);
-- BTREE (ensure VM all-visible)
CREATE INDEX fw_i4_btree ON fw_i4 (b,a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) fw_i4;
SELECT 'BTREE size (i4 uni)' AS metric, pg_size_pretty(pg_relation_size('fw_i4_btree')) AS idx_size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM fw_i4 WHERE b>500000;
\timing off
DROP INDEX fw_i4_btree;
-- SMOL
CREATE INDEX fw_i4_smol ON fw_i4 USING smol (b,a);
SELECT 'SMOL size (i4 uni)' AS metric, pg_size_pretty(pg_relation_size('fw_i4_smol')) AS idx_size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM fw_i4 WHERE b>500000;
\timing off
DROP TABLE fw_i4 CASCADE;

-- 4) INT4 — Parallel
\echo ''
\echo '--- INT4 (PARALLEL) ---'
SET max_parallel_workers_per_gather = :par_workers_i4;
SET min_parallel_index_scan_size = 0; SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0;
DROP TABLE IF EXISTS fw_i4_p CASCADE;
CREATE UNLOGGED TABLE fw_i4_p (a int4, b int4);
INSERT INTO fw_i4_p SELECT (random()*1000000)::int, (random()*1000000)::int FROM generate_series(1, :rows_i4);
ANALYZE fw_i4_p;
ALTER TABLE fw_i4_p SET (autovacuum_enabled = off);
-- BTREE (ensure VM all-visible)
CREATE INDEX fw_i4_p_btree ON fw_i4_p (b,a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) fw_i4_p;
SELECT 'BTREE size (i4 par)' AS metric, pg_size_pretty(pg_relation_size('fw_i4_p_btree')) AS idx_size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM fw_i4_p WHERE b>500000;
\timing off
DROP INDEX fw_i4_p_btree;
-- SMOL
CREATE INDEX fw_i4_p_smol ON fw_i4_p USING smol (b,a);
-- SELECT 'SMOL size (i4 par)' AS metric, pg_size_pretty(pg_relation_size('fw_i4_p_smol')) AS idx_size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM fw_i4_p WHERE b>500000;
\timing off
DROP TABLE fw_i4_p CASCADE;

\echo ''
\echo '=== FIXED-WIDTH BENCHMARKS COMPLETE ==='
