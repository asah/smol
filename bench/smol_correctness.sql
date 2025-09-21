-- Correctness check: compare BTREE vs SMOL sums for two integer types
-- Parameters:
--   rows_si : rows for SMALLINT (default 5000000)
--   rows_i4 : rows for INT4 (default 2500000)

\if :{?rows_si}
\else
\set rows_si 5000000
\endif
\if :{?rows_i4}
\else
\set rows_i4 2500000
\endif

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET min_parallel_index_scan_size = 0;
SET max_parallel_workers_per_gather = 0;  -- uniprocessor for correctness

-- SMALLINT correctness
DROP TABLE IF EXISTS cc_si CASCADE;
CREATE UNLOGGED TABLE cc_si(a int2, b int2);
INSERT INTO cc_si SELECT (random()*32767)::int2, (random()*32767)::int2 FROM generate_series(1, :rows_si);
ANALYZE cc_si;
ALTER TABLE cc_si SET (autovacuum_enabled = off);

-- BTREE sum
DROP INDEX IF EXISTS cc_si_btree;
CREATE INDEX cc_si_btree ON cc_si(b,a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) cc_si;
SELECT sum(a::bigint) AS sum_btree_si FROM cc_si WHERE b > 5000;\gset

-- SMOL sum
DROP INDEX cc_si_btree;
CREATE INDEX cc_si_smol ON cc_si USING smol(b,a);
SELECT sum(a::bigint) AS sum_smol_si FROM cc_si WHERE b > 5000;\gset

-- Compare
SELECT (:sum_btree_si)::bigint AS sum_btree_si, (:sum_smol_si)::bigint AS sum_smol_si,
       ((:sum_btree_si)::bigint = (:sum_smol_si)::bigint) AS smallint_match;
DROP TABLE cc_si CASCADE;

-- INT4 correctness
DROP TABLE IF EXISTS cc_i4 CASCADE;
CREATE UNLOGGED TABLE cc_i4(a int4, b int4);
INSERT INTO cc_i4 SELECT (random()*1000000)::int, (random()*1000000)::int FROM generate_series(1, :rows_i4);
ANALYZE cc_i4;
ALTER TABLE cc_i4 SET (autovacuum_enabled = off);

-- BTREE sum
DROP INDEX IF EXISTS cc_i4_btree;
CREATE INDEX cc_i4_btree ON cc_i4(b,a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) cc_i4;
SELECT sum(a::bigint) AS sum_btree_i4 FROM cc_i4 WHERE b > 500000;\gset

-- SMOL sum
DROP INDEX cc_i4_btree;
CREATE INDEX cc_i4_smol ON cc_i4 USING smol(b,a);
SELECT sum(a::bigint) AS sum_smol_i4 FROM cc_i4 WHERE b > 500000;\gset

-- Compare
SELECT (:sum_btree_i4)::bigint AS sum_btree_i4, (:sum_smol_i4)::bigint AS sum_smol_i4,
       ((:sum_btree_i4)::bigint = (:sum_smol_i4)::bigint) AS int4_match;
DROP TABLE cc_i4 CASCADE;

