-- BRC-style benchmark: smallint x 2, compare BTREE vs SMOL
-- Workload: SELECT sum(x) FROM tmp_brc WHERE y > 5000

\echo '=== BRC BENCHMARK (SMALLINT, configurable rows) ==='
-- Optional: pass number of rows with -v rows=NNN (default 1000000)
\if :{?rows}
\else
\set rows 1000000
\endif

SET client_min_messages = warning;
SET search_path = public;
CREATE EXTENSION IF NOT EXISTS smol;

-- planner settings for IOS
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;

-- data
DROP TABLE IF EXISTS tmp_brc CASCADE;
CREATE TABLE tmp_brc (x int2, y int2);
INSERT INTO tmp_brc
SELECT (random()*32767)::int2, (random()*32767)::int2
FROM generate_series(1, :rows);
ANALYZE tmp_brc;
ALTER TABLE tmp_brc SET (autovacuum_enabled = off);

\echo 'Rows and table size:'
SELECT count(*) AS rows, pg_size_pretty(pg_relation_size('tmp_brc')) AS table_size FROM tmp_brc;

-- BTREE
\echo '--- BTREE index (ensure VM all-visible) ---'
CREATE INDEX tmp_brc_btree_idx ON tmp_brc (y, x);
-- Encourage true IOS by marking heap pages all-visible when possible
CHECKPOINT;
SET vacuum_freeze_min_age = 0;
SET vacuum_freeze_table_age = 0;
VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) tmp_brc;
SELECT 'BTREE size' AS metric, pg_size_pretty(pg_relation_size('tmp_brc_btree_idx')) AS size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(x::bigint) FROM tmp_brc WHERE y > 5000;
\timing off
DROP INDEX tmp_brc_btree_idx;

-- SMOL
\echo '--- SMOL index ---'
CREATE INDEX tmp_brc_smol_idx ON tmp_brc USING smol (y, x);
SELECT 'SMOL size' AS metric, pg_size_pretty(pg_relation_size('tmp_brc_smol_idx')) AS size;
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(x::bigint) FROM tmp_brc WHERE y > 5000;
\timing off

-- cleanup large table
DROP TABLE tmp_brc CASCADE;
