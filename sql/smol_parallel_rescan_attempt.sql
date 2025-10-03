-- Attempt to trigger smol_parallelrescan (lines 2486-2493)
-- This is extremely rare - amparallelrescan is called when a parallel
-- index scan is rescanned during execution
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Setup
DROP TABLE IF EXISTS t_outer CASCADE;
DROP TABLE IF EXISTS t_inner CASCADE;

CREATE TABLE t_outer (id int4);
INSERT INTO t_outer SELECT i FROM generate_series(1, 10) i;

CREATE TABLE t_inner (k int4, v int4);
INSERT INTO t_inner SELECT i, i*10 FROM generate_series(1, 100000) i;
CREATE INDEX t_inner_smol ON t_inner USING smol(k);
ANALYZE t_outer;
ANALYZE t_inner;

-- Force parallel settings
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_index_scan_size = 0;
SET enable_seqscan = off;
SET enable_material = off;

-- Try: VALUES clause that might cause parallel scan with rescan behavior
-- Each VALUES row triggers evaluation of the subquery
EXPLAIN (ANALYZE, COSTS OFF)
SELECT v.id, (
    SELECT count(*) FROM t_inner WHERE k > v.id * 1000
)
FROM (VALUES (1), (2), (3), (4), (5)) v(id);

SELECT v.id, (
    SELECT count(*) FROM t_inner WHERE k > v.id * 1000
)
FROM (VALUES (1), (2), (3), (4), (5)) v(id);

-- Cleanup
DROP TABLE t_outer CASCADE;
DROP TABLE t_inner CASCADE;
