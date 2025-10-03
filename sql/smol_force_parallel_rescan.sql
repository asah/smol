-- Aggressive attempt to force parallel rescan (lines 2486-2493)
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Force parallel execution settings
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET min_parallel_index_scan_size = 0;

-- Large inner table to encourage parallel scan
DROP TABLE IF EXISTS t_inner CASCADE;
CREATE TABLE t_inner (k int4, v int4);
INSERT INTO t_inner SELECT i, i*10 FROM generate_series(1, 100000) i;
CREATE INDEX t_inner_smol ON t_inner USING smol(k);
ANALYZE t_inner;

-- Small outer table
DROP TABLE IF EXISTS t_outer CASCADE;
CREATE TABLE t_outer (id int4);
INSERT INTO t_outer VALUES (1), (2), (3), (4), (5);
ANALYZE t_outer;

-- Force nested loop, disable other joins
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;

-- Try to force parallel index scan on inner side
-- Each outer row should trigger a rescan of the parallel scan
EXPLAIN (COSTS OFF, VERBOSE)
SELECT t_outer.id, COUNT(*)
FROM t_outer
CROSS JOIN LATERAL (
    SELECT * FROM t_inner WHERE k > t_outer.id * 1000 LIMIT 100
) sub
GROUP BY t_outer.id;

-- Execute it
SELECT t_outer.id, COUNT(*)
FROM t_outer
CROSS JOIN LATERAL (
    SELECT * FROM t_inner WHERE k > t_outer.id * 1000 LIMIT 100
) sub
GROUP BY t_outer.id
ORDER BY t_outer.id;

-- Alternative: nested loop with parallel scan
SET enable_indexscan = off;
SET enable_seqscan = on;

EXPLAIN (COSTS OFF)
SELECT COUNT(*)
FROM t_outer, t_inner
WHERE t_inner.k BETWEEN t_outer.id * 10000 AND (t_outer.id + 1) * 10000;

SELECT COUNT(*)
FROM t_outer, t_inner  
WHERE t_inner.k BETWEEN t_outer.id * 10000 AND (t_outer.id + 1) * 10000;

-- Cleanup
DROP TABLE t_inner CASCADE;
DROP TABLE t_outer CASCADE;
