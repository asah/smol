-- Test rescan with buffer pin cleanup
-- Targets lines 1263-1268: ReleaseBuffer during rescan

CREATE EXTENSION IF NOT EXISTS smol;

SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

-- Create table with index
DROP TABLE IF EXISTS t_rescan CASCADE;
CREATE UNLOGGED TABLE t_rescan (k int4, v int4);
INSERT INTO t_rescan SELECT i, i*10 FROM generate_series(1, 10000) i;
CREATE INDEX idx_rescan ON t_rescan USING smol(k);

-- Use a cursor to initiate a scan, which will pin a buffer
BEGIN;
DECLARE c1 CURSOR FOR SELECT k, v FROM t_rescan WHERE k >= 100;

-- Fetch some rows (this will pin a buffer in the scan state)
FETCH 5 FROM c1;

-- Close and reopen cursor (this should trigger rescan which releases buffer)
CLOSE c1;
DECLARE c1 CURSOR FOR SELECT k, v FROM t_rescan WHERE k >= 200;
FETCH 5 FROM c1;

CLOSE c1;
COMMIT;

-- Try with nested loop join that might trigger rescan
DROP TABLE IF EXISTS t_outer CASCADE;
CREATE UNLOGGED TABLE t_outer (id int4);
INSERT INTO t_outer VALUES (100), (200), (300);

-- Nested loop where inner side gets rescanned
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_material = off;

EXPLAIN (COSTS OFF) SELECT t_outer.id, t_rescan.k
FROM t_outer
JOIN t_rescan ON t_rescan.k = t_outer.id;

SELECT t_outer.id, t_rescan.k
FROM t_outer
JOIN t_rescan ON t_rescan.k = t_outer.id
ORDER BY t_outer.id;

-- Cleanup
DROP TABLE t_rescan CASCADE;
DROP TABLE t_outer CASCADE;
