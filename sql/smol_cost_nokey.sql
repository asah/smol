-- Test cost estimation for query without leading key constraint (line 2701)
SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

DROP TABLE IF EXISTS t_cost CASCADE;
CREATE TABLE t_cost (a int4, b int4, c int4);
INSERT INTO t_cost SELECT i, i*2, i*3 FROM generate_series(1, 1000) i;

-- Create TWO-column index (SMOL supports up to 2 key columns)
CREATE INDEX t_cost_idx ON t_cost USING smol(a, b);
ANALYZE t_cost;

-- Query that has WHERE clause on second key column only (not on first column 'a')
-- This should create an index clause but with indexcol != 0, triggering line 2701
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN SELECT * FROM t_cost WHERE b > 500;

-- Cleanup
DROP TABLE t_cost CASCADE;
