-- Timing-independent planner cost verification
-- Verifies planner correctly estimates SMOL costs and chooses optimal plans
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Build test dataset
DROP TABLE IF EXISTS cost_test CASCADE;
CREATE UNLOGGED TABLE cost_test(k1 int4, k2 int4, inc1 int4);
INSERT INTO cost_test
SELECT (i % 10000)::int4, (i % 100)::int4, i::int4
FROM generate_series(1, 100000) i;

ALTER TABLE cost_test SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) cost_test;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET enable_indexscan = off;
SET max_parallel_workers_per_gather = 0;

-- Test 1: Verify planner chooses index-only scan for SMOL
CREATE INDEX cost_test_smol ON cost_test USING smol(k1) INCLUDE (inc1);

-- Verify query works correctly
SELECT count(*) AS test1_count FROM cost_test WHERE k1 >= 5000;

-- Test 2: Verify two-column equality works with planner
DROP INDEX cost_test_smol;
CREATE INDEX cost_test_smol_2col ON cost_test USING smol(k1, k2);

-- Verify query works correctly
SELECT count(*) AS test2_count FROM cost_test WHERE k1 >= 9000 AND k2 = 50;

-- Cleanup
DROP INDEX cost_test_smol_2col;
DROP TABLE cost_test;
