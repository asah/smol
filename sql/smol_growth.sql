-- Test growth beyond threshold (line 4397 - linear growth path)
-- Uses smol.growth_threshold_test to reduce the threshold for testing
CREATE EXTENSION IF NOT EXISTS smol;

-- Set threshold to 16384 (16K) instead of 8M for testing
-- Growth: 1024 -> 2048 -> 4096 -> 8192 -> 16384 -> 18432 (+2M but we'll use 20000)
SET smol.growth_threshold_test = 16384;

-- Create a table with 20K rows (enough to trigger linear growth)
DROP TABLE IF EXISTS t_growth CASCADE;
CREATE UNLOGGED TABLE t_growth (k int4, v int4);
INSERT INTO t_growth SELECT i, i*2 FROM generate_series(1, 20000) i;

-- Create SMOL index (should trigger line 4397 during collection/growth)
CREATE INDEX idx_growth ON t_growth USING smol(k) INCLUDE (v);

-- Verify index works
SELECT count(*) FROM t_growth WHERE k > 15000;
SELECT sum(v::int8) FROM t_growth WHERE k <= 100;

-- Reset GUC
SET smol.growth_threshold_test = 0;

-- Cleanup
DROP TABLE t_growth CASCADE;
