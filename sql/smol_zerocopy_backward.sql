-- Test zero-copy backward scan (lines 2303, 2590)
-- Covers keyp adjustment for zero-copy pages in backward scans

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET enable_indexscan = off;

-- Create table with unique keys to trigger zero-copy format
DROP TABLE IF EXISTS t_zerocopy_bwd CASCADE;
CREATE UNLOGGED TABLE t_zerocopy_bwd (k int4 PRIMARY KEY);

-- Insert unique sequential values (triggers zero-copy format)
INSERT INTO t_zerocopy_bwd SELECT i FROM generate_series(1, 10000) i;

ALTER TABLE t_zerocopy_bwd SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) t_zerocopy_bwd;

-- Create SMOL index
CREATE INDEX t_zerocopy_bwd_smol ON t_zerocopy_bwd USING smol(k);

-- VACUUM again to update visibility map for index-only scans
VACUUM (ANALYZE) t_zerocopy_bwd;

-- Verify zero-copy format is used
SELECT zerocopy_pages > 0 AS has_zerocopy FROM smol_inspect('t_zerocopy_bwd_smol');

-- Test both forward and backward scans with zero-copy pages
-- Line 2303: Forward scan initial seek with lower bound (requires have_bound && dir != Backward)
-- Line 2590: Backward scan upper bound check (requires have_upper_bound && dir == Backward)

-- FORWARD scan with lower bound (triggers line 2303: binary search for >= bound)
EXPLAIN (COSTS OFF) SELECT k FROM t_zerocopy_bwd WHERE k > 5000 ORDER BY k LIMIT 5;
SELECT k FROM t_zerocopy_bwd WHERE k > 5000 ORDER BY k LIMIT 5;

-- FORWARD scan with >= bound
SELECT k FROM t_zerocopy_bwd WHERE k >= 7500 LIMIT 10;

-- BACKWARD scan with upper bound < (triggers line 2590: upper bound check during iteration)
EXPLAIN (COSTS OFF) SELECT k FROM t_zerocopy_bwd WHERE k < 5000 ORDER BY k DESC LIMIT 5;
SELECT k FROM t_zerocopy_bwd WHERE k < 5000 ORDER BY k DESC LIMIT 5;

-- BACKWARD scan with <= bound
SELECT k FROM t_zerocopy_bwd WHERE k <= 9500 ORDER BY k DESC LIMIT 10;

-- Backward scan with range (both bounds, triggers both lines)
SELECT k FROM t_zerocopy_bwd WHERE k > 1000 AND k < 2000 ORDER BY k DESC LIMIT 10;

-- Verify correctness
SELECT count(*) FROM t_zerocopy_bwd WHERE k < 5000;
SELECT count(*) FROM t_zerocopy_bwd WHERE k <= 9500;
SELECT count(*) FROM t_zerocopy_bwd WHERE k > 1000 AND k < 2000;

-- Cleanup
DROP TABLE t_zerocopy_bwd CASCADE;
