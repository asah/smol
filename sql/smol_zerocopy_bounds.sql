-- Test zero-copy format with bounds checking
-- Covers lines 981, 993-994, 1006-1007 in smol_page_matches_scan_bounds

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Force zero-copy format for this test
SET smol.enable_zero_copy = on;

-- Create table with single-column index (zero-copy format)
DROP TABLE IF EXISTS t_zerocopy_bounds CASCADE;
CREATE UNLOGGED TABLE t_zerocopy_bounds (k int4, data text);

-- Insert data to create multiple pages with zero-copy format
-- Zero-copy is used when smol.enable_zero_copy = on
INSERT INTO t_zerocopy_bounds
SELECT i, 'data' || i
FROM generate_series(1, 50000) i;

CREATE INDEX t_zerocopy_bounds_idx ON t_zerocopy_bounds USING smol(k);

-- Verify zero-copy format is used
SELECT zerocopy_pct > 50 AS uses_zero_copy FROM smol_inspect('t_zerocopy_bounds_idx'::regclass);

-- Verify multiple pages
SELECT total_pages > 1 AS has_multiple_pages, leaf_pages > 1 AS has_multiple_leaves
FROM smol_inspect('t_zerocopy_bounds_idx'::regclass);

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = on;

-- Test 1: Upper bound check (lines 987-994)
-- Query with upper bound that stops mid-index
SELECT count(*) FROM t_zerocopy_bounds WHERE k < 10000;
SELECT count(*) FROM t_zerocopy_bounds WHERE k <= 25000;

-- Test 2: Upper bound with strict inequality (line 990)
SELECT count(*) FROM t_zerocopy_bounds WHERE k < 30000;

-- Test 3: Non-strict upper bound
SELECT count(*) FROM t_zerocopy_bounds WHERE k <= 35000;

-- Test 4: Equality bound check (lines 1000-1007)
-- These should stop scanning once k exceeds the target
SELECT count(*) FROM t_zerocopy_bounds WHERE k = 1000;
SELECT count(*) FROM t_zerocopy_bounds WHERE k = 25000;
SELECT count(*) FROM t_zerocopy_bounds WHERE k = 49000;

-- Test 5: Range queries that span multiple pages
SELECT count(*) FROM t_zerocopy_bounds WHERE k >= 10000 AND k < 20000;
SELECT count(*) FROM t_zerocopy_bounds WHERE k >= 30000 AND k <= 40000;

-- Test 6: Queries that should trigger stop_scan when first key exceeds bound
-- This creates a scenario where page bounds checking stops the scan early
SELECT count(*) FROM t_zerocopy_bounds WHERE k BETWEEN 5000 AND 5100;
SELECT count(*) FROM t_zerocopy_bounds WHERE k >= 40000 AND k < 40100;

-- Test 7: Query with very restrictive upper bound to trigger early page stop
-- Force page bounds check by using a bound that falls in middle of index
SELECT count(*) FROM t_zerocopy_bounds WHERE k < 100;
SELECT count(*) FROM t_zerocopy_bounds WHERE k <= 50;
SELECT count(*) FROM t_zerocopy_bounds WHERE k = 1;

-- Reset GUC
RESET smol.enable_zero_copy;

-- Cleanup
DROP TABLE t_zerocopy_bounds CASCADE;
