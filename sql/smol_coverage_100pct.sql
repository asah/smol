-- Test to achieve 100% code coverage for SMOL
-- Targets remaining uncovered lines identified in COVERAGE_STATUS.md

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- Test 1: Empty two-column index (lines 1490-1496)
-- ============================================================================
DROP TABLE IF EXISTS t_empty_twocol CASCADE;
CREATE UNLOGGED TABLE t_empty_twocol (a int4, b int4);
-- Create index on empty table with two columns
CREATE INDEX t_empty_twocol_idx ON t_empty_twocol USING smol(a, b);
-- Query empty index
SELECT * FROM t_empty_twocol WHERE a > 0;
DROP TABLE t_empty_twocol CASCADE;

-- ============================================================================
-- Test 2: Empty page detection (lines 943, 947)
-- No upper bounds path (line 943)
-- ============================================================================
DROP TABLE IF EXISTS t_no_bounds CASCADE;
CREATE UNLOGGED TABLE t_no_bounds (k int4);
INSERT INTO t_no_bounds SELECT i FROM generate_series(1, 100) i;
CREATE INDEX t_no_bounds_idx ON t_no_bounds USING smol(k);
-- Query with no upper bound (only lower bound)
SELECT count(*) FROM t_no_bounds WHERE k >= 50;
DROP TABLE t_no_bounds CASCADE;

-- ============================================================================
-- Test 3: NULL key handling in rescan (lines 2056, 2061, 2066-2068)
-- ============================================================================
DROP TABLE IF EXISTS t_nulltest CASCADE;
CREATE UNLOGGED TABLE t_nulltest (k int4);
INSERT INTO t_nulltest SELECT i FROM generate_series(1, 10) i;
CREATE INDEX t_nulltest_idx ON t_nulltest USING smol(k);

-- Try to query with potentially NULL scan keys
-- These should be handled by planner or runtime key evaluation
SELECT * FROM t_nulltest WHERE k = CASE WHEN random() < 0 THEN NULL ELSE 5 END;
SELECT * FROM t_nulltest WHERE k IN (1, 2, 3) AND k >= 2;

DROP TABLE t_nulltest CASCADE;

-- ============================================================================
-- Test 4: Zero-copy paths (lines 2303, 2590, 2760-2778)
-- Zero-copy with plain page and ultra-fast path
-- ============================================================================
DROP TABLE IF EXISTS t_zerocopy CASCADE;
CREATE UNLOGGED TABLE t_zerocopy (k int4) WITH (autovacuum_enabled = off);
-- Insert unique values to get plain zero-copy pages
INSERT INTO t_zerocopy SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX t_zerocopy_idx ON t_zerocopy USING smol(k);

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;

-- Query to trigger zero-copy ultra-fast path
SELECT count(*) FROM t_zerocopy WHERE k >= 100;
SELECT count(*) FROM t_zerocopy WHERE k >= 500 AND k <= 700;

DROP TABLE t_zerocopy CASCADE;

-- ============================================================================
-- Test 5: Backward scan with zero-copy (line 2590)
-- ============================================================================
DROP TABLE IF EXISTS t_backward_zerocopy CASCADE;
CREATE UNLOGGED TABLE t_backward_zerocopy (k int4);
INSERT INTO t_backward_zerocopy SELECT i FROM generate_series(1, 100) i;
CREATE INDEX t_backward_zerocopy_idx ON t_backward_zerocopy USING smol(k);

-- Backward scan (ORDER BY DESC) should trigger line 2590
SELECT k FROM t_backward_zerocopy WHERE k <= 50 ORDER BY k DESC LIMIT 10;

DROP TABLE t_backward_zerocopy CASCADE;

-- ============================================================================
-- Test 6: RLE INCLUDE cached pointer paths (lines 2690-2702, 2721-2725)
-- ============================================================================
DROP TABLE IF EXISTS t_rle_inc_paths CASCADE;
CREATE UNLOGGED TABLE t_rle_inc_paths (k int4, v1 int4, v2 int8, v3 int2);
-- Insert duplicates to trigger RLE
INSERT INTO t_rle_inc_paths SELECT i % 10, i, i*2, i%100 FROM generate_series(1, 1000) i;
CREATE INDEX t_rle_inc_paths_idx ON t_rle_inc_paths USING smol(k) INCLUDE (v1, v2, v3);

-- Query to access RLE INCLUDE columns
SELECT k, v1, v2, v3 FROM t_rle_inc_paths WHERE k = 5 LIMIT 10;
SELECT k, v1 FROM t_rle_inc_paths WHERE k >= 3 AND k <= 7 LIMIT 20;

DROP TABLE t_rle_inc_paths CASCADE;

-- ============================================================================
-- Test 7: Backward scan with INCLUDE columns (lines 2690-2702)
-- ============================================================================
DROP TABLE IF EXISTS t_backward_inc CASCADE;
CREATE UNLOGGED TABLE t_backward_inc (k int4, v int4);
INSERT INTO t_backward_inc SELECT i, i*10 FROM generate_series(1, 100) i;
CREATE INDEX t_backward_inc_idx ON t_backward_inc USING smol(k) INCLUDE (v);

-- Backward scan with INCLUDE columns
SELECT k, v FROM t_backward_inc WHERE k <= 50 ORDER BY k DESC LIMIT 10;

DROP TABLE t_backward_inc CASCADE;

-- ============================================================================
-- Test 8: Runtime key filtering paths (lines 2737-2740, 2985-2992)
-- ============================================================================
DROP TABLE IF EXISTS t_runtime_keys CASCADE;
CREATE UNLOGGED TABLE t_runtime_keys (k int4, v int4);
INSERT INTO t_runtime_keys SELECT i, i*10 FROM generate_series(1, 100) i;
CREATE INDEX t_runtime_keys_idx ON t_runtime_keys USING smol(k) INCLUDE (v);

-- Queries that might trigger runtime key evaluation
SELECT * FROM t_runtime_keys WHERE k = 50;
SELECT * FROM t_runtime_keys WHERE k IN (10, 20, 30);
SELECT * FROM t_runtime_keys WHERE k BETWEEN 25 AND 75;

-- Prepare statement with parameters (runtime keys)
PREPARE stmt_runtime AS SELECT * FROM t_runtime_keys WHERE k = $1;
EXECUTE stmt_runtime(42);
EXECUTE stmt_runtime(99);
DEALLOCATE stmt_runtime;

DROP TABLE t_runtime_keys CASCADE;

-- ============================================================================
-- Test 9: Parallel scan exhaustion (lines 3049-3084, 3255-3256)
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_exhaust CASCADE;
CREATE UNLOGGED TABLE t_parallel_exhaust (k int4);
INSERT INTO t_parallel_exhaust SELECT i FROM generate_series(1, 100000) i;
CREATE INDEX t_parallel_exhaust_idx ON t_parallel_exhaust USING smol(k);

-- Force parallel scan
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_index_scan_size = 0;

-- Scan all rows to exhaust parallel scan
SELECT count(*) FROM t_parallel_exhaust WHERE k >= 1;

-- Reset parallel settings
SET max_parallel_workers_per_gather = 0;

DROP TABLE t_parallel_exhaust CASCADE;

-- ============================================================================
-- Test 10: Equality bound stop scan (lines 980-981)
-- ============================================================================
DROP TABLE IF EXISTS t_eq_bound CASCADE;
CREATE UNLOGGED TABLE t_eq_bound (k int4);
-- Insert data where equality boundary aligns with page boundary
INSERT INTO t_eq_bound SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX t_eq_bound_idx ON t_eq_bound USING smol(k);

-- Equality query that should stop scan early
SELECT * FROM t_eq_bound WHERE k = 5000;
SELECT count(*) FROM t_eq_bound WHERE k = 1;

DROP TABLE t_eq_bound CASCADE;

-- ============================================================================
-- Test 11: Two-column index with attribute 2 equality (line 2056)
-- ============================================================================
DROP TABLE IF EXISTS t_twocol_eq CASCADE;
CREATE UNLOGGED TABLE t_twocol_eq (k1 int4, k2 int4, v int4);
INSERT INTO t_twocol_eq SELECT i, i % 100, i*10 FROM generate_series(1, 1000) i;
CREATE INDEX t_twocol_eq_idx ON t_twocol_eq USING smol(k1, k2) INCLUDE (v);

-- Query with k1 range and k2 equality
SELECT k1, k2, v FROM t_twocol_eq WHERE k1 >= 100 AND k2 = 50 LIMIT 10;

DROP TABLE t_twocol_eq CASCADE;

-- ============================================================================
-- Test 12: First key extraction from zero-copy page (line 955)
-- ============================================================================
DROP TABLE IF EXISTS t_zerocopy_firstkey CASCADE;
CREATE UNLOGGED TABLE t_zerocopy_firstkey (k int4);
INSERT INTO t_zerocopy_firstkey SELECT i FROM generate_series(1, 5000) i;
CREATE INDEX t_zerocopy_firstkey_idx ON t_zerocopy_firstkey USING smol(k);

-- Query that causes page filtering with zero-copy pages
SELECT count(*) FROM t_zerocopy_firstkey WHERE k >= 2500 AND k <= 3500;

DROP TABLE t_zerocopy_firstkey CASCADE;

-- ============================================================================
-- Test 13: Zero-copy itup pointer path (lines 2996-3001)
-- ============================================================================
DROP TABLE IF EXISTS t_zerocopy_itup CASCADE;
CREATE UNLOGGED TABLE t_zerocopy_itup (k int4);
INSERT INTO t_zerocopy_itup SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX t_zerocopy_itup_idx ON t_zerocopy_itup USING smol(k);

-- Query to trigger zero-copy itup pointer restoration
SELECT * FROM t_zerocopy_itup WHERE k >= 100 AND k <= 200;

DROP TABLE t_zerocopy_itup CASCADE;

-- ============================================================================
-- Test 14: Rescan paths (line 2061, 3255-3256, 3392-3395)
-- ============================================================================
DROP TABLE IF EXISTS t_rescan CASCADE;
CREATE UNLOGGED TABLE t_rescan (k int4, v int4);
INSERT INTO t_rescan SELECT i, i*10 FROM generate_series(1, 100) i;
CREATE INDEX t_rescan_idx ON t_rescan USING smol(k) INCLUDE (v);

-- Nested loop join to trigger index rescan
DROP TABLE IF EXISTS t_outer CASCADE;
CREATE UNLOGGED TABLE t_outer (k int4);
INSERT INTO t_outer VALUES (10), (20), (30);

SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_nestloop = on;

-- This should rescan the index for each outer row
SELECT t_outer.k, t_rescan.k, t_rescan.v
FROM t_outer, t_rescan
WHERE t_rescan.k = t_outer.k;

DROP TABLE t_rescan CASCADE;
DROP TABLE t_outer CASCADE;

-- ============================================================================
-- Test 15: Parallel scan with two-column index (lines 2303, 3050-3084)
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_twocol CASCADE;
CREATE UNLOGGED TABLE t_parallel_twocol (k1 int4, k2 int4);
INSERT INTO t_parallel_twocol SELECT i, i*2 FROM generate_series(1, 100000) i;
CREATE INDEX t_parallel_twocol_idx ON t_parallel_twocol USING smol(k1, k2);

SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_index_scan_size = 0;

-- Parallel scan on two-column index
SELECT count(*) FROM t_parallel_twocol WHERE k1 >= 1000;

SET max_parallel_workers_per_gather = 0;

DROP TABLE t_parallel_twocol CASCADE;

-- ============================================================================
-- Test 16: Backward scan runtime key rejection (lines 2737-2740)
-- ============================================================================
DROP TABLE IF EXISTS t_backward_filter CASCADE;
CREATE UNLOGGED TABLE t_backward_filter (k int4, v int4);
INSERT INTO t_backward_filter SELECT i, i*10 FROM generate_series(1, 100) i;
CREATE INDEX t_backward_filter_idx ON t_backward_filter USING smol(k) INCLUDE (v);

-- Backward scan with filtering
PREPARE stmt_backward AS SELECT k, v FROM t_backward_filter WHERE k <= $1 ORDER BY k DESC LIMIT 10;
EXECUTE stmt_backward(80);
EXECUTE stmt_backward(50);
DEALLOCATE stmt_backward;

DROP TABLE t_backward_filter CASCADE;
