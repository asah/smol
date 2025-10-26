-- smol_scan.sql: Scanning and navigation tests
-- Consolidates: runtime_keys_coverage, prefetch_boundary, rightmost_descend,
--               parallel, deep_backward_navigation, options_coverage
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- smol_runtime_keys_coverage
-- ============================================================================

-- Test runtime keys (non-native predicates) for coverage
-- Targets lines 2741-2742 (backward), 2770-2780 (forward ultra-fast), 2991-2994 (forward)
-- Runtime keys are needed for multi-column indexes with range predicates on the 2nd column
-- SMOL handles attribute 1 (leading key) natively, but attribute 2 range predicates need rechecking

SET client_min_messages = warning;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

-- Create table with two key columns for multi-column index
DROP TABLE IF EXISTS t_runtime CASCADE;
CREATE UNLOGGED TABLE t_runtime (k1 int4, k2 int4, v int4);

-- Insert data: k1 in 1..100, k2 = k1*10, v = k1*100
INSERT INTO t_runtime SELECT i, i*10, i*100 FROM generate_series(1, 100) i;

ALTER TABLE t_runtime SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) t_runtime;

-- Multi-column index on (k1, k2) with INCLUDE(v)
CREATE INDEX t_runtime_idx ON t_runtime USING smol(k1, k2) INCLUDE (v);

-- Test 1: Forward scan with runtime keys that FAIL (lines 2991-2994)
-- k1=50 matches 1 row (k1=50, k2=500), but k2 > 600 fails, so runtime key returns false
SELECT k1, k2, v FROM t_runtime WHERE k1 = 50 AND k2 > 600;

-- Test 2: Backward scan with runtime keys that FAIL (lines 2741-2742)
-- k1 in 51..60, but k2 < 550 only matches k1=51..54 (k2=510,520,530,540)
-- Backward scan checks k1=60,59,...,51 and runtime keys fail for k1=60..55
SELECT k1, k2 FROM t_runtime WHERE k1 > 50 AND k1 < 61 AND k2 < 550 ORDER BY k1 DESC;

-- Test 3: Forward scan with runtime keys on larger dataset
-- Tests runtime key evaluation with bound checks
DROP TABLE IF EXISTS t_larger CASCADE;
CREATE UNLOGGED TABLE t_larger (k1 int4, k2 int4, v int4);
INSERT INTO t_larger SELECT i, i*10, i*100 FROM generate_series(1, 5000) i;
VACUUM (FREEZE, ANALYZE) t_larger;
CREATE INDEX t_larger_idx ON t_larger USING smol(k1, k2) INCLUDE (v);

-- Query with runtime key k2 > 50000 fails for all rows (max k2 is 50000)
-- Must SELECT columns to force xs_want_itup=true (count(*) doesn't need tuples)
SELECT k1, k2, v FROM t_larger WHERE k1 >= 100 AND k2 > 50000 LIMIT 10;

-- Also test with some runtime key successes
SELECT k1, k2, v FROM t_larger WHERE k1 >= 4990 AND k2 >= 49900 LIMIT 10;

-- Cleanup
DROP TABLE t_runtime CASCADE;
DROP TABLE t_larger CASCADE;

-- ============================================================================
-- smol_prefetch_boundary
-- ============================================================================

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Create a small index so prefetch can reach the boundary
DROP TABLE IF EXISTS t_prefetch_boundary CASCADE;
CREATE UNLOGGED TABLE t_prefetch_boundary (k int4);

-- Insert just enough data to create a few pages
-- With default settings, we need enough to create 3-4 pages
INSERT INTO t_prefetch_boundary SELECT i FROM generate_series(1, 5000) i;

CREATE INDEX idx_prefetch_boundary ON t_prefetch_boundary USING smol(k);

-- Check index size
SELECT total_pages, leaf_pages FROM smol_inspect('idx_prefetch_boundary');

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;

-- Set prefetch depth high to ensure we try to prefetch beyond end
SET smol.prefetch_depth = 8;

-- Do an unbounded forward scan that will trigger aggressive prefetching
-- The prefetch logic at line 3601-3608 will try to prefetch beyond the index boundary
-- This should hit the break at line 3607 when pb >= nblocks
SELECT count(*) FROM t_prefetch_boundary WHERE k >= 1;

-- Try with a scan starting near the end
SELECT count(*) FROM t_prefetch_boundary WHERE k >= 4500;

-- Cleanup
DROP TABLE t_prefetch_boundary CASCADE;

-- ============================================================================
-- smol_rightmost_descend
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS smol;

-- Lines 3517-3518: Forward scan where all keys are < lower_bound
-- This forces choosing rightmost child in find_first_leaf
DROP TABLE IF EXISTS t_rightmost CASCADE;
CREATE TABLE t_rightmost (k int4);

-- Insert enough data to create multi-level tree (height > 1)
INSERT INTO t_rightmost SELECT i FROM generate_series(1, 100000) i;
CREATE INDEX t_rightmost_idx ON t_rightmost USING smol(k);

-- Query with lower bound > all keys forces rightmost child selection
-- When searching for k >= 999999, all internal node keys will be < bound
-- so it chooses rightmost child at each level
SELECT count(*) FROM t_rightmost WHERE k >= 999999;

-- Lines 4107-4113: smol_rightmost_leaf() direct whitebox test
DROP TABLE t_rightmost CASCADE;
CREATE TABLE t_rightmost (k int4);
-- Create multi-level tree (height > 1, 300K rows is sufficient)
INSERT INTO t_rightmost SELECT i FROM generate_series(1, 300000) i;
CREATE INDEX t_rightmost_idx ON t_rightmost USING smol(k);

-- Directly call smol_rightmost_leaf() to trigger lines 4107-4113
SELECT smol_test_rightmost_leaf('t_rightmost_idx'::regclass) AS rightmost_leaf_block;

-- Lines 3517-3518: smol_find_first_leaf() with rightmost child selection
-- Call with bound > all keys to force rightmost child selection at each level
SELECT smol_test_find_first_leaf_rightmost('t_rightmost_idx'::regclass, 999999999::bigint) AS first_leaf_block;

DROP TABLE t_rightmost CASCADE;

-- Test with two-column index for completeness
DROP TABLE IF EXISTS t_rightmost2 CASCADE;
CREATE TABLE t_rightmost2 (k int4, v int4);
INSERT INTO t_rightmost2 SELECT i, i*10 FROM generate_series(1, 100000) i;
CREATE INDEX t_rightmost2_idx ON t_rightmost2 USING smol(k, v);

-- Backward scan on two-column index
SELECT k, v FROM t_rightmost2 WHERE k <= 200000 ORDER BY k DESC, v DESC LIMIT 5;

DROP TABLE t_rightmost2 CASCADE;

-- ============================================================================
-- smol_parallel
-- ============================================================================
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- Parallel Sequential Scan
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_seq CASCADE;
CREATE TABLE t_parallel_seq (k int4, v text);
INSERT INTO t_parallel_seq SELECT i, 'value_' || i FROM generate_series(1, 10000) i;
CREATE INDEX t_parallel_seq_idx ON t_parallel_seq USING smol(k);

SET max_parallel_workers_per_gather = 2;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

-- Force parallel scan
SELECT count(*) FROM t_parallel_seq WHERE k > 5000;
SELECT count(*) FROM t_parallel_seq WHERE k BETWEEN 2000 AND 8000;

-- ============================================================================
-- Parallel Index Scan
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_idx CASCADE;
CREATE TABLE t_parallel_idx (k int4, v int4);
INSERT INTO t_parallel_idx SELECT i, i * 2 FROM generate_series(1, 20000) i;
CREATE INDEX t_parallel_idx_idx ON t_parallel_idx USING smol(k);

SET enable_seqscan = off;

-- Parallel index scan
SELECT count(*) FROM t_parallel_idx WHERE k > 10000;
SELECT sum(v) FROM t_parallel_idx WHERE k BETWEEN 5000 AND 15000;

-- ============================================================================
-- Parallel Index Build
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_build CASCADE;
CREATE TABLE t_parallel_build (k int4);
INSERT INTO t_parallel_build SELECT i FROM generate_series(1, 30000) i;

SET max_parallel_maintenance_workers = 4;

CREATE INDEX t_parallel_build_idx ON t_parallel_build USING smol(k);

SELECT count(*) FROM t_parallel_build WHERE k < 10000;

-- Reset parallel settings
RESET max_parallel_workers_per_gather;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET max_parallel_maintenance_workers;
RESET enable_seqscan;

-- Cleanup
DROP TABLE t_parallel_seq CASCADE;
DROP TABLE t_parallel_idx CASCADE;
DROP TABLE t_parallel_build CASCADE;

-- ============================================================================
-- smol_deep_backward_navigation
-- ============================================================================

SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Force extremely deep tree structure
-- Use very small fanout and very few tuples per page to maximize tree height
SET smol.test_max_tuples_per_page = 20;  -- Very small to create many leaves
SET smol.test_max_internal_fanout = 3;    -- Very small to create tall internal structure

DROP TABLE IF EXISTS t_deep_backward CASCADE;
CREATE UNLOGGED TABLE t_deep_backward (k int4, data text);

-- Insert data in a pattern that will create specific tree structure
-- We need enough data to create height >= 4 with our aggressive GUCs
-- Height 4: 20 * 3 * 3 * 3 = 540 rows minimum
-- Use 5000 rows to ensure height >= 5
INSERT INTO t_deep_backward
SELECT i, 'data' || i
FROM generate_series(1, 5000) i;

CREATE INDEX t_deep_backward_idx ON t_deep_backward USING smol(k);

-- Inspect the tree structure
SELECT total_pages, leaf_pages FROM smol_inspect('t_deep_backward_idx'::regclass);

-- Force backward scan with specific target
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET max_parallel_workers_per_gather = 0;

-- Critical: Use smol_test_backward_scan with a target value
-- that should be the first leaf in a deep subtree.
-- With our tree structure (fanout=3, 20 tuples/page), we can calculate:
-- First leaf page has tuples 1-20
-- Second leaf page has tuples 21-40
-- Third leaf page has tuples 41-60
-- Fourth leaf page has tuples 61-80 (this is first child of second internal node)
--
-- If we do a backward scan starting from tuple 61, it should:
-- 1. Find tuple 61 (first tuple of a new internal node's first child)
-- 2. Need to find previous tuple (60)
-- 3. That requires going to previous sibling internal node's rightmost leaf
-- 4. This should trigger smol_rightmost_in_subtree with levels >= 2

-- Try multiple strategic positions that should be at internal node boundaries
SELECT smol_test_backward_scan('t_deep_backward_idx'::regclass, 61);
SELECT smol_test_backward_scan('t_deep_backward_idx'::regclass, 121);
SELECT smol_test_backward_scan('t_deep_backward_idx'::regclass, 181);
SELECT smol_test_backward_scan('t_deep_backward_idx'::regclass, 241);

-- Also try with ORDER BY DESC to trigger backward navigation
SELECT k FROM t_deep_backward WHERE k >= 60 AND k <= 65 ORDER BY k DESC;
SELECT k FROM t_deep_backward WHERE k >= 120 AND k <= 125 ORDER BY k DESC;

-- Reset GUCs
RESET smol.test_max_tuples_per_page;
RESET smol.test_max_internal_fanout;

-- Cleanup
DROP TABLE t_deep_backward CASCADE;

-- ============================================================================
-- smol_options_coverage
-- ============================================================================
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;
DROP TABLE IF EXISTS t_opt CASCADE;
CREATE TABLE t_opt (k int4);
-- Create index with fillfactor option (calls smol_options)
CREATE INDEX t_opt_idx ON t_opt USING smol(k) WITH (fillfactor=90);
-- Verify index was created
\d t_opt
-- Cleanup
DROP TABLE t_opt CASCADE;

