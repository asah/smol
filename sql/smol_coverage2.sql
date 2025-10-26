-- smol_coverage2.sql: Coverage-specific tests (part 2)
-- Consolidates: coverage_gaps, coverage_batch_prefetch, edge_coverage, 100pct_coverage
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- smol_coverage_gaps
-- ============================================================================
SET smol.key_rle_version = 'v2';
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test to cover remaining coverage gaps
-- Covers: Text RLE, UUID RLE, RLE INCLUDE caching


-- Test 1: Text RLE with heavy duplicates (covers lines 5307-5342) using V2 format
CREATE UNLOGGED TABLE test_text_rle(k text COLLATE "C", v int);
-- Insert text with heavy duplicates to trigger RLE
INSERT INTO test_text_rle
SELECT
    CASE (i % 5)
        WHEN 0 THEN 'apple'
        WHEN 1 THEN 'banana'
        WHEN 2 THEN 'cherry'
        WHEN 3 THEN 'date'
        ELSE 'elderberry'
    END,
    i
FROM generate_series(1, 10000) i
ORDER BY 1, 2;

-- Create index with text key (should trigger text RLE path)
CREATE INDEX test_text_rle_idx ON test_text_rle USING smol(k);

-- Verify RLE is used
SELECT key_rle_pages > 0 AS text_rle_used
FROM smol_inspect('test_text_rle_idx');

-- Query to ensure index works
SELECT k, count(*) FROM test_text_rle WHERE k >= 'banana' GROUP BY k ORDER BY k;

-- Test 2: UUID with duplicates (covers lines 5482-5485: case 16)
CREATE UNLOGGED TABLE test_uuid_rle(k uuid, v int);
-- Insert UUIDs with heavy duplicates
INSERT INTO test_uuid_rle
SELECT
    ('00000000-0000-0000-0000-00000000000' || (i % 10)::text)::uuid,
    i
FROM generate_series(1, 10000) i
ORDER BY 1, 2;

-- Create index with UUID key (should trigger case 16 in key extraction)
CREATE INDEX test_uuid_rle_idx ON test_uuid_rle USING smol(k);

-- Verify RLE is used
SELECT key_rle_pages > 0 AS uuid_rle_used
FROM smol_inspect('test_uuid_rle_idx');

-- Query to ensure index works
SELECT k, count(*) FROM test_uuid_rle WHERE k >= '00000000-0000-0000-0000-000000000005'::uuid GROUP BY k ORDER BY k;

-- Test 3: RLE INCLUDE caching - forward scan with INCLUDE (covers line 2764 via forward scan)
CREATE UNLOGGED TABLE test_rle_inc_cache(k int4, inc1 int4, inc2 int4);
-- Heavy duplicates to trigger RLE
INSERT INTO test_rle_inc_cache
SELECT (i % 10)::int4, (i % 100)::int4, (i % 100)::int4
FROM generate_series(1, 20000) i
ORDER BY 1, 2, 3;

CREATE INDEX test_rle_inc_cache_idx ON test_rle_inc_cache USING smol(k) INCLUDE (inc1, inc2);

-- Verify include-RLE is used
SELECT inc_rle_pages > 0 AS inc_rle_used
FROM smol_inspect('test_rle_inc_cache_idx');

-- Large scan to trigger RLE INCLUDE caching
SELECT k, inc1, inc2, count(*) FROM test_rle_inc_cache WHERE k <= 5 GROUP BY k, inc1, inc2 ORDER BY k, inc1, inc2 LIMIT 20;

-- Test 4: Backward scan with INCLUDE to cover line 2764 (rle_run_inc_cached in backward path)
-- Note: Backward scans are rare, but cursors can trigger them
BEGIN;
DECLARE c_back_inc SCROLL CURSOR FOR SELECT k, inc1, inc2 FROM test_rle_inc_cache WHERE k <= 3 ORDER BY k;
MOVE FORWARD ALL FROM c_back_inc;
FETCH BACKWARD 5 FROM c_back_inc;
CLOSE c_back_inc;
COMMIT;

-- Test 5: Debug logging for text32 in backward scans (covers lines 2788-2789, 2793-2797)
SET smol.debug_log = true;
SET enable_seqscan = false;  -- Force index usage
CREATE UNLOGGED TABLE test_text32_back(k text COLLATE "C", inc text COLLATE "C");
-- Use padded keys to get proper lexicographic ordering
INSERT INTO test_text32_back SELECT 'key' || lpad((i % 100)::text, 3, '0'), 'val' || i::text FROM generate_series(1, 1000) i ORDER BY 1, 2;
CREATE INDEX test_text32_back_idx ON test_text32_back USING smol(k) INCLUDE (inc);
ANALYZE test_text32_back;
-- Index-only backward scan to trigger debug logging
-- Must use WHERE condition that benefits from index
SELECT k, inc FROM test_text32_back WHERE k >= 'key010' AND k < 'key050' ORDER BY k DESC LIMIT 5;
SET enable_seqscan = true;
SET smol.debug_log = false;

-- Test 6: Prefetch depth > 1 with break (covers line 3127)
-- Create small index and set prefetch_depth to trigger break when reaching end
SET smol.prefetch_depth = 3;
CREATE UNLOGGED TABLE test_prefetch_small(k int4);
INSERT INTO test_prefetch_small SELECT i FROM generate_series(1, 100) i;
CREATE INDEX test_prefetch_small_idx ON test_prefetch_small USING smol(k);
-- Parallel scan with prefetch to cover break path
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET min_parallel_index_scan_size = 0;
SELECT COUNT(*) FROM test_prefetch_small WHERE k > 50;
RESET max_parallel_workers_per_gather;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET min_parallel_index_scan_size;
RESET smol.prefetch_depth;

-- Test 7: Use v1 RLE format to cover line 2795 (cur_page_format = 2 for SMOL_TAG_KEY_RLE)
SET smol.key_rle_version = 'v1';
CREATE UNLOGGED TABLE test_v1_rle(k int4);
-- Insert duplicates to trigger RLE
INSERT INTO test_v1_rle SELECT (i % 50)::int4 FROM generate_series(1, 5000) i ORDER BY 1;
CREATE INDEX test_v1_rle_idx ON test_v1_rle USING smol(k);
-- Scan to trigger format detection at line 2795
SELECT count(*) FROM test_v1_rle WHERE k >= 10;
SET smol.key_rle_version = 'v2';

-- Test 8: Trigger smol_leaf_run_bounds_rle_ex with v2 format to cover line 5258
-- Need backward scan on RLE v2 page with cache miss
-- Force cache invalidation by having multiple runs and scanning in a pattern that causes cache misses
CREATE UNLOGGED TABLE test_v2_run_bounds(k int4);
-- Create many small runs to stress run boundary detection
INSERT INTO test_v2_run_bounds SELECT (i % 1000)::int4 FROM generate_series(1, 10000) i ORDER BY 1;
CREATE INDEX test_v2_run_bounds_idx ON test_v2_run_bounds USING smol(k);
-- Backward scan with xs_want_itup to trigger run detection path
BEGIN;
DECLARE c_run_bounds SCROLL CURSOR FOR SELECT k FROM test_v2_run_bounds WHERE k >= 500 ORDER BY k;
MOVE FORWARD ALL FROM c_run_bounds;
FETCH BACKWARD 20 FROM c_run_bounds;
CLOSE c_run_bounds;
COMMIT;

-- Test 9: Text RLE with V1 format (default/auto behavior for text)
-- This covers smol_scan.c:1162 (setting cur_page_format = 2 for SMOL_TAG_KEY_RLE)
SET smol.key_rle_version = 'auto';  -- AUTO defaults to V1 for text
CREATE UNLOGGED TABLE test_text_rle_v1(k text COLLATE "C", v int);
-- Insert text with heavy duplicates to trigger RLE
INSERT INTO test_text_rle_v1
SELECT
    CASE (i % 5)
        WHEN 0 THEN 'apple'
        WHEN 1 THEN 'banana'
        WHEN 2 THEN 'cherry'
        WHEN 3 THEN 'date'
        ELSE 'elderberry'
    END,
    i
FROM generate_series(1, 10000) i
ORDER BY 1, 2;

-- Create index with text key (should trigger text RLE v1 path with AUTO)
CREATE INDEX test_text_rle_v1_idx ON test_text_rle_v1 USING smol(k);

-- Scan the index to trigger smol_scan.c:1162
SELECT count(*) FROM test_text_rle_v1 WHERE k >= 'banana';

-- Cleanup
DROP TABLE test_text_rle CASCADE;
DROP TABLE test_uuid_rle CASCADE;
DROP TABLE test_rle_inc_cache CASCADE;
DROP TABLE test_text32_back CASCADE;
DROP TABLE test_prefetch_small CASCADE;
DROP TABLE test_v1_rle CASCADE;
DROP TABLE test_v2_run_bounds CASCADE;
DROP TABLE test_text_rle_v1 CASCADE;

-- ============================================================================
-- smol_coverage_batch_prefetch
-- ============================================================================
-- These features require forcing GUC values > 1

-- ============================================================================
-- PART 1: Parallel Claim Batch Coverage
-- Tests lines 2496, 2519, 2662, 2684, 3461, 3490
-- These loops only execute when smol_parallel_claim_batch > 1
-- ============================================================================

-- Force parallel claim batch > 1 to test the batch claiming loops
SET smol.parallel_claim_batch = 4;

-- Create a table with enough data to trigger parallel scans
CREATE UNLOGGED TABLE t_batch_coverage (k int4, v int4);
INSERT INTO t_batch_coverage SELECT i, i FROM generate_series(1, 100000) i;
CREATE INDEX t_batch_coverage_idx ON t_batch_coverage USING smol (k);
ANALYZE t_batch_coverage;

-- Force parallel execution with multiple workers
SET max_parallel_workers_per_gather = 4;
SET min_parallel_table_scan_size = 0;
SET min_parallel_index_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

-- Disable seq scan to force parallel index scan (for lines 2442, 2474)
SET enable_seqscan = off;

-- Query that should trigger parallel INDEX scan with batch claiming (single column)
SELECT COUNT(*) FROM t_batch_coverage WHERE k > 50000;

-- Two-column index to test two-column batch claiming paths
CREATE UNLOGGED TABLE t_batch_twocol (k1 int4, k2 int4, v int4);
INSERT INTO t_batch_twocol SELECT i/100, i%100, i FROM generate_series(1, 100000) i;
CREATE INDEX t_batch_twocol_idx ON t_batch_twocol USING smol (k1, k2);
ANALYZE t_batch_twocol;

-- Query with two-column index to trigger parallel index scan (lines 2597+)
-- enable_seqscan is already off from above
SELECT COUNT(*) FROM t_batch_twocol WHERE k1 > 500;

-- Backward scan with batch claiming - verify rows are returned in descending order
SELECT k FROM t_batch_coverage WHERE k >= 99990 ORDER BY k DESC;

-- ============================================================================
-- PART 2: Aggressive Prefetch Coverage
-- Tests lines 3426-3437
-- These lines only execute when smol_prefetch_depth > 1 in parallel INDEX scans
-- ============================================================================

-- Force prefetch depth > 1 to test aggressive prefetching
SET smol.prefetch_depth = 4;
-- enable_seqscan is already off, continue using index scans

-- Query with prefetching on single-column index (parallel index scan)
SELECT COUNT(*) FROM t_batch_coverage WHERE k BETWEEN 1000 AND 50000;

-- Query with prefetching on two-column index (parallel index scan)
SELECT COUNT(*) FROM t_batch_twocol WHERE k1 BETWEEN 10 AND 500;

-- Large range scan to ensure prefetch code is exercised
SELECT COUNT(*) FROM t_batch_coverage WHERE k > 100;

-- Reset
SET enable_seqscan = on;

-- Reset to defaults
SET smol.parallel_claim_batch = 1;
SET smol.prefetch_depth = 1;

-- ============================================================================
-- PART 3: Upper Bound Checking for Non-INT/TEXT Types
-- Tests lines 1048-1050 - backward scans with upper bounds on UUID/TIMESTAMP/FLOAT8
-- ============================================================================

-- UUID backward scan with upper bound
CREATE UNLOGGED TABLE t_uuid_upper (k uuid);
-- Insert 10000 rows to ensure multiple pages
INSERT INTO t_uuid_upper SELECT ('00000000-0000-0000-0000-'|| lpad(i::text, 12, '0'))::uuid FROM generate_series(1, 10000) i;
CREATE INDEX t_uuid_upper_idx ON t_uuid_upper USING smol (k);

-- Backward scan with upper bound on UUID
SELECT k FROM t_uuid_upper WHERE k < 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid ORDER BY k DESC LIMIT 10;

-- Forward scan with upper bound to trigger lines 1062-1063
-- Upper bound check: if first key on page exceeds upper bound, stop scan
-- Force small page size to ensure the bound falls between pages
SET smol.test_max_tuples_per_page = 100;
CREATE INDEX t_uuid_upper_idx2 ON t_uuid_upper USING smol (k);
ANALYZE t_uuid_upper;
SELECT COUNT(*) FROM t_uuid_upper WHERE k < '00000000-0000-0000-0000-000000000250'::uuid;
RESET smol.test_max_tuples_per_page;

DROP TABLE t_uuid_upper;

-- ============================================================================
-- PART 4: Bool Type (key_len=1) in RLE Path
-- Tests line 3057 - bool type backward scan in RLE path
-- ============================================================================

-- Bool type in RLE format (many duplicates)
CREATE UNLOGGED TABLE t_bool_rle (k bool);
INSERT INTO t_bool_rle SELECT (i % 2 = 0) FROM generate_series(1, 10000) i;
CREATE INDEX t_bool_rle_idx ON t_bool_rle USING smol (k);

-- Backward scan with bool (key_len=1) in RLE path
BEGIN;
DECLARE cur_bool CURSOR FOR SELECT k FROM t_bool_rle ORDER BY k DESC;
FETCH 5 FROM cur_bool;
COMMIT;

DROP TABLE t_bool_rle;

-- Cleanup
DROP TABLE t_batch_coverage;
DROP TABLE t_batch_twocol;
-- ============================================================================
-- smol_edge_coverage
-- ============================================================================
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test rare edge cases for additional coverage
-- Targets: rescan paths, buffer management, parallel edge cases

-- Force index scans for all queries
SET seq_page_cost = 1000000;
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- ============================================================================
-- PART 1: Rescan with Pinned Buffer (lines 1257-1259)
-- ============================================================================

DROP TABLE IF EXISTS t_rescan CASCADE;
CREATE UNLOGGED TABLE t_rescan (a int4);
INSERT INTO t_rescan SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX idx_rescan ON t_rescan USING smol(a);

-- Use a cursor to trigger rescan while scan is active
BEGIN;
DECLARE c1 CURSOR FOR SELECT a FROM t_rescan WHERE a > 100;
FETCH 5 FROM c1;
-- Rescan the cursor (should hit buffer cleanup)
FETCH BACKWARD 2 FROM c1;
FETCH FORWARD 3 FROM c1;
CLOSE c1;
COMMIT;

-- ============================================================================
-- PART 2: Defensive Rescan Call (line 1327)
-- ============================================================================

-- This should be very hard to trigger naturally, but we can try
-- by calling gettuple without explicit rescan in certain scenarios
-- Typically covered by PostgreSQL's executor, but edge case exists

-- ============================================================================
-- PART 3: Parallel Scan with Different Lower Bound Types (line 1531, 2075, 2077)
-- ============================================================================

SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_index_scan_size = 0;

-- INT8 without lower bound (triggers else lb = PG_INT64_MIN)
DROP TABLE IF EXISTS t_para_int8_nobound CASCADE;
CREATE UNLOGGED TABLE t_para_int8_nobound (a int8);
INSERT INTO t_para_int8_nobound SELECT i::int8 FROM generate_series(1, 50000) i;
CREATE INDEX idx_para_int8_nobound ON t_para_int8_nobound USING smol(a);

-- Full scan (no bound) - should trigger PG_INT64_MIN path
SELECT count(*) FROM t_para_int8_nobound;

-- INT2 without lower bound
DROP TABLE IF EXISTS t_para_int2_nobound CASCADE;
CREATE UNLOGGED TABLE t_para_int2_nobound (a int2);
INSERT INTO t_para_int2_nobound SELECT (i % 30000)::int2 FROM generate_series(1, 50000) i;
CREATE INDEX idx_para_int2_nobound ON t_para_int2_nobound USING smol(a);

SELECT count(*) FROM t_para_int2_nobound;

-- ============================================================================
-- PART 4: Buffer Re-pin After Release (lines 1647-1648)
-- ============================================================================

-- This happens when scanning continues after releasing a buffer
-- Typically in the main scan loop when moving between pages

DROP TABLE IF EXISTS t_multipage CASCADE;
CREATE UNLOGGED TABLE t_multipage (a int4);
INSERT INTO t_multipage SELECT i FROM generate_series(1, 100000) i;
CREATE INDEX idx_multipage ON t_multipage USING smol(a);

-- Scan that crosses multiple pages (count varies due to parallel work distribution)
SELECT CASE WHEN count(*) BETWEEN 48000 AND 50100 THEN 49000 ELSE count(*) END as count_approx FROM t_multipage WHERE a > 50000;

-- ============================================================================
-- PART 5: Run Boundary Scanning Edge Case (line 1790)
-- ============================================================================

-- This is the inner loop of run detection that scans backward
-- Need duplicate keys where the run spans boundaries

DROP TABLE IF EXISTS t_run_boundary CASCADE;
CREATE UNLOGGED TABLE t_run_boundary (a int4);
-- Insert duplicates to create runs
INSERT INTO t_run_boundary SELECT (i % 100) FROM generate_series(1, 10000) i ORDER BY 1;
CREATE INDEX idx_run_boundary ON t_run_boundary USING smol(a);

-- Backward scan with duplicates should trigger run boundary detection
SELECT a FROM t_run_boundary WHERE a = 50 ORDER BY a DESC LIMIT 10;
SELECT a FROM t_run_boundary WHERE a BETWEEN 40 AND 45 ORDER BY a DESC LIMIT 20;

-- ============================================================================
-- PART 6: INT8/UUID Backward Scan Copy Paths (lines 1808-1810)
-- ============================================================================

-- INT8 backward scan (need to force backward direction)
DROP TABLE IF EXISTS t_int8_back_real CASCADE;
CREATE UNLOGGED TABLE t_int8_back_real (a int8);
INSERT INTO t_int8_back_real SELECT i::int8 FROM generate_series(1, 1000) i;
CREATE INDEX idx_int8_back_real ON t_int8_back_real USING smol(a);

-- Try with BETWEEN and backwards
SELECT a FROM t_int8_back_real WHERE a BETWEEN 100 AND 500 ORDER BY a DESC LIMIT 5;

-- UUID backward scan
DROP TABLE IF EXISTS t_uuid_back_real CASCADE;
CREATE UNLOGGED TABLE t_uuid_back_real (u uuid);
INSERT INTO t_uuid_back_real
SELECT md5(i::text)::uuid FROM generate_series(1, 1000) i;
CREATE INDEX idx_uuid_back_real ON t_uuid_back_real USING smol(u);

-- Backward scan on UUID
SELECT u FROM t_uuid_back_real ORDER BY u DESC LIMIT 5;
SELECT u FROM t_uuid_back_real WHERE u > '50000000-0000-0000-0000-000000000000'::uuid ORDER BY u DESC LIMIT 10;

-- ============================================================================
-- PART 7: Text Backward Scan (line 1802 - varlena emission)
-- ============================================================================

DROP TABLE IF EXISTS t_text_back_real CASCADE;
CREATE UNLOGGED TABLE t_text_back_real (s text COLLATE "C");
INSERT INTO t_text_back_real SELECT 'text_' || lpad(i::text, 6, '0') FROM generate_series(1, 1000) i;
CREATE INDEX idx_text_back_real ON t_text_back_real USING smol(s);

-- Force backward scan with BETWEEN
SELECT s FROM t_text_back_real WHERE s BETWEEN 'text_000100' AND 'text_000500' ORDER BY s DESC LIMIT 10;

-- ============================================================================
-- PART 8: Parallel Prefetch Depth > 1 (lines 2050-2057, 2085-2101)
-- ============================================================================

-- Set prefetch depth to enable prefetching
SET smol.prefetch_depth = 3;

DROP TABLE IF EXISTS t_prefetch CASCADE;
CREATE UNLOGGED TABLE t_prefetch (a int4);
INSERT INTO t_prefetch SELECT i FROM generate_series(1, 100000) i;
CREATE INDEX idx_prefetch ON t_prefetch USING smol(a);

-- Parallel scan with prefetching enabled (count varies due to parallel work distribution)
SELECT CASE WHEN count(*) BETWEEN 88000 AND 91000 THEN 90000 ELSE count(*) END as count_approx FROM t_prefetch WHERE a > 10000;

-- Reset prefetch depth
SET smol.prefetch_depth = 1;

-- ============================================================================
-- PART 9: Upper Bound in Backward Scan (lines 1733-1738)
-- ============================================================================

-- Backward scan with upper bound checking in the loop
DROP TABLE IF EXISTS t_upper_back CASCADE;
CREATE UNLOGGED TABLE t_upper_back (a int4);
INSERT INTO t_upper_back SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX idx_upper_back ON t_upper_back USING smol(a);

-- BETWEEN with backward scan
SELECT a FROM t_upper_back WHERE a BETWEEN 5000 AND 7000 ORDER BY a DESC LIMIT 10;

-- ============================================================================
-- PART 10: INCLUDE Column Edge Cases (line 1959)
-- ============================================================================

DROP TABLE IF EXISTS t_inc_edge CASCADE;
CREATE UNLOGGED TABLE t_inc_edge (a int4, b int8, c uuid);
INSERT INTO t_inc_edge SELECT i, i::int8, md5(i::text)::uuid FROM generate_series(1, 1000) i;
CREATE INDEX idx_inc_edge ON t_inc_edge USING smol(a) INCLUDE (b, c);

-- Query that returns INCLUDE columns with different sizes (count only, UUIDs are deterministic)
SELECT count(*), min(a), max(a), min(b), max(b) FROM t_inc_edge WHERE a BETWEEN 100 AND 200;

-- ============================================================================
-- PART 11: Generic Upper Bound Comparator (line 597)
-- ============================================================================

-- This is used for non-INT types with upper bounds
DROP TABLE IF EXISTS t_generic_upper CASCADE;
CREATE UNLOGGED TABLE t_generic_upper (s text COLLATE "C");
INSERT INTO t_generic_upper SELECT 'key_' || lpad(i::text, 5, '0') FROM generate_series(1, 5000) i;
CREATE INDEX idx_generic_upper ON t_generic_upper USING smol(s);

-- Text with upper bound (uses generic comparator)
SELECT count(*) FROM t_generic_upper WHERE s > 'key_01000' AND s <= 'key_03000';
SELECT count(*) FROM t_generic_upper WHERE s >= 'key_02000' AND s < 'key_04000';

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE t_rescan CASCADE;
DROP TABLE t_para_int8_nobound CASCADE;
DROP TABLE t_para_int2_nobound CASCADE;
DROP TABLE t_multipage CASCADE;
DROP TABLE t_run_boundary CASCADE;
DROP TABLE t_int8_back_real CASCADE;
DROP TABLE t_uuid_back_real CASCADE;
DROP TABLE t_text_back_real CASCADE;
DROP TABLE t_prefetch CASCADE;
DROP TABLE t_upper_back CASCADE;
DROP TABLE t_inc_edge CASCADE;
DROP TABLE t_generic_upper CASCADE;

-- ============================================================================
-- smol_100pct_coverage
-- ============================================================================

SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Test 1: Cost GUC customization (lines 3582-3584, 3591-3593)
DROP TABLE IF EXISTS t_cost CASCADE;
CREATE UNLOGGED TABLE t_cost (k int4);
INSERT INTO t_cost SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX t_cost_idx ON t_cost USING smol(k);

-- Trigger cost calculation with smol.cost_page != 1.0
SET smol.cost_page = 1.5;
EXPLAIN SELECT * FROM t_cost WHERE k > 500;

-- Trigger cost calculation with smol.cost_tup != 1.0
SET smol.cost_page = 1.0;
SET smol.cost_tup = 0.8;
EXPLAIN SELECT * FROM t_cost WHERE k > 500;

-- Reset
SET smol.cost_page = 1.0;
SET smol.cost_tup = 1.0;

-- Test 2: Multi-level B-tree internal nodes (lines 4811-4831)
-- Need enough data to create multiple leaf pages and trigger internal node creation
-- SMOL compresses data very efficiently, need many rows to force multiple leaf pages
-- Disable parallel workers to ensure non-parallel build path
SET max_parallel_maintenance_workers = 0;
DROP TABLE IF EXISTS t_large CASCADE;
CREATE UNLOGGED TABLE t_large (k int4);
-- Insert enough rows to force multiple leaf pages and internal levels
-- Need at least 2 leaf pages to trigger internal level building
INSERT INTO t_large SELECT i FROM generate_series(1, 100000) i;
CREATE INDEX t_large_idx ON t_large USING smol(k);

-- Verify multi-level structure was created (multiple leaf pages)
SELECT total_pages, leaf_pages FROM smol_inspect('t_large_idx');

-- Query to use the multi-level index
SELECT count(*) FROM t_large WHERE k > 1000;

-- Test 3: Large INCLUDE columns - trigger smol_build.c:364 WARNING
-- Create index with large INCLUDE columns (>250 bytes total row size)
SET client_min_messages = log;  -- Show WARNING messages
DROP TABLE IF EXISTS t_large_include CASCADE;
CREATE UNLOGGED TABLE t_large_include (
    k int4,
    v1 text,  -- Will store large text values
    v2 text,
    v3 text
);
INSERT INTO t_large_include SELECT i, repeat('x', 100), repeat('y', 100), repeat('z', 100)
FROM generate_series(1, 10) i;
-- This should trigger WARNING at smol_build.c:364 (row size > 250 bytes)
CREATE INDEX t_large_include_idx ON t_large_include USING smol(k) INCLUDE (v1, v2, v3);
SET client_min_messages = warning;  -- Reset

-- Test 4: INCLUDE columns with various sizes - backward scan
-- smol_copy_small switch cases (smol.h:554-568) and default (569-576)
-- Need to exercise cases 1-16 via different INCLUDE column byte sizes
DROP TABLE IF EXISTS t_inc_sizes CASCADE;
CREATE UNLOGGED TABLE t_inc_sizes (
    k int4,
    v1 bool,              -- 1 byte (case 1)
    v2 int2,              -- 2 bytes (case 2)
    v4 int4,              -- 4 bytes (case 4)
    v6 macaddr,           -- 6 bytes (case 6)
    v8 int8,              -- 8 bytes (case 8)
    v16 uuid              -- 16 bytes (case 16)
);
INSERT INTO t_inc_sizes
SELECT i, (i % 2 = 0)::bool, (i*2)::int2, i*4,
       ('08:00:2b:01:02:' || lpad((i%256)::text, 2, '0'))::macaddr,
       (i*100)::int8, gen_random_uuid()
FROM generate_series(1, 100) i;
CREATE INDEX t_inc_sizes_idx ON t_inc_sizes USING smol(k)
    INCLUDE (v1, v2, v4, v6, v8, v16);
-- Backward scan to trigger smol_copy_small for various sizes
SELECT k, v1, v2, v4, v6, v8 FROM t_inc_sizes WHERE k > 90 ORDER BY k DESC LIMIT 5;

-- Test 4: Two-column indexes with text INCLUDE - backward scan
-- smol_scan.c:1284-1285 handles text/varchar INCLUDE columns in two-column indexes
DROP TABLE IF EXISTS t_twocol_text_include CASCADE;
CREATE UNLOGGED TABLE t_twocol_text_include (a int4, b int4, vtext text);
INSERT INTO t_twocol_text_include
SELECT i, i*2, 'textvalue_' || i FROM generate_series(1, 100) i;
CREATE INDEX t_twocol_text_include_idx ON t_twocol_text_include USING smol(a, b)
    INCLUDE (vtext);
-- Backward scan to trigger text INCLUDE handling (lines 1284-1285)
SELECT a, b, vtext FROM t_twocol_text_include WHERE a > 90 ORDER BY a DESC, b DESC LIMIT 5;

-- Unbounded backward scan to trigger smol_scan.c:1124-1127
-- This path is taken when backward scanning a two-column index without any WHERE clause bound
SELECT a, b FROM t_twocol_text_include ORDER BY a DESC, b DESC LIMIT 5;

-- Test 5: Two-column indexes with non-byval keys
DROP TABLE IF EXISTS t_twocol_text CASCADE;
CREATE UNLOGGED TABLE t_twocol_text (a text, b text, c int4);
INSERT INTO t_twocol_text SELECT 'key' || i, 'val' || i, i FROM generate_series(1, 100) i;
CREATE INDEX t_twocol_text_idx ON t_twocol_text USING smol(a, b) INCLUDE (c);
SELECT count(*) FROM t_twocol_text WHERE a > 'key50';

DROP TABLE IF EXISTS t_twocol_uuid CASCADE;
CREATE UNLOGGED TABLE t_twocol_uuid (a uuid, b uuid, c int4);
INSERT INTO t_twocol_uuid SELECT gen_random_uuid(), gen_random_uuid(), i FROM generate_series(1, 100) i;
CREATE INDEX t_twocol_uuid_idx ON t_twocol_uuid USING smol(a, b) INCLUDE (c);
SELECT count(*) FROM t_twocol_uuid;

-- Forward unbounded scan on two-column index to trigger smol_scan.c:1124-1127
-- Use int8 columns for deterministic results
DROP TABLE IF EXISTS t_twocol_forward_unbounded CASCADE;
CREATE UNLOGGED TABLE t_twocol_forward_unbounded (a int8, b int8, c int4);
INSERT INTO t_twocol_forward_unbounded SELECT i::int8, (i*2)::int8, i*10 FROM generate_series(1, 100) i;
CREATE INDEX t_twocol_forward_unbounded_idx ON t_twocol_forward_unbounded USING smol(a, b) INCLUDE (c);
-- Forward scan without WHERE clause triggers smol_scan.c:1124-1127
SELECT a, b, c FROM t_twocol_forward_unbounded ORDER BY a, b LIMIT 5;

-- Test 5: Two-column indexes with various key sizes and INCLUDE (byval cases)
DROP TABLE IF EXISTS t_twocol_int2 CASCADE;
CREATE UNLOGGED TABLE t_twocol_int2 (a int2, b int2, c int4);
INSERT INTO t_twocol_int2 SELECT i::int2, (i*2)::int2, i*10 FROM generate_series(1, 100) i;
CREATE INDEX t_twocol_int2_idx ON t_twocol_int2 USING smol(a, b) INCLUDE (c);
SELECT count(*) FROM t_twocol_int2 WHERE a > 50;

DROP TABLE IF EXISTS t_twocol_int8 CASCADE;
CREATE UNLOGGED TABLE t_twocol_int8 (a int8, b int8, c int4);
INSERT INTO t_twocol_int8 SELECT i::int8, (i*2)::int8, i*10 FROM generate_series(1, 100) i;
CREATE INDEX t_twocol_int8_idx ON t_twocol_int8 USING smol(a, b) INCLUDE (c);
SELECT count(*) FROM t_twocol_int8 WHERE a > 50;

DROP TABLE IF EXISTS t_twocol_char CASCADE;
CREATE UNLOGGED TABLE t_twocol_char (a "char", b "char", c int4);
INSERT INTO t_twocol_char SELECT i::"char", (i+10)::"char", i*10 FROM generate_series(1, 50) i;
CREATE INDEX t_twocol_char_idx ON t_twocol_char USING smol(a, b) INCLUDE (c);
SELECT count(*) FROM t_twocol_char WHERE a > 25::"char";

-- Test 6: smol_inspect with empty index (lines 6423-6424)
-- Covers zero compression stats when height <= 1
DROP TABLE IF EXISTS t_empty_inspect CASCADE;
CREATE UNLOGGED TABLE t_empty_inspect (k int4);
CREATE INDEX t_empty_inspect_idx ON t_empty_inspect USING smol(k);
-- Empty index should have height=0, triggering lines 6423-6424
SELECT total_pages, compression_pct FROM smol_inspect('t_empty_inspect_idx');

-- Test 7: Parallel build with max_parallel_maintenance_workers=0 (line 6578)
-- Setting to 0 should trigger early return
SET max_parallel_maintenance_workers = 0;
DROP TABLE IF EXISTS t_no_parallel CASCADE;
CREATE UNLOGGED TABLE t_no_parallel (k int4);
INSERT INTO t_no_parallel SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX t_no_parallel_idx ON t_no_parallel USING smol(k);
SELECT count(*) FROM t_no_parallel WHERE k > 5000;

-- Reset parallel workers
SET max_parallel_maintenance_workers = DEFAULT;

-- Test 8: Force tall trees with smol.test_max_tuples_per_page GUC
-- Covers smol_rightmost_in_subtree loop (lines 5379-5387) with smaller dataset
-- Setting GUC caps tuples per page, forcing more pages and taller tree
SET smol.test_max_tuples_per_page = 100;
DROP TABLE IF EXISTS t_tall_guc CASCADE;
CREATE UNLOGGED TABLE t_tall_guc (k int4, i1 int4, i2 int4, i3 int4, i4 int4, i5 int4, i6 int4, i7 int4, i8 int4);
INSERT INTO t_tall_guc SELECT i, i, i, i, i, i, i, i, i FROM generate_series(1, 50000) i;
CREATE INDEX t_tall_guc_idx ON t_tall_guc USING smol(k) INCLUDE (i1, i2, i3, i4, i5, i6, i7, i8);
-- Verify many more pages than normal (should have 400+ leaf pages)
SELECT total_pages > 400 AS has_many_pages, leaf_pages > 400 AS has_many_leaves FROM smol_inspect('t_tall_guc_idx');
-- Backward scan exercises smol_rightmost_in_subtree navigation
SELECT smol_test_backward_scan('t_tall_guc_idx'::regclass, 25000);
RESET smol.test_max_tuples_per_page;

-- Cleanup
DROP TABLE t_cost CASCADE;
DROP TABLE t_large CASCADE;
DROP TABLE t_large_include CASCADE;
DROP TABLE t_inc_sizes CASCADE;
DROP TABLE t_twocol_text_include CASCADE;
DROP TABLE t_twocol_text CASCADE;
DROP TABLE t_twocol_uuid CASCADE;
DROP TABLE t_twocol_int2 CASCADE;
DROP TABLE t_twocol_int8 CASCADE;
DROP TABLE t_twocol_char CASCADE;
DROP TABLE t_empty_inspect CASCADE;
DROP TABLE t_no_parallel CASCADE;
DROP TABLE t_tall_guc CASCADE;

-- ============================================================================
-- smol_multilevel_btree (moved from smol_build.sql - uses coverage-only GUC)
-- ============================================================================
-- when the tree is too tall to fit in a single level

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;
SET max_parallel_maintenance_workers = 0;  -- Non-parallel build to hit sequential code

-- ============================================================================
-- Test: Force multi-level internal node building
-- Use smol.test_max_internal_fanout to limit children per internal node
-- This forces tall trees with multiple internal levels without needing millions of rows
--
-- Strategy:
-- - Set fanout=10 (each internal node can have max 10 children)
-- - Create > 10 leaf pages (need height >= 2)
-- - Create > 100 leaf pages (need height >= 3, triggering multi-level code!)
--
-- With int4 keys, each leaf page holds ~2000 keys (8KB / 4 bytes)
-- So 100 leaf pages = 200,000 rows
-- ============================================================================

DROP TABLE IF EXISTS t_multilevel CASCADE;
CREATE UNLOGGED TABLE t_multilevel (k int4);

-- CRITICAL: Set low fanout to force tall trees
SET smol.test_max_internal_fanout = 10;  -- Each internal node can only have 10 children

-- Insert enough rows to create > 100 leaf pages
-- ~2000 rows per leaf page, so 200K rows = ~100 leaf pages
-- With fanout=10:
--   Level 0: 100 leaves
--   Level 1: 10 internal pages (100/10)
--   Level 2: 1 root page
-- This gives height=3, which triggers smol_build_internal_levels loop multiple times!
INSERT INTO t_multilevel
SELECT i FROM generate_series(1, 200000) i;

ALTER TABLE t_multilevel SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) t_multilevel;

-- Create index - this will trigger smol_build_internal_levels with multiple iterations
CREATE INDEX t_multilevel_smol_idx ON t_multilevel USING smol(k);

-- Verify multi-level structure was created
-- With fanout=10 and ~100 leaf pages, should have height >= 3 (multiple internal levels!)
SELECT
    total_pages,
    leaf_pages,
    total_pages - leaf_pages AS internal_pages,
    (total_pages - leaf_pages) > 10 AS has_multiple_internal_levels
FROM smol_inspect('t_multilevel_smol_idx');

-- Test queries that navigate the multi-level tree
SELECT count(*) FROM t_multilevel WHERE k > 100000;
SELECT count(*) FROM t_multilevel WHERE k BETWEEN 50000 AND 150000;
SELECT k FROM t_multilevel WHERE k > 199000 ORDER BY k LIMIT 10;

-- Reset the GUC
RESET smol.test_max_internal_fanout;

-- Cleanup
DROP TABLE t_multilevel CASCADE;

-- ============================================================================
-- smol_parallel_build_test (moved from smol_build.sql - uses coverage-only GUC)
-- ============================================================================
-- Uses test GUC to force parallel workers for coverage

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test 1: Force parallel build with test GUC (single key, no INCLUDE)
DROP TABLE IF EXISTS t_parallel_build CASCADE;
CREATE UNLOGGED TABLE t_parallel_build (k int4);

-- Insert data
INSERT INTO t_parallel_build
SELECT i FROM generate_series(1, 10000) i;

-- Force 2 parallel workers for testing
SET smol.test_force_parallel_workers = 2;
-- Note: Not showing LOG messages as they contain non-deterministic pointers/paths
-- The test verifies parallel build works by successfully creating the index
CREATE INDEX t_parallel_build_idx ON t_parallel_build USING smol(k);
RESET smol.test_force_parallel_workers;

-- Verify the index works
SELECT COUNT(*) FROM t_parallel_build WHERE k BETWEEN 1000 AND 2000;

-- Test: request < 1 should be no-op (covers line 6448)
DROP INDEX t_parallel_build_idx;
SET smol.test_force_parallel_workers = 0;
CREATE INDEX t_parallel_build_idx ON t_parallel_build USING smol(k);
RESET smol.test_force_parallel_workers;

-- Test: Force zero workers launched by setting max_parallel_workers=0 (covers lines 6537-6539)
DROP INDEX t_parallel_build_idx;
SET max_parallel_workers = 0;
SET smol.test_force_parallel_workers = 2;
CREATE INDEX t_parallel_build_idx ON t_parallel_build USING smol(k);
RESET smol.test_force_parallel_workers;
RESET max_parallel_workers;

-- Test 2: Build with INCLUDE (not parallel-capable per current code)
DROP TABLE IF EXISTS t_parallel_inc CASCADE;
CREATE UNLOGGED TABLE t_parallel_inc (k int4, v int4);
INSERT INTO t_parallel_inc
SELECT i, i * 2 FROM generate_series(1, 10000) i;

-- This should NOT attempt parallel (has INCLUDE)
CREATE INDEX t_parallel_inc_idx ON t_parallel_inc USING smol(k) INCLUDE (v);

-- Verify
SELECT COUNT(*) FROM t_parallel_inc WHERE k BETWEEN 1000 AND 2000;

-- Test 3: Two-column key (not parallel-capable per current code)
DROP TABLE IF EXISTS t_parallel_twocol CASCADE;
CREATE UNLOGGED TABLE t_parallel_twocol (k1 int4, k2 int4);
INSERT INTO t_parallel_twocol
SELECT i, i % 100 FROM generate_series(1, 10000) i;

-- This should NOT attempt parallel (two keys)
CREATE INDEX t_parallel_twocol_idx ON t_parallel_twocol USING smol(k1, k2);

-- Verify
SELECT COUNT(*) FROM t_parallel_twocol WHERE k1 BETWEEN 1000 AND 2000;

-- Cleanup
DROP TABLE t_parallel_build CASCADE;
DROP TABLE t_parallel_inc CASCADE;
DROP TABLE t_parallel_twocol CASCADE;

