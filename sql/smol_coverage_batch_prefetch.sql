-- smol_coverage_batch_prefetch.sql
-- Test coverage for parallel claim batch loops and aggressive prefetch
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

-- Query that should trigger parallel scan with batch claiming
EXPLAIN SELECT COUNT(*) FROM t_batch_coverage WHERE k > 0;
SELECT COUNT(*) FROM t_batch_coverage WHERE k > 0;

-- Two-column index to test two-column batch claiming paths
CREATE UNLOGGED TABLE t_batch_twocol (k1 int4, k2 int4, v int4);
INSERT INTO t_batch_twocol SELECT i/100, i%100, i FROM generate_series(1, 100000) i;
CREATE INDEX t_batch_twocol_idx ON t_batch_twocol USING smol (k1, k2);
ANALYZE t_batch_twocol;

-- Force index-only scan to encourage parallel index scan
-- This triggers the two-column parallel scan path (lines 2597-2728)
SET enable_indexonlyscan = on;
SET enable_seqscan = off;
-- Use a selective WHERE to make index scan attractive (triggers parallel index-only scan)
SELECT COUNT(*) FROM t_batch_twocol WHERE k1 > 900;
-- Reset
SET enable_seqscan = on;

-- Backward scan with batch claiming - verify rows are returned in descending order
SELECT k FROM t_batch_coverage WHERE k >= 99990 ORDER BY k DESC;

-- ============================================================================
-- PART 2: Aggressive Prefetch Coverage
-- Tests lines 3426-3437
-- These lines only execute when smol_prefetch_depth > 1 in parallel INDEX scans
-- ============================================================================

-- Force prefetch depth > 1 to test aggressive prefetching
SET smol.prefetch_depth = 4;
-- Force index scans instead of seq scans
SET enable_seqscan = off;

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