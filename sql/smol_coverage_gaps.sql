SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test to cover remaining coverage gaps
-- Covers: Text RLE, UUID RLE, RLE INCLUDE caching


-- Test 1: Text RLE with heavy duplicates (covers lines 5307-5342)
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

-- Cleanup
DROP TABLE test_text_rle CASCADE;
DROP TABLE test_uuid_rle CASCADE;
DROP TABLE test_rle_inc_cache CASCADE;
DROP TABLE test_text32_back CASCADE;
DROP TABLE test_prefetch_small CASCADE;
DROP TABLE test_v1_rle CASCADE;
DROP TABLE test_v2_run_bounds CASCADE;
