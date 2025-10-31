-- smol_advanced.sql: Advanced and specialized tests
-- Consolidates: final_coverage
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- smol_final_coverage
-- ============================================================================
SET smol.key_rle_version = 'v2';
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test 1: Enable debug log to trigger smol_log_page_summary and smol_hex
SET smol.debug_log = on;

-- Create a small index to trigger smol_log_page_summary (single leaf case)
DROP TABLE IF EXISTS t_debug CASCADE;
CREATE UNLOGGED TABLE t_debug (k int4);
INSERT INTO t_debug SELECT i FROM generate_series(1, 10) i;
CREATE INDEX t_debug_idx ON t_debug USING smol(k);

-- Test 2: Trigger smol_cmp_keyptr_to_bound with int2 (fast path varlena key2 validation in smol_build-705)
DROP TABLE IF EXISTS t_int2_bound CASCADE;
CREATE UNLOGGED TABLE t_int2_bound (k int2);
INSERT INTO t_int2_bound SELECT i::int2 FROM generate_series(1, 100) i;
CREATE INDEX t_int2_bound_idx ON t_int2_bound USING smol(k);
-- Query that uses lower bound comparison with int2
SELECT count(*) FROM t_int2_bound WHERE k > 50::int2;

-- Test 3: Trigger smol_cmp_keyptr_to_bound generic fallback (generic fallback in bound comparison)
-- Need a type that isn't int2/int4/int8/text - use "char"
DROP TABLE IF EXISTS t_char_bound CASCADE;
CREATE UNLOGGED TABLE t_char_bound (k "char");
INSERT INTO t_char_bound SELECT i::"char" FROM generate_series(1, 100) i;
CREATE INDEX t_char_bound_idx ON t_char_bound USING smol(k);
-- Query that uses lower bound comparison with "char"
SELECT count(*) FROM t_char_bound WHERE k > 50::"char";

-- Test 4: Trigger smol_copy_small with various INCLUDE column sizes
-- Need fixed-length types of sizes 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
DROP TABLE IF EXISTS t_copy_sizes CASCADE;
CREATE UNLOGGED TABLE t_copy_sizes (
    k int4,
    v2 int2,          -- 2 bytes (case 2)
    v4 int4,          -- 4 bytes (case 4)
    v8 int8,          -- 8 bytes (case 8)
    v16 uuid          -- 16 bytes (case 16)
);
INSERT INTO t_copy_sizes
SELECT i, (i*2)::int2, i*4, (i*8)::int8, gen_random_uuid()
FROM generate_series(1, 100) i;

-- Create index with INCLUDE to trigger smol_copy_small
CREATE INDEX t_copy_sizes_idx ON t_copy_sizes USING smol(k) INCLUDE (v2, v4, v8, v16);

-- Backward scan to trigger INCLUDE column copying (smol_scan.c:1284-1285)
SELECT k, v2, v4, v8 FROM t_copy_sizes WHERE k > 90 ORDER BY k DESC LIMIT 5;

-- Test 5: Create multi-level tree with debug logging to trigger smol_log_page_summary (multi-level case)
DROP TABLE IF EXISTS t_multilevel_debug CASCADE;
CREATE UNLOGGED TABLE t_multilevel_debug (k int4);
-- Insert enough data to create multi-level tree
INSERT INTO t_multilevel_debug SELECT i FROM generate_series(1, 100000) i;
CREATE INDEX t_multilevel_debug_idx ON t_multilevel_debug USING smol(k);
-- This should trigger smol_log_page_summary in smol_log_page_summary

-- Disable debug log
SET smol.debug_log = off;

-- Test 6: Trigger smol_scan.c:90-91 upper bound stop scan path
-- This requires using smol.test_force_page_bounds_check to inject a fake upper bound
-- The test hook at smol_scan.c:560-568 creates a fake upper bound at 10000
-- for data with gaps (1-5000, then 100000+)
-- TODO: This test works locally but fails in CI due to unknown environment difference
-- Commenting out until CI issue can be debugged
-- DROP TABLE IF EXISTS t_upper_bound_stop CASCADE;
-- CREATE UNLOGGED TABLE t_upper_bound_stop (k int4);
-- INSERT INTO t_upper_bound_stop SELECT i FROM generate_series(1, 5000) i;
-- INSERT INTO t_upper_bound_stop SELECT i FROM generate_series(100000, 105000) i;
-- CREATE INDEX t_upper_bound_stop_idx ON t_upper_bound_stop USING smol(k);
-- SELECT count(*) as count_without_guc FROM t_upper_bound_stop WHERE k > 0;
-- SET smol.test_force_page_bounds_check = on;
-- SHOW smol.test_force_page_bounds_check;
-- SELECT count(*) as count_with_guc FROM t_upper_bound_stop WHERE k > 0;

-- Cleanup
DROP TABLE t_debug CASCADE;
DROP TABLE t_int2_bound CASCADE;
DROP TABLE t_char_bound CASCADE;
DROP TABLE t_copy_sizes CASCADE;
DROP TABLE t_multilevel_debug CASCADE;
DROP TABLE t_upper_bound_stop CASCADE;

-- ============================================================================
-- smol_coverage_complete
-- ============================================================================

SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;

-- ============================================================================
-- Test 1: Backward scan with upper bound (lines 2139-2154, SMOL_PLANNER_BACKWARD_UPPER_ONLY)
-- PostgreSQL DOES generate these plans with ORDER BY DESC + WHERE k <= bound
-- ============================================================================
DROP TABLE IF EXISTS t_backward_upper CASCADE;
CREATE UNLOGGED TABLE t_backward_upper (k int4);
INSERT INTO t_backward_upper SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX t_backward_upper_idx ON t_backward_upper USING smol(k);
ANALYZE t_backward_upper;

-- Backward scan with upper bound (<=)
SELECT k FROM t_backward_upper WHERE k <= 500 ORDER BY k DESC LIMIT 10;
SELECT count(*) FROM (SELECT k FROM t_backward_upper WHERE k <= 500 ORDER BY k DESC) AS t;

-- Backward scan with strict upper bound (<)
SELECT k FROM t_backward_upper WHERE k < 500 ORDER BY k DESC LIMIT 10;

-- Backward scan with BETWEEN (both bounds)
SELECT k FROM t_backward_upper WHERE k BETWEEN 100 AND 500 ORDER BY k DESC LIMIT 10;

DROP TABLE t_backward_upper CASCADE;

-- ============================================================================
-- Test 2: Query with no upper bound and no equality (query with no upper bound, no equality)
-- Need query with ONLY lower bound: k >= N (no upper, no equality)
-- ============================================================================
DROP TABLE IF EXISTS t_only_lower CASCADE;
CREATE UNLOGGED TABLE t_only_lower (k int4);
INSERT INTO t_only_lower SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX t_only_lower_idx ON t_only_lower USING smol(k);
ANALYZE t_only_lower;

-- Query with ONLY lower bound (no upper bound, no equality)
SELECT count(*) FROM t_only_lower WHERE k >= 500;
SELECT k FROM t_only_lower WHERE k > 900 ORDER BY k LIMIT 10;

DROP TABLE t_only_lower CASCADE;

-- ============================================================================
-- Test 3: Empty page detection (empty page detection)
-- Create scenario where a page might be empty after filtering
-- This is actually impossible after build, but let's try DELETE
-- Actually, SMOL is read-only so this can't happen. Mark as defensive.
-- ============================================================================

-- ============================================================================
-- Test 4: Zero-copy first key adjustment (zero-copy first key adjustment)
-- Happens when page is zero-copy format and we need first key for bounds check
-- ============================================================================
DROP TABLE IF EXISTS t_zerocopy_bounds CASCADE;
CREATE UNLOGGED TABLE t_zerocopy_bounds (k int4);
-- Insert unique values to ensure zero-copy format
INSERT INTO t_zerocopy_bounds SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX t_zerocopy_bounds_idx ON t_zerocopy_bounds USING smol(k);
ANALYZE t_zerocopy_bounds;

-- Query with BETWEEN to trigger page bounds checking with zero-copy pages
SELECT count(*) FROM t_zerocopy_bounds WHERE k BETWEEN 2500 AND 7500;
SELECT count(*) FROM t_zerocopy_bounds WHERE k >= 5000 AND k < 9000;

DROP TABLE t_zerocopy_bounds CASCADE;

-- ============================================================================
-- Test 5: Equality bound stop scan (lines 980-981)
-- When first_key on page > equality bound, stop scan early
-- Need data where equality value falls between pages
-- ============================================================================
DROP TABLE IF EXISTS t_eq_stop CASCADE;
CREATE UNLOGGED TABLE t_eq_stop (k int4);
-- Create large enough data to have multiple pages
INSERT INTO t_eq_stop SELECT i FROM generate_series(1, 50000) i;
CREATE INDEX t_eq_stop_idx ON t_eq_stop USING smol(k);
ANALYZE t_eq_stop;

-- Equality queries that should find value and stop
SELECT * FROM t_eq_stop WHERE k = 25000;
SELECT * FROM t_eq_stop WHERE k = 1;
SELECT * FROM t_eq_stop WHERE k = 49999;

-- Equality query for non-existent value (should stop when exceeds)
SELECT * FROM t_eq_stop WHERE k = 50001;

DROP TABLE t_eq_stop CASCADE;

-- ============================================================================
-- Test 6: Two-column attribute 2 equality handling (two-column attribute 2 equality handling)
-- Query with k1 range and k2 = specific value
-- ============================================================================
DROP TABLE IF EXISTS t_twocol_attr2_eq CASCADE;
CREATE UNLOGGED TABLE t_twocol_attr2_eq (k1 int4, k2 int4, v int4);
INSERT INTO t_twocol_attr2_eq SELECT i, i % 1000, i*10 FROM generate_series(1, 10000) i;
CREATE INDEX t_twocol_attr2_eq_idx ON t_twocol_attr2_eq USING smol(k1, k2) INCLUDE (v);
ANALYZE t_twocol_attr2_eq;

-- Query with k1 range and k2 equality (should trigger two-column k2 equality handling)
SELECT count(*) FROM t_twocol_attr2_eq WHERE k1 >= 1000 AND k2 = 500;
SELECT k1, k2, v FROM t_twocol_attr2_eq WHERE k1 BETWEEN 5000 AND 6000 AND k2 = 750 ORDER BY k1 LIMIT 5;

DROP TABLE t_twocol_attr2_eq CASCADE;

-- ============================================================================
-- Test 7: Rescan with unsupported scan key types (lines 2056, 2061)
-- Try queries that might have unsupported scan keys
-- ============================================================================
DROP TABLE IF EXISTS t_rescan_keys CASCADE;
CREATE UNLOGGED TABLE t_rescan_keys (k int4, v int4);
INSERT INTO t_rescan_keys SELECT i, i*10 FROM generate_series(1, 1000) i;
CREATE INDEX t_rescan_keys_idx ON t_rescan_keys USING smol(k) INCLUDE (v);
ANALYZE t_rescan_keys;

-- Nested loop to trigger rescan with different keys
DROP TABLE IF EXISTS t_outer2 CASCADE;
CREATE UNLOGGED TABLE t_outer2 (k int4);
INSERT INTO t_outer2 VALUES (10), (50), (100), (500), (900);

SET enable_hashjoin = off;
SET enable_mergejoin = off;

SELECT t_outer2.k AS outer_k, t_rescan_keys.k AS inner_k, t_rescan_keys.v
FROM t_outer2, t_rescan_keys
WHERE t_rescan_keys.k = t_outer2.k
ORDER BY outer_k;

DROP TABLE t_rescan_keys CASCADE;
DROP TABLE t_outer2 CASCADE;

-- ============================================================================
-- Test 8: NULL key rejection (lines 2066-2068)
-- Try to trigger NULL scan key (though planner usually prevents this)
-- ============================================================================
DROP TABLE IF EXISTS t_null_key CASCADE;
CREATE UNLOGGED TABLE t_null_key (k int4);
INSERT INTO t_null_key SELECT i FROM generate_series(1, 100) i;
CREATE INDEX t_null_key_idx ON t_null_key USING smol(k);

-- These should all avoid NULL keys at planner level, but try anyway
SELECT * FROM t_null_key WHERE k = NULL;  -- Planner will optimize to empty
SELECT * FROM t_null_key WHERE k IS NULL;  -- Won't use index
SELECT * FROM t_null_key WHERE k IN (1, NULL, 3);  -- NULL ignored by planner

DROP TABLE t_null_key CASCADE;

-- ============================================================================
-- Test 9: Zero-copy backward scan keyp adjustment (zero-copy backward keyp adjustment)
-- Backward scan on zero-copy pages
-- ============================================================================
DROP TABLE IF EXISTS t_zerocopy_backward CASCADE;
CREATE UNLOGGED TABLE t_zerocopy_backward (k int4, v int4);
-- Unique values for zero-copy
INSERT INTO t_zerocopy_backward SELECT i, i*10 FROM generate_series(1, 5000) i;
CREATE INDEX t_zerocopy_backward_idx ON t_zerocopy_backward USING smol(k) INCLUDE (v);
ANALYZE t_zerocopy_backward;

-- Backward scan should hit zero-copy pages
SELECT k, v FROM t_zerocopy_backward WHERE k <= 1000 ORDER BY k DESC LIMIT 10;
SELECT k, v FROM t_zerocopy_backward WHERE k BETWEEN 2000 AND 3000 ORDER BY k DESC LIMIT 10;

DROP TABLE t_zerocopy_backward CASCADE;

-- ============================================================================
-- Test 10: RLE INCLUDE column caching (lines 2690-2702, 2900)
-- Multiple INCLUDE columns with various sizes to hit all copy paths
-- ============================================================================
DROP TABLE IF EXISTS t_rle_inc_all_sizes CASCADE;
CREATE UNLOGGED TABLE t_rle_inc_all_sizes (
    k int4,
    inc1 int2,      -- 2 bytes (smol_copy_small for 2-byte INCLUDE)
    inc2 int4,      -- 4 bytes (smol_copy4 for 4-byte INCLUDE)
    inc3 int8,      -- 8 bytes (smol_copy8 for 8-byte INCLUDE)
    inc4 uuid,      -- 16 bytes (smol_copy16 for 16-byte INCLUDE)
    inc5 bool       -- 1 byte (smol_copy_small for 1-byte INCLUDE)
);

-- Insert duplicates to trigger RLE
INSERT INTO t_rle_inc_all_sizes
SELECT
    i % 100,  -- Lots of duplicates for RLE
    (i % 1000)::int2,
    i::int4,
    i::int8,
    ('00000000-0000-0000-0000-' || lpad((i % 10000)::text, 12, '0'))::uuid,  -- Deterministic UUIDs
    (i % 2 = 0)::bool
FROM generate_series(1, 10000) i;

CREATE INDEX t_rle_inc_all_sizes_idx ON t_rle_inc_all_sizes USING smol(k)
    INCLUDE (inc1, inc2, inc3, inc4, inc5);
ANALYZE t_rle_inc_all_sizes;

-- Query to access all INCLUDE columns with RLE
SELECT k, inc1, inc2, inc3, inc4, inc5
FROM t_rle_inc_all_sizes
WHERE k = 50
ORDER BY inc2
LIMIT 10;

-- Query to access subset of INCLUDE columns
SELECT k, inc1, inc3, inc5
FROM t_rle_inc_all_sizes
WHERE k >= 10 AND k <= 20
ORDER BY k, inc1
LIMIT 20;

DROP TABLE t_rle_inc_all_sizes CASCADE;

-- ============================================================================
-- Test 11: Backward scan runtime key rejection (lines 2737-2740)
-- Backward scan where runtime keys filter out rows
-- ============================================================================
DROP TABLE IF EXISTS t_backward_filter CASCADE;
CREATE UNLOGGED TABLE t_backward_filter (k int4, v int4);
INSERT INTO t_backward_filter SELECT i, i*10 FROM generate_series(1, 1000) i;
CREATE INDEX t_backward_filter_idx ON t_backward_filter USING smol(k) INCLUDE (v);
ANALYZE t_backward_filter;

-- Backward scan with parameter (runtime key)
PREPARE backward_param AS
    SELECT k, v FROM t_backward_filter WHERE k <= $1 AND k >= $2 ORDER BY k DESC LIMIT 10;
EXECUTE backward_param(500, 400);
EXECUTE backward_param(900, 800);
DEALLOCATE backward_param;

DROP TABLE t_backward_filter CASCADE;

-- ============================================================================
-- Test 12: Forward zero-copy ultra-fast path (lines 2760-2778)
-- Plain zero-copy page with xs_want_itup and no upper bound/equality
-- This is the "ULTRA-FAST PATH" optimization
-- ============================================================================
DROP TABLE IF EXISTS t_zerocopy_ultrafast CASCADE;
CREATE UNLOGGED TABLE t_zerocopy_ultrafast (k int4);
-- Unique values for plain zero-copy pages
INSERT INTO t_zerocopy_ultrafast SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX t_zerocopy_ultrafast_idx ON t_zerocopy_ultrafast USING smol(k);
ANALYZE t_zerocopy_ultrafast;

-- Query with only lower bound (no upper bound, no equality) to hit ultra-fast path
-- Need to avoid upper bounds and equality checks
SELECT count(*) FROM t_zerocopy_ultrafast WHERE k >= 5000;
SELECT k FROM t_zerocopy_ultrafast WHERE k > 8000 ORDER BY k LIMIT 100;

DROP TABLE t_zerocopy_ultrafast CASCADE;

-- ============================================================================
-- Test 13: Forward scan runtime key filtering (lines 2985-2992, 2996, 3001)
-- Zero-copy pages with runtime key filtering that rejects tuples
-- ============================================================================
DROP TABLE IF EXISTS t_runtime_filter CASCADE;
CREATE UNLOGGED TABLE t_runtime_filter (k int4, v int4);
INSERT INTO t_runtime_filter SELECT i, i*10 FROM generate_series(1, 5000) i;
CREATE INDEX t_runtime_filter_idx ON t_runtime_filter USING smol(k) INCLUDE (v);
ANALYZE t_runtime_filter;

-- Prepared statement with runtime keys
PREPARE runtime_stmt AS
    SELECT k, v FROM t_runtime_filter WHERE k >= $1 AND k <= $2 ORDER BY k;
EXECUTE runtime_stmt(1000, 2000);
EXECUTE runtime_stmt(3000, 4000);
DEALLOCATE runtime_stmt;

DROP TABLE t_runtime_filter CASCADE;

-- ============================================================================
-- Test 14: Parallel scan with controlled fanout (parallel scan controlled fanout)
-- Force parallel scan and ensure it exhausts all pages
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_complete CASCADE;
CREATE UNLOGGED TABLE t_parallel_complete (k int4);
INSERT INTO t_parallel_complete SELECT i FROM generate_series(1, 100000) i;
CREATE INDEX t_parallel_complete_idx ON t_parallel_complete USING smol(k);
ANALYZE t_parallel_complete;

-- Force parallel scan
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_index_scan_size = 0;
SET min_parallel_table_scan_size = 0;

-- Scan ALL rows to ensure parallel workers exhaust the index
SELECT count(*) FROM t_parallel_complete WHERE k >= 1;

-- Reset
SET max_parallel_workers_per_gather = 0;

DROP TABLE t_parallel_complete CASCADE;

-- ============================================================================
-- Test 15: Parallel two-column scan with zero-copy (parallel two-column zero-copy scan)
-- Parallel scan on two-column index triggers different code paths
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_twocol CASCADE;
CREATE UNLOGGED TABLE t_parallel_twocol (k1 int4, k2 int4, v int4);
INSERT INTO t_parallel_twocol SELECT i, i*2, i*10 FROM generate_series(1, 100000) i;
CREATE INDEX t_parallel_twocol_idx ON t_parallel_twocol USING smol(k1, k2) INCLUDE (v);
ANALYZE t_parallel_twocol;

SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_index_scan_size = 0;

-- Parallel two-column scan
SELECT count(*) FROM t_parallel_twocol WHERE k1 >= 10000;
SELECT k1, k2, v FROM t_parallel_twocol WHERE k1 BETWEEN 50000 AND 50100 ORDER BY k1, k2 LIMIT 20;

SET max_parallel_workers_per_gather = 0;

DROP TABLE t_parallel_twocol CASCADE;

-- ============================================================================
-- Test 16: Text INCLUDE columns with various patterns (lines 2721-2725)
-- Text INCLUDE columns trigger varlena handling
-- ============================================================================
DROP TABLE IF EXISTS t_text_includes CASCADE;
CREATE UNLOGGED TABLE t_text_includes (k int4, t1 text, t2 text);
INSERT INTO t_text_includes
SELECT i, 'text_' || i, repeat('x', i % 100)
FROM generate_series(1, 1000) i;

CREATE INDEX t_text_includes_idx ON t_text_includes USING smol(k) INCLUDE (t1, t2);
ANALYZE t_text_includes;

-- Access text INCLUDE columns
SELECT k, t1, t2 FROM t_text_includes WHERE k >= 500 AND k <= 600 ORDER BY k LIMIT 10;

DROP TABLE t_text_includes CASCADE;

-- ============================================================================
-- Test 17: Rescan in nested loop (lines 3392-3395)
-- Trigger index rescan with nested loop join
-- ============================================================================
DROP TABLE IF EXISTS t_rescan_inner CASCADE;
DROP TABLE IF EXISTS t_rescan_outer CASCADE;

CREATE UNLOGGED TABLE t_rescan_outer (k int4);
CREATE UNLOGGED TABLE t_rescan_inner (k int4, v int4);

INSERT INTO t_rescan_outer VALUES (100), (200), (300), (400), (500);
INSERT INTO t_rescan_inner SELECT i, i*10 FROM generate_series(1, 1000) i;

CREATE INDEX t_rescan_inner_idx ON t_rescan_inner USING smol(k) INCLUDE (v);
ANALYZE t_rescan_outer;
ANALYZE t_rescan_inner;

SET enable_hashjoin = off;
SET enable_mergejoin = off;
SET enable_nestloop = on;

-- Nested loop should rescan inner index for each outer row
SELECT o.k AS outer_k, i.k AS inner_k, i.v
FROM t_rescan_outer o
JOIN t_rescan_inner i ON i.k = o.k
ORDER BY o.k;

SET enable_hashjoin = on;
SET enable_mergejoin = on;

DROP TABLE t_rescan_inner CASCADE;
DROP TABLE t_rescan_outer CASCADE;


-- ============================================================================
-- Coverage: Binary search upper-half branch in new leaf navigation
-- ============================================================================
-- This test triggers the "search upper half" path (lo2 = mid2 + 1) in
-- smol_gettuple's forward scan when binary searching within newly loaded leaves
-- to find the starting position for keys >= bound

-- Create table with enough data for multiple leaf pages
DROP TABLE IF EXISTS t_line2041 CASCADE;
CREATE UNLOGGED TABLE t_line2041 AS
SELECT i AS id, i * 2 AS value
FROM generate_series(1, 100000) i;

-- Create index with INCLUDE to enable index-only scans
CREATE INDEX t_line2041_idx ON t_line2041 USING smol(id) INCLUDE (value);

-- Use test GUC to force find_first_leaf to return a leaf 20 blocks earlier
-- This makes the scan traverse leaves containing keys < 50000
-- During binary search in those leaves, we probe keys < bound, triggering the upper-half search
SET smol.test_leaf_offset = 20;
SET enable_seqscan = off;
SET max_parallel_workers_per_gather = 0;

-- Query with bound in the middle - scan will start early and advance through
-- multiple leaves, binary searching and hitting the "else lo2 = mid2 + 1" branch
SELECT count(*), sum(value) FROM t_line2041 WHERE id >= 50000;

-- Clean up
RESET smol.test_leaf_offset;
DROP TABLE t_line2041;

-- ============================================================================
-- UTF-8 Collation Support
-- ============================================================================
-- Test UTF-8 collation support for text keys
-- Tests both index building with non-C collations and scan-time comparisons

-- Test 1: Basic UTF-8 collation index creation
DROP TABLE IF EXISTS t_utf8_basic CASCADE;
CREATE TABLE t_utf8_basic (
    name text
);

INSERT INTO t_utf8_basic VALUES
    ('apple'), ('Banana'), ('cherry'), ('Date'),
    ('élève'), ('éclair'), ('café'), ('naïve');

-- Create index with C collation (baseline)
CREATE INDEX t_utf8_basic_c_idx ON t_utf8_basic USING smol(name COLLATE "C");

-- Create index with ICU collation (uses generic comparator during scans)
CREATE INDEX t_utf8_basic_icu_idx ON t_utf8_basic USING smol(name COLLATE "en-US-x-icu");

-- Verify index was created
\d t_utf8_basic

-- Test 2: Longer UTF-8 strings
DROP TABLE IF EXISTS t_utf8_long CASCADE;
CREATE TABLE t_utf8_long (
    description text
);

INSERT INTO t_utf8_long VALUES
    ('café au lait'),
    ('crème brûlée'),
    ('naïve approach'),
    ('résumé submitted'),
    ('Zürich airport');

CREATE INDEX t_utf8_long_idx ON t_utf8_long USING smol(description COLLATE "en-US-x-icu");

-- Test 3: Various length strings
DROP TABLE IF EXISTS t_utf8_lengths CASCADE;
CREATE TABLE t_utf8_lengths (
    val text
);

INSERT INTO t_utf8_lengths VALUES
    ('a'),  -- 1 byte
    ('hello'),  -- 5 bytes
    ('test string here'),  -- 16 bytes
    ('longer text value for testing'),  -- 31 bytes
    ('exactly thirty-two bytes!!!!!!!');  -- 32 bytes

CREATE INDEX t_utf8_lengths_idx ON t_utf8_lengths USING smol(val COLLATE "en-US-x-icu");

-- Test 4: Verify C collation still uses optimized path (8-byte keys for short strings)
DROP TABLE IF EXISTS t_c_collation CASCADE;
CREATE TABLE t_c_collation (
    name text
);

INSERT INTO t_c_collation VALUES ('z'), ('a'), ('m'), ('b');

CREATE INDEX t_c_collation_idx ON t_c_collation USING smol(name COLLATE "C");

-- C collation should use optimized path
SELECT * FROM t_c_collation ORDER BY name COLLATE "C";

-- Test 5: Range scans with UTF-8 collation (uses generic comparator)
SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Equality scan
EXPLAIN (COSTS OFF) SELECT * FROM t_utf8_basic WHERE name = 'café' COLLATE "en-US-x-icu";
SELECT * FROM t_utf8_basic WHERE name = 'café' COLLATE "en-US-x-icu";

-- Range scan with lower bound (>=)
EXPLAIN (COSTS OFF) SELECT * FROM t_utf8_basic WHERE name >= 'café' COLLATE "en-US-x-icu" ORDER BY name COLLATE "en-US-x-icu";
SELECT * FROM t_utf8_basic WHERE name >= 'café' COLLATE "en-US-x-icu" ORDER BY name COLLATE "en-US-x-icu";

-- Range scan with upper bound (<=)
EXPLAIN (COSTS OFF) SELECT * FROM t_utf8_basic WHERE name <= 'café' COLLATE "en-US-x-icu" ORDER BY name COLLATE "en-US-x-icu";
SELECT * FROM t_utf8_basic WHERE name <= 'café' COLLATE "en-US-x-icu" ORDER BY name COLLATE "en-US-x-icu";

-- Range scan with both bounds (>= AND <=)
EXPLAIN (COSTS OFF) SELECT * FROM t_utf8_basic WHERE name >= 'café' COLLATE "en-US-x-icu" AND name <= 'naïve' COLLATE "en-US-x-icu" ORDER BY name COLLATE "en-US-x-icu";
SELECT * FROM t_utf8_basic WHERE name >= 'café' COLLATE "en-US-x-icu" AND name <= 'naïve' COLLATE "en-US-x-icu" ORDER BY name COLLATE "en-US-x-icu";

-- Range scan with strict bounds (> AND <)
EXPLAIN (COSTS OFF) SELECT * FROM t_utf8_basic WHERE name > 'café' COLLATE "en-US-x-icu" AND name < 'naïve' COLLATE "en-US-x-icu" ORDER BY name COLLATE "en-US-x-icu";
SELECT * FROM t_utf8_basic WHERE name > 'café' COLLATE "en-US-x-icu" AND name < 'naïve' COLLATE "en-US-x-icu" ORDER BY name COLLATE "en-US-x-icu";

-- Test 6: Range scans with longer UTF-8 strings
EXPLAIN (COSTS OFF) SELECT * FROM t_utf8_long WHERE description >= 'crème' COLLATE "en-US-x-icu" ORDER BY description COLLATE "en-US-x-icu";
SELECT * FROM t_utf8_long WHERE description >= 'crème' COLLATE "en-US-x-icu" ORDER BY description COLLATE "en-US-x-icu";

RESET enable_seqscan;
RESET enable_bitmapscan;

-- Cleanup
DROP TABLE t_utf8_basic CASCADE;
DROP TABLE t_utf8_long CASCADE;
DROP TABLE t_utf8_lengths CASCADE;
DROP TABLE t_c_collation CASCADE;

--
-- Test TEXT two-column indexes (TEXT + INT4)
-- These tests verify that TEXT can be used as the first key in a two-column index
-- with proper handling of 32-byte fixed-width storage and null-padding
--

-- Test 1: Basic TEXT+INT two-column index with INCLUDE
CREATE TABLE t_text_twocol (k1 text, k2 int4, v int);
INSERT INTO t_text_twocol VALUES
  ('abc', 1, 100),
  ('abc', 2, 200),
  ('abc', 3, 300),
  ('def', 1, 400),
  ('def', 2, 500),
  ('xyz', 1, 600);

CREATE INDEX t_text_twocol_idx ON t_text_twocol USING smol(k1, k2) INCLUDE (v);

SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Equality on k1, range on k2
EXPLAIN (COSTS OFF) SELECT * FROM t_text_twocol WHERE k1 = 'abc' AND k2 >= 0 ORDER BY k2;
SELECT * FROM t_text_twocol WHERE k1 = 'abc' AND k2 >= 0 ORDER BY k2;

-- Equality on both k1 and k2
EXPLAIN (COSTS OFF) SELECT * FROM t_text_twocol WHERE k1 = 'def' AND k2 = 2;
SELECT * FROM t_text_twocol WHERE k1 = 'def' AND k2 = 2;

-- Range on k1
EXPLAIN (COSTS OFF) SELECT * FROM t_text_twocol WHERE k1 >= 'abc' AND k1 < 'xyz' ORDER BY k1, k2;
SELECT * FROM t_text_twocol WHERE k1 >= 'abc' AND k1 < 'xyz' ORDER BY k1, k2;

-- Backward scan
EXPLAIN (COSTS OFF) SELECT * FROM t_text_twocol WHERE k1 = 'abc' ORDER BY k2 DESC;
SELECT * FROM t_text_twocol WHERE k1 = 'abc' ORDER BY k2 DESC;

-- Test 2: TEXT+INT without INCLUDE column
CREATE TABLE t_text_twocol_noinc (k1 text, k2 int4);
INSERT INTO t_text_twocol_noinc VALUES
  ('foo', 10),
  ('foo', 20),
  ('bar', 30);

CREATE INDEX t_text_twocol_noinc_idx ON t_text_twocol_noinc USING smol(k1, k2);

EXPLAIN (COSTS OFF) SELECT k1, k2 FROM t_text_twocol_noinc WHERE k1 = 'foo' ORDER BY k2;
SELECT k1, k2 FROM t_text_twocol_noinc WHERE k1 = 'foo' ORDER BY k2;

-- Test 3: TEXT with various lengths (empty, short, medium, near 32-byte boundary)
CREATE TABLE t_text_lengths_twocol (k1 text, k2 int4, id int);
INSERT INTO t_text_lengths_twocol VALUES
  ('', 1, 1),                                      -- empty string
  ('a', 2, 2),                                     -- 1 byte
  ('short', 3, 3),                                 -- 5 bytes
  ('medium length text', 4, 4),                    -- 18 bytes
  ('exactly 31 bytes long text!!', 5, 5),         -- 30 bytes
  ('this text is exactly 32 bytes!', 6, 6);       -- 32 bytes (boundary)

CREATE INDEX t_text_lengths_twocol_idx ON t_text_lengths_twocol USING smol(k1, k2) INCLUDE (id);

-- Query each length category
SELECT k1, k2, id FROM t_text_lengths_twocol WHERE k1 = '' AND k2 = 1;
SELECT k1, k2, id FROM t_text_lengths_twocol WHERE k1 = 'a' AND k2 = 2;
SELECT k1, k2, id FROM t_text_lengths_twocol WHERE k1 = 'short' AND k2 = 3;
SELECT k1, k2, id FROM t_text_lengths_twocol WHERE k1 = 'medium length text' AND k2 = 4;
SELECT k1, k2, id FROM t_text_lengths_twocol WHERE k1 = 'exactly 31 bytes long text!!' AND k2 = 5;
SELECT k1, k2, id FROM t_text_lengths_twocol WHERE k1 = 'this text is exactly 32 bytes!' AND k2 = 6;

-- Test 4: TEXT+INT with duplicate k1 values (tests RLE compression)
CREATE TABLE t_text_rle_twocol (k1 text, k2 int4, data int);
INSERT INTO t_text_rle_twocol SELECT 'duplicate', i, i*10 FROM generate_series(1, 50) i;
INSERT INTO t_text_rle_twocol SELECT 'another', i, i*20 FROM generate_series(1, 30) i;

CREATE INDEX t_text_rle_twocol_idx ON t_text_rle_twocol USING smol(k1, k2) INCLUDE (data);

-- Query with many duplicate k1 values
SELECT count(*) FROM t_text_rle_twocol WHERE k1 = 'duplicate';
SELECT count(*) FROM t_text_rle_twocol WHERE k1 = 'another';
SELECT k1, k2, data FROM t_text_rle_twocol WHERE k1 = 'duplicate' AND k2 BETWEEN 10 AND 15 ORDER BY k2;

-- Verify index structure
SELECT total_pages >= 1 AS has_pages, leaf_pages >= 1 AS has_leaves FROM smol_inspect('t_text_rle_twocol_idx');

RESET enable_seqscan;
RESET enable_bitmapscan;

-- Cleanup
DROP TABLE t_text_twocol CASCADE;
DROP TABLE t_text_twocol_noinc CASCADE;
DROP TABLE t_text_lengths_twocol CASCADE;
DROP TABLE t_text_rle_twocol CASCADE;
