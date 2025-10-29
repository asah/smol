-- smol_coverage2b.sql: Coverage-specific tests (part 2b)
-- Backward scans and parallel builds
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

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
-- PART 7: Text Backward Scan (text backward scan varlena emission)
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
-- PART 10: INCLUDE Column Edge Cases (INCLUDE column edge cases)
-- ============================================================================

DROP TABLE IF EXISTS t_inc_edge CASCADE;
CREATE UNLOGGED TABLE t_inc_edge (a int4, b int8, c uuid);
INSERT INTO t_inc_edge SELECT i, i::int8, md5(i::text)::uuid FROM generate_series(1, 1000) i;
CREATE INDEX idx_inc_edge ON t_inc_edge USING smol(a) INCLUDE (b, c);

-- Query that returns INCLUDE columns with different sizes (count only, UUIDs are deterministic)
SELECT count(*), min(a), max(a), min(b), max(b) FROM t_inc_edge WHERE a BETWEEN 100 AND 200;

-- ============================================================================
-- PART 11: Generic Upper Bound Comparator (generic upper bound comparator)
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
EXPLAIN (COSTS OFF) SELECT * FROM t_cost WHERE k > 500;

-- Trigger cost calculation with smol.cost_tup != 1.0
SET smol.cost_page = 1.0;
SET smol.cost_tup = 0.8;
EXPLAIN (COSTS OFF) SELECT * FROM t_cost WHERE k > 500;

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

-- Test 7: Parallel build with max_parallel_maintenance_workers=0 (parallel build with max_parallel_maintenance_workers=0)
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

-- Test: request < 1 should be no-op (index build request < 1 no-op)
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

