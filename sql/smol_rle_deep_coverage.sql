-- Deep RLE coverage for remaining uncovered lines
-- Targets: 3565, 3588-3589, 3593, 3641, 3666-3667, 3671, 3713, 3718-3720

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- Test 1: Key-only RLE (0x8001) with INCLUDE columns present
-- This triggers lines 3718-3720: accessing includes when tag is 0x8001
-- ============================================================================

DROP TABLE IF EXISTS t_key_rle_with_inc CASCADE;
CREATE UNLOGGED TABLE t_key_rle_with_inc (k int8, v1 int4, v2 int4);

-- Insert data that creates key-only RLE
-- When keys are highly repetitive but we have includes, we might get 0x8001 + columnar includes
INSERT INTO t_key_rle_with_inc
SELECT
    ((i / 1000) % 10)::int8 AS k,
    i AS v1,
    i * 2 AS v2
FROM generate_series(1, 50000) i;

CREATE INDEX t_key_rle_with_inc_smol ON t_key_rle_with_inc USING smol(k) INCLUDE (v1, v2);

-- Access data to trigger include pointer access in key-RLE format
SELECT k, v1, v2 FROM t_key_rle_with_inc WHERE k = 5 ORDER BY v1 LIMIT 10;
SELECT k, count(*), sum(v1::int8), sum(v2::int8) FROM t_key_rle_with_inc WHERE k >= 3 GROUP BY k ORDER BY k;

-- ============================================================================
-- Test 2: RLE with out-of-range index access (line 3565)
-- Need to create a situation where we scan beyond valid offsets
-- ============================================================================

DROP TABLE IF EXISTS t_rle_boundary_scan CASCADE;
CREATE UNLOGGED TABLE t_rle_boundary_scan (k int8, v int4);

-- Create small RLE runs
INSERT INTO t_rle_boundary_scan
SELECT
    ((i / 10) % 100)::int8 AS k,
    i AS v
FROM generate_series(1, 1000) i;

CREATE INDEX t_rle_boundary_scan_smol ON t_rle_boundary_scan USING smol(k) INCLUDE (v);

-- Scan to potentially trigger boundary conditions
SELECT k, v FROM t_rle_boundary_scan WHERE k >= 95 ORDER BY k, v;
SELECT k, count(*) FROM t_rle_boundary_scan GROUP BY k HAVING count(*) > 5 ORDER BY k;

-- ============================================================================
-- Test 3: RLE key-RLE with single include (to hit specific code paths)
-- ============================================================================

DROP TABLE IF EXISTS t_keyrle_single_inc CASCADE;
CREATE UNLOGGED TABLE t_keyrle_single_inc (k int4, v int2);

-- Small include (int2 = 2 bytes) with repetitive keys
INSERT INTO t_keyrle_single_inc
SELECT
    ((i / 500) % 50)::int4 AS k,
    (i % 32767)::int2 AS v
FROM generate_series(1, 25000) i;

CREATE INDEX t_keyrle_single_inc_smol ON t_keyrle_single_inc USING smol(k) INCLUDE (v);

-- Query to access includes in potential key-RLE format
SELECT k, min(v), max(v), count(*) FROM t_keyrle_single_inc WHERE k BETWEEN 10 AND 15 GROUP BY k ORDER BY k;

-- ============================================================================
-- Test 4: Multi-run RLE with many small runs (to test run iteration)
-- ============================================================================

DROP TABLE IF EXISTS t_multi_run CASCADE;
CREATE UNLOGGED TABLE t_multi_run (k int8, v1 int4, v2 int4, v3 int4);

-- Create many distinct runs (small runs, many of them)
INSERT INTO t_multi_run
SELECT
    ((i / 20) % 500)::int8 AS k,
    i AS v1,
    i * 2 AS v2,
    i * 3 AS v3
FROM generate_series(1, 10000) i;

CREATE INDEX t_multi_run_smol ON t_multi_run USING smol(k) INCLUDE (v1, v2, v3);

-- Access different parts of multi-run RLE
SELECT k, v1, v2, v3 FROM t_multi_run WHERE k = 250 ORDER BY v1 LIMIT 5;
SELECT k, v1, v2, v3 FROM t_multi_run WHERE k = 100 ORDER BY v1 LIMIT 5;
SELECT k, count(*) FROM t_multi_run WHERE k >= 400 GROUP BY k ORDER BY k LIMIT 10;

-- ============================================================================
-- Test 5: RLE with varying run lengths to test run detection edge cases
-- ============================================================================

DROP TABLE IF EXISTS t_varied_runs CASCADE;
CREATE UNLOGGED TABLE t_varied_runs (k int8, v1 int4, v2 int4);

-- Run 1: 100 rows
INSERT INTO t_varied_runs SELECT 1000, generate_series, generate_series * 2 FROM generate_series(1, 100);
-- Run 2: 1 row (singleton)
INSERT INTO t_varied_runs VALUES (2000, 101, 202);
-- Run 3: 500 rows
INSERT INTO t_varied_runs SELECT 3000, generate_series, generate_series * 2 FROM generate_series(102, 601);
-- Run 4: 2 rows
INSERT INTO t_varied_runs VALUES (4000, 602, 1204), (4000, 603, 1206);
-- Run 5: 1000 rows
INSERT INTO t_varied_runs SELECT 5000, generate_series, generate_series * 2 FROM generate_series(604, 1603);

CREATE INDEX t_varied_runs_smol ON t_varied_runs USING smol(k) INCLUDE (v1, v2);

-- Query each run type
SELECT k, count(*), min(v1), max(v1) FROM t_varied_runs WHERE k = 1000 GROUP BY k;  -- Medium run
SELECT k, count(*), min(v1), max(v1) FROM t_varied_runs WHERE k = 2000 GROUP BY k;  -- Singleton
SELECT k, count(*), min(v1), max(v1) FROM t_varied_runs WHERE k = 3000 GROUP BY k;  -- Large run
SELECT k, count(*), min(v1), max(v1) FROM t_varied_runs WHERE k = 4000 GROUP BY k;  -- Tiny run
SELECT k, count(*), min(v1), max(v1) FROM t_varied_runs WHERE k = 5000 GROUP BY k;  -- Very large run

-- Scan across multiple runs
SELECT k, v1, v2 FROM t_varied_runs WHERE k >= 3000 AND v1 < 110 ORDER BY k, v1;

-- ============================================================================
-- Test 6: RLE with text keys and multiple include columns
-- Test run detection with text (lines 3645, 3671)
-- ============================================================================

DROP TABLE IF EXISTS t_text_multirle CASCADE;
CREATE UNLOGGED TABLE t_text_multirle (k text COLLATE "C", v1 int4, v2 int4, v3 int2);

-- Create multiple runs with text keys
INSERT INTO t_text_multirle
SELECT
    'textkey' || lpad(((i / 30) % 100)::text, 3, '0') AS k,
    i AS v1,
    i * 2 AS v2,
    (i % 30000)::int2 AS v3
FROM generate_series(1, 3000) i;

CREATE INDEX t_text_multirle_smol ON t_text_multirle USING smol(k) INCLUDE (v1, v2, v3);

-- Query to test text run detection
SELECT k, count(*), min(v1) FROM t_text_multirle WHERE k >= 'textkey050' AND k <= 'textkey055' GROUP BY k ORDER BY k;
SELECT k, v1, v2, v3 FROM t_text_multirle WHERE k = 'textkey025' ORDER BY v1 LIMIT 5;

-- ============================================================================
-- Test 7: Key-RLE (0x8001) without includes - pure key compression
-- ============================================================================

DROP TABLE IF EXISTS t_pure_key_rle CASCADE;
CREATE UNLOGGED TABLE t_pure_key_rle (k int8);

-- Extremely repetitive keys, no includes
INSERT INTO t_pure_key_rle
SELECT ((i / 10000) % 5)::int8 FROM generate_series(1, 50000) i;

CREATE INDEX t_pure_key_rle_smol ON t_pure_key_rle USING smol(k);

-- Query pure key RLE
SELECT k, count(*) FROM t_pure_key_rle GROUP BY k ORDER BY k;
SELECT k FROM t_pure_key_rle WHERE k = 2 LIMIT 100;

-- Cleanup
DROP TABLE t_key_rle_with_inc CASCADE;
DROP TABLE t_rle_boundary_scan CASCADE;
DROP TABLE t_keyrle_single_inc CASCADE;
DROP TABLE t_multi_run CASCADE;
DROP TABLE t_varied_runs CASCADE;
DROP TABLE t_text_multirle CASCADE;
DROP TABLE t_pure_key_rle CASCADE;
