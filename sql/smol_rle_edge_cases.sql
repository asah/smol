-- Comprehensive RLE edge case testing
-- Targets specific uncovered lines in RLE implementation

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- Test 1: int32 (4-byte) key Include-RLE (line 3169: else if key_len == 4)
-- ============================================================================

DROP TABLE IF EXISTS t_rle_int4 CASCADE;
CREATE UNLOGGED TABLE t_rle_int4 (k int4, v1 int4, v2 int4);

-- Insert repetitive int4 data to trigger RLE
INSERT INTO t_rle_int4
SELECT
    ((i / 100) % 100000)::int4 AS k,
    ((i / 100))::int4 AS v1,
    ((i / 100) * 2)::int4 AS v2
FROM generate_series(1, 10000) i;

CREATE INDEX t_rle_int4_smol ON t_rle_int4 USING smol(k) INCLUDE (v1, v2);

-- Query to access RLE data (triggers smol_leaf_keyptr_ex lines 3560-3575)
SELECT count(*), sum(v1::int8) FROM t_rle_int4 WHERE k >= 50;
SELECT k, v1, v2 FROM t_rle_int4 WHERE k = 10 LIMIT 5;

-- ============================================================================
-- Test 2: int16 (2-byte) key Include-RLE (line 3172: else { int16 v = ... })
-- ============================================================================

DROP TABLE IF EXISTS t_rle_int2 CASCADE;
CREATE UNLOGGED TABLE t_rle_int2 (k int2, v int4);

INSERT INTO t_rle_int2
SELECT
    ((i / 50) % 30000)::int2 AS k,
    ((i / 50))::int4 AS v
FROM generate_series(1, 5000) i;

CREATE INDEX t_rle_int2_smol ON t_rle_int2 USING smol(k) INCLUDE (v);
SELECT count(*) FROM t_rle_int2 WHERE k >= 100;

-- ============================================================================
-- Test 3: Page boundary - RLE run doesn't fit (lines 3111, 3322)
-- Create data that fills page almost exactly to trigger "run doesn't fit" break
-- ============================================================================

DROP TABLE IF EXISTS t_rle_boundary CASCADE;
CREATE UNLOGGED TABLE t_rle_boundary (k int8, v1 int4, v2 int4, v3 int4, v4 int4);

-- Insert data sized to fill ~8KB pages
-- Each row: 8 (key) + 4*4 (includes) = 24 bytes
-- With RLE overhead: tag(2) + nitems(2) + nruns(2) + per-run(8+2+16) = need ~300 rows
INSERT INTO t_rle_boundary
SELECT
    ((i / 300) % 10000)::int8 AS k,
    ((i / 300))::int4 AS v1,
    ((i / 300) * 2)::int4 AS v2,
    ((i / 300) * 3)::int4 AS v3,
    ((i / 300) * 4)::int4 AS v4
FROM generate_series(1, 30000) i;

CREATE INDEX t_rle_boundary_smol ON t_rle_boundary USING smol(k) INCLUDE (v1, v2, v3, v4);
SELECT count(*) FROM t_rle_boundary WHERE k >= 5;

-- ============================================================================
-- Test 4: Text key Include-RLE (lines 3345, 3381-3382)
-- ============================================================================

DROP TABLE IF EXISTS t_rle_text_inc CASCADE;
CREATE UNLOGGED TABLE t_rle_text_inc (k text COLLATE "C", v1 int4, v2 int4);

-- Insert repetitive text keys
INSERT INTO t_rle_text_inc
SELECT
    'textkey' || lpad(((i / 100) % 500)::text, 4, '0') AS k,
    ((i / 100))::int4 AS v1,
    ((i / 100) * 2)::int4 AS v2
FROM generate_series(1, 10000) i;

CREATE INDEX t_rle_text_inc_smol ON t_rle_text_inc USING smol(k) INCLUDE (v1, v2);

-- Query to access text RLE (line 3381: memcpy(p, k0, key_len))
SELECT count(*), sum(v1::int8) FROM t_rle_text_inc WHERE k >= 'textkey0050';
SELECT k, v1 FROM t_rle_text_inc WHERE k = 'textkey0010' LIMIT 3;

-- ============================================================================
-- Test 5: Text RLE fall back to plain (line 3345)
-- Create text data where RLE doesn't help (all unique keys)
-- ============================================================================

DROP TABLE IF EXISTS t_text_plain CASCADE;
CREATE UNLOGGED TABLE t_text_plain (k text COLLATE "C", v int4);

-- All unique keys - RLE won't help
INSERT INTO t_text_plain
SELECT
    'unique' || lpad(i::text, 8, '0') AS k,
    i AS v
FROM generate_series(1, 5000) i;

CREATE INDEX t_text_plain_smol ON t_text_plain USING smol(k) INCLUDE (v);
SELECT count(*) FROM t_text_plain WHERE k >= 'unique00001000';

-- ============================================================================
-- Test 6: Multi-page text+include build (line 3422 - link previous page)
-- ============================================================================

DROP TABLE IF EXISTS t_text_multipage CASCADE;
CREATE UNLOGGED TABLE t_text_multipage (k text COLLATE "C", v1 int4, v2 int4);

-- Insert enough text data to span multiple pages (50K rows)
INSERT INTO t_text_multipage
SELECT
    'data' || lpad(i::text, 10, '0') AS k,
    i AS v1,
    i * 2 AS v2
FROM generate_series(1, 50000) i;

CREATE INDEX t_text_multipage_smol ON t_text_multipage USING smol(k) INCLUDE (v1, v2);
SELECT count(*) FROM t_text_multipage WHERE k >= 'data0000025000';

-- ============================================================================
-- Test 7: RLE run boundaries and key access (lines 3645-3651, smol_run_index_range)
-- ============================================================================

DROP TABLE IF EXISTS t_rle_runs CASCADE;
CREATE UNLOGGED TABLE t_rle_runs (k int8, v1 int4, v2 int4);

-- Create distinct runs with varying lengths
INSERT INTO t_rle_runs
SELECT
    CASE
        WHEN i <= 1000 THEN 100  -- Run 1: 1000 rows
        WHEN i <= 1500 THEN 200  -- Run 2: 500 rows
        WHEN i <= 3500 THEN 300  -- Run 3: 2000 rows
        ELSE 400                 -- Run 4: remaining
    END::int8 AS k,
    i AS v1,
    i * 2 AS v2
FROM generate_series(1, 5000) i;

CREATE INDEX t_rle_runs_smol ON t_rle_runs USING smol(k) INCLUDE (v1, v2);

-- Query different runs to trigger run boundary detection
SELECT k, count(*) FROM t_rle_runs WHERE k = 100 GROUP BY k;  -- First run
SELECT k, count(*) FROM t_rle_runs WHERE k = 200 GROUP BY k;  -- Middle run
SELECT k, count(*) FROM t_rle_runs WHERE k = 400 GROUP BY k;  -- Last run
SELECT k, v1, v2 FROM t_rle_runs WHERE k = 300 LIMIT 100;     -- Access within run

-- ============================================================================
-- Test 8: Key-only RLE without includes (0x8001 format) (lines 3717-3721)
-- This requires RLE compression on keys but NO include columns
-- ============================================================================

DROP TABLE IF EXISTS t_key_rle_only CASCADE;
CREATE UNLOGGED TABLE t_key_rle_only (k int8);

-- Insert highly repetitive keys (no includes)
INSERT INTO t_key_rle_only
SELECT ((i / 1000) % 100)::int8 FROM generate_series(1, 100000) i;

-- Create index WITHOUT include columns to trigger key-only RLE (0x8001)
CREATE INDEX t_key_rle_only_smol ON t_key_rle_only USING smol(k);

-- Query to access key-only RLE
SELECT k, count(*) FROM t_key_rle_only WHERE k >= 10 GROUP BY k ORDER BY k LIMIT 10;

-- ============================================================================
-- Test 9: RLE key pointer navigation (lines 3560-3575)
-- Access individual keys within RLE runs via scanning
-- ============================================================================

DROP TABLE IF EXISTS t_rle_keyptr CASCADE;
CREATE UNLOGGED TABLE t_rle_keyptr (k int8, v int4);

-- Create RLE data with specific run patterns
INSERT INTO t_rle_keyptr VALUES
    -- Run 1: key=1000, 500 times
    (SELECT 1000, generate_series FROM generate_series(1, 500)),
    -- Run 2: key=2000, 300 times
    (SELECT 2000, generate_series FROM generate_series(501, 800)),
    -- Run 3: key=3000, 700 times
    (SELECT 3000, generate_series FROM generate_series(801, 1500));

CREATE INDEX t_rle_keyptr_smol ON t_rle_keyptr USING smol(k) INCLUDE (v);

-- Scan to trigger key pointer access in different runs
SELECT k, v FROM t_rle_keyptr WHERE k = 1000 AND v >= 250 AND v < 260;  -- Middle of run 1
SELECT k, v FROM t_rle_keyptr WHERE k = 2000 AND v >= 700 AND v < 710;  -- Middle of run 2
SELECT k, v FROM t_rle_keyptr WHERE k = 3000 AND v >= 1400;              -- End of run 3

-- Cleanup
DROP TABLE t_rle_int4 CASCADE;
DROP TABLE t_rle_int2 CASCADE;
DROP TABLE t_rle_boundary CASCADE;
DROP TABLE t_rle_text_inc CASCADE;
DROP TABLE t_text_plain CASCADE;
DROP TABLE t_text_multipage CASCADE;
DROP TABLE t_rle_runs CASCADE;
DROP TABLE t_key_rle_only CASCADE;
DROP TABLE t_rle_keyptr CASCADE;
