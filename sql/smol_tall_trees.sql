-- Test tree navigation for tall trees (height > 2)
-- Targets lines 4042-4050 (smol_rightmost_leaf), 4067-4086 (smol_prev_leaf)
-- Also targets 3472-3473 (rightmost child), 3787-3789 (internal node realloc)

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- Test 1: Very tall tree requiring smol_rightmost_leaf navigation
-- Need enough data to build height>2 tree (typically 5-10M+ rows)
-- Lines 4042-4050: Navigate down tree to find rightmost leaf
-- Lines 3787-3789: Internal node array reallocation during multi-level build
-- ============================================================================

DROP TABLE IF EXISTS t_very_tall CASCADE;
CREATE UNLOGGED TABLE t_very_tall (k int8);

-- Insert 5 million rows to force tall tree (height 3)
-- Each leaf holds ~500-1000 int64 keys, so:
-- - 1K rows = 1-2 leaves (height 1)
-- - 500K rows = ~500-1000 leaves (height 2)
-- - 5M rows = ~5000-10000 leaves (height 3)
INSERT INTO t_very_tall
SELECT i::int8 FROM generate_series(1, 5000000) i;

CREATE INDEX t_very_tall_smol ON t_very_tall USING smol(k);

-- Backward scan triggers smol_prev_leaf navigation (lines 4067-4086)
-- This should navigate the tall tree structure
SELECT k FROM t_very_tall WHERE k <= 4999990 ORDER BY k DESC LIMIT 10;

-- Query rightmost values (triggers rightmost leaf navigation lines 4042-4050)
SELECT k FROM t_very_tall WHERE k >= 4999990 ORDER BY k LIMIT 10;

-- Additional queries to ensure tree navigation is exercised
SELECT count(*) FROM t_very_tall WHERE k >= 2500000;
SELECT count(*) FROM t_very_tall WHERE k <= 2500000;

-- ============================================================================
-- Test 2: 1-byte byval key type (lines 3495)
-- ============================================================================

DROP TABLE IF EXISTS t_char_key CASCADE;
CREATE UNLOGGED TABLE t_char_key (k "char");

INSERT INTO t_char_key
SELECT (i % 127)::"char" FROM generate_series(1, 10000) i;

CREATE INDEX t_char_key_smol ON t_char_key USING smol(k);

SELECT k FROM t_char_key WHERE k >= '0'::"char" ORDER BY k LIMIT 10;
SELECT count(*) FROM t_char_key WHERE k >= 'A'::"char";

-- ============================================================================
-- Test 3: Very large Include-RLE to trigger NOTICE (line 3341)
-- Need >10K rows in single RLE run
-- ============================================================================

DROP TABLE IF EXISTS t_large_inc_rle CASCADE;
CREATE UNLOGGED TABLE t_large_inc_rle (k int8, v1 int4, v2 int4);

-- Create data with same key repeated 15K times to get large RLE run
INSERT INTO t_large_inc_rle
SELECT
    1::int8,              -- All same key
    (i / 100)::int4,      -- Repetitive includes (groups of 100)
    (i / 100)::int4
FROM generate_series(1, 15000) i;

-- This should trigger the NOTICE at line 3341 for >10K row RLE
SET client_min_messages = notice;
CREATE INDEX t_large_inc_rle_smol ON t_large_inc_rle USING smol(k) INCLUDE (v1, v2);
SET client_min_messages = warning;

SELECT count(*) FROM t_large_inc_rle WHERE k = 1;

-- Cleanup
DROP TABLE t_very_tall CASCADE;
DROP TABLE t_char_key CASCADE;
DROP TABLE t_large_inc_rle CASCADE;
