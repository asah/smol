-- Test tall tree navigation using limited internal fanout
-- Targets lines 4040-4084 (smol_rightmost_leaf, smol_prev_leaf)
-- Targets lines 3785-3787 (internal node capacity doubling during realloc)

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Strategy: Limit internal node fanout to 10 children
-- With ~500 leaf pages, this creates:
-- Level 0: 500 leaf pages
-- Level 1: 50 internal pages (500/10)
-- Level 2: 5 internal pages (50/10)
-- Level 3: 1 root (5/10)
-- Height = 4

SET smol.test_max_internal_fanout = 10;

DROP TABLE IF EXISTS t_tall CASCADE;
CREATE UNLOGGED TABLE t_tall (k int8);

-- Create enough data to force ~500+ leaf pages
-- Each leaf holds ~450 int8 values (8 bytes each)
-- 500 leaves * 450 = 225,000 rows should be enough
INSERT INTO t_tall SELECT i FROM generate_series(1, 225000) i;

-- Build index with limited fanout to force tall tree
CREATE INDEX t_tall_smol ON t_tall USING smol(k);

-- Test backward scan to trigger smol_rightmost_leaf (lines 4070-4082)
-- This should navigate down the tree to find the rightmost leaf (height > 2 required)
-- Force index-only scan with backward direction
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM (SELECT k FROM t_tall ORDER BY k DESC LIMIT 10) s;

-- Test backward scan with LIMIT to exercise smol_prev_leaf (lines 4097-4116)
-- This triggers backward leaf-to-leaf navigation across multiple pages
SELECT k FROM t_tall ORDER BY k DESC LIMIT 1000 OFFSET 10;

-- Test forward scan across many pages to ensure tall tree works correctly
SELECT count(*) FROM t_tall WHERE k > 50000 AND k < 200000;

-- Test index-only scan to verify tuple retrieval works with tall trees
SET enable_seqscan = off;
SELECT min(k), max(k), count(*) FROM t_tall;

-- Cleanup
DROP TABLE t_tall CASCADE;
RESET smol.test_max_internal_fanout;
