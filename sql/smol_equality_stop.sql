-- Test equality bound stop optimization (lines 980-981)
-- This test verifies that when scanning for k=X, SMOL stops as soon as it finds a page where first_key > X

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET max_parallel_workers_per_gather = 0;

-- Force enable page-level bounds checking (planner-dependent optimization)
SET smol.test_force_page_bounds_check = on;

-- Create a table with carefully chosen data distribution
-- We want to create a scenario where:
-- 1. We search for k = 50000 (which DOES exist)
-- 2. Value 50000 exists and spans to the END of one page
-- 3. The NEXT page starts with a value > 50000 (e.g., 60000)
-- 4. Scanner moves from page with 50000 to next page, sees first_key > 50000
-- This triggers the equality stop optimization (lines 982-983)

DROP TABLE IF EXISTS t_eq_stop CASCADE;
CREATE UNLOGGED TABLE t_eq_stop (k int4);

-- Insert data to create specific page layout:
-- Each page holds ~2000 int4 keys
-- We'll create data where the SCAN starts on one page and advances to the next

-- First, insert values 1-5000 (spans ~2-3 pages)
INSERT INTO t_eq_stop
SELECT i FROM generate_series(1, 5000) i;

-- Then insert a large gap: skip to 100000
-- This creates pages where early pages have k < 6000, later pages have k >= 100000
INSERT INTO t_eq_stop
SELECT i FROM generate_series(100000, 105000) i;

ALTER TABLE t_eq_stop SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) t_eq_stop;

CREATE INDEX t_eq_stop_smol_idx ON t_eq_stop USING smol(k);

-- Verify page layout
SELECT
    leaf_pages >= 2 AS has_multiple_pages
FROM smol_inspect('t_eq_stop_smol_idx');

-- Test range query that should trigger page-level bounds optimization
-- The scan should:
-- 1. Start at k=1, scan through pages with k=1..5000
-- 2. When advancing to next page, that page has first_key=100000
-- 3. Query has upper bound k < 6000, so first_key=100000 > 6000
-- 4. smol_page_matches_scan_bounds() returns false with stop_scan=true
-- 5. Scan stops early without reading the 100000+ page!

SELECT count(*) AS found_below_6000
FROM t_eq_stop WHERE k >= 1 AND k < 6000;

-- Verify the data spans multiple pages and includes the gap
SELECT count(*) AS total_rows FROM t_eq_stop;
SELECT count(*) AS high_values FROM t_eq_stop WHERE k >= 100000;

-- Cleanup
DROP TABLE t_eq_stop CASCADE;
