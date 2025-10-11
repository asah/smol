-- Test backward navigation in tall trees (height >= 3)
-- Covers smol_rightmost_in_subtree function (lines 5404-5420)

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Use GUC to force very tall tree with height >= 3
-- Also force low internal fanout to ensure multiple internal levels
SET smol.test_max_tuples_per_page = 50;
SET smol.test_max_internal_fanout = 10;

DROP TABLE IF EXISTS t_very_tall_backward CASCADE;
CREATE UNLOGGED TABLE t_very_tall_backward (
    k int4,
    i1 int4, i2 int4, i3 int4, i4 int4,
    i5 int4, i6 int4, i7 int4, i8 int4
);

-- Insert enough data to create height >= 4 with our GUCs
-- With max 50 tuples/page and 10 children/internal:
-- Height 2: 50 * 10 = 500 rows
-- Height 3: 50 * 10 * 10 = 5,000 rows
-- Height 4: 50 * 10 * 10 * 10 = 50,000 rows
-- Use 100,000 rows to ensure height >= 4 and trigger smol_rightmost_in_subtree loop
INSERT INTO t_very_tall_backward
SELECT i, i, i, i, i, i, i, i, i
FROM generate_series(1, 100000) i;

CREATE INDEX t_very_tall_backward_idx ON t_very_tall_backward
USING smol(k) INCLUDE (i1, i2, i3, i4, i5, i6, i7, i8);

-- Verify we have a tall tree
SELECT
    total_pages,
    leaf_pages,
    total_pages > 100 AS has_many_pages,
    leaf_pages > 100 AS has_many_leaves
FROM smol_inspect('t_very_tall_backward_idx'::regclass);

-- Force index-only scan
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET max_parallel_workers_per_gather = 0;

-- Backward scan from a specific point that will cross subtree boundaries
-- This should trigger smol_rightmost_in_subtree when moving backward
-- across internal node boundaries
SELECT smol_test_backward_scan('t_very_tall_backward_idx'::regclass, 10000);

-- Also test with a full backward scan
SELECT smol_test_backward_scan('t_very_tall_backward_idx'::regclass);

-- Query that triggers backward navigation
SELECT k FROM t_very_tall_backward WHERE k >= 15000 ORDER BY k DESC LIMIT 10;

-- Reset GUCs
RESET smol.test_max_tuples_per_page;
RESET smol.test_max_internal_fanout;

-- Cleanup
DROP TABLE t_very_tall_backward CASCADE;
