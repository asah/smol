-- Test RLE with 65534 items per page (maximum allowed)
-- This tests the uint16 boundary handling in scan code

CREATE EXTENSION IF NOT EXISTS smol;

-- Test 1: Create RLE page with exactly 65534 items
DROP TABLE IF EXISTS t_rle_65k CASCADE;
CREATE UNLOGGED TABLE t_rle_65k(k int8);

-- Insert data that will create runs totaling 65534 items
-- 65534 items = 655 runs of 100 items + 1 run of 34 items
INSERT INTO t_rle_65k
SELECT (i % 656)::int8
FROM generate_series(1, 65534) i;

CREATE INDEX t_rle_65k_smol ON t_rle_65k USING smol(k);

-- Verify the index was created and scan works correctly
SELECT COUNT(*) FROM t_rle_65k;
SELECT COUNT(DISTINCT k) FROM t_rle_65k;

-- Test scanning across the boundary
SELECT COUNT(*) FROM t_rle_65k WHERE k >= 0;
SELECT COUNT(*) FROM t_rle_65k WHERE k > 100;
SELECT COUNT(*) FROM t_rle_65k WHERE k BETWEEN 50 AND 150;

-- Verify scanning works at the boundary
SELECT k FROM t_rle_65k ORDER BY k LIMIT 5;
SELECT k FROM t_rle_65k ORDER BY k DESC LIMIT 5;

DROP TABLE t_rle_65k CASCADE;

-- Test 2: Create data that would exceed 65534 if not limited
DROP TABLE IF EXISTS t_rle_overflow CASCADE;
CREATE UNLOGGED TABLE t_rle_overflow(k int4);

-- Insert 100K items that compress into very few runs
-- This should create multiple pages, each with max 65534 items
INSERT INTO t_rle_overflow
SELECT (i % 100)
FROM generate_series(1, 100000) i;

CREATE INDEX t_rle_overflow_smol ON t_rle_overflow USING smol(k);

-- Verify the data
SELECT COUNT(*) FROM t_rle_overflow;
SELECT COUNT(*) FROM t_rle_overflow WHERE k >= 50;

-- Verify boundary handling by scanning
SELECT DISTINCT k FROM t_rle_overflow WHERE k < 10 ORDER BY k;

DROP TABLE t_rle_overflow CASCADE;

-- Test 3: Unbounded parallel scan (tests the PG_INT64_MIN fallback)
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_index_scan_size = 0;

DROP TABLE IF EXISTS t_unbounded_parallel CASCADE;
CREATE UNLOGGED TABLE t_unbounded_parallel(k int4, v int4);

INSERT INTO t_unbounded_parallel
SELECT i, i*2
FROM generate_series(1, 50000) i;

CREATE INDEX t_unbounded_parallel_smol ON t_unbounded_parallel USING smol(k, v);
ANALYZE t_unbounded_parallel;

-- Query without bounds - should use unbounded parallel scan
SELECT COUNT(*) FROM t_unbounded_parallel WHERE k IS NOT NULL;

-- Query with IS NOT NULL on both columns
SELECT COUNT(*) FROM t_unbounded_parallel WHERE k IS NOT NULL AND v IS NOT NULL;

DROP TABLE t_unbounded_parallel CASCADE;

RESET max_parallel_workers_per_gather;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_index_scan_size;
