-- Test to cover zerocopy format detection in scans (line 2756)
-- Zerocopy format is enabled via smol.zerocopy_uniqueness_threshold GUC

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Disable parallel build to ensure GUC is honored
SET max_parallel_maintenance_workers = 0;

-- Enable zerocopy format for unique keys (uniqueness >= 0.9)
-- This will use zerocopy for highly unique data with 8+ byte keys
SET smol.zerocopy_uniqueness_threshold = 0.9;

DROP TABLE IF EXISTS t_zerocopy_fmt CASCADE;
CREATE UNLOGGED TABLE t_zerocopy_fmt (k int8);  -- 8-byte key for zerocopy

-- Insert unique keys to trigger zerocopy format
INSERT INTO t_zerocopy_fmt SELECT i FROM generate_series(1, 1000) i;

CREATE INDEX idx_zerocopy_fmt ON t_zerocopy_fmt USING smol(k);

-- Check if we got zerocopy pages
SELECT zerocopy_pages > 0 AS has_zerocopy FROM smol_inspect('idx_zerocopy_fmt');

-- Force index-only scan to trigger page format detection at line 2756
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;

-- Scan the index to hit the zerocopy format detection code
SELECT count(*) FROM t_zerocopy_fmt WHERE k >= 1;
SELECT count(*) FROM t_zerocopy_fmt WHERE k >= 10;

-- Test with upper bound to hit line 1072 (zerocopy bounds checking)
SELECT count(*) FROM t_zerocopy_fmt WHERE k >= 1 AND k < 500;

-- Test with BETWEEN to hit runtime key failure path (lines 3422-3423)
SELECT count(*) FROM t_zerocopy_fmt WHERE k BETWEEN 100 AND 200;

-- Reset GUC
RESET smol.test_max_tuples_per_page;

-- Cleanup
DROP TABLE t_zerocopy_fmt CASCADE;
