-- Test to specifically trigger zerocopy format detection in scans (line 2754)
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Force enable zero-copy
SET smol.enable_zero_copy = on;

DROP TABLE IF EXISTS t_zerocopy_scan CASCADE;
CREATE UNLOGGED TABLE t_zerocopy_scan (k int4, v int4);

-- Insert enough data to create multiple pages
INSERT INTO t_zerocopy_scan SELECT i, i*2 FROM generate_series(1, 10000) i;

CREATE INDEX idx_zerocopy_scan ON t_zerocopy_scan USING smol(k);

-- Verify index uses zerocopy format
SELECT zerocopy_pages > 0 AS has_zerocopy FROM smol_inspect('idx_zerocopy_scan'::regclass);

-- Force index-only scan
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET enable_indexscan = off;

-- This should trigger the zerocopy format detection at line 2754
SELECT count(*) FROM t_zerocopy_scan WHERE k >= 5000;
SELECT count(*) FROM t_zerocopy_scan WHERE k >= 1 AND k <= 100;
SELECT count(*) FROM t_zerocopy_scan WHERE k = 42;

-- Cleanup
DROP TABLE t_zerocopy_scan CASCADE;
