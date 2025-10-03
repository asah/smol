-- Force parallel index build to cover parallel worker (lines 4352-4401)
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Configure for parallel maintenance
SET max_parallel_maintenance_workers = 4;
SET maintenance_work_mem = '1GB';
SET min_parallel_table_scan_size = 0;

-- Create large table (need enough data to trigger parallel build)
DROP TABLE IF EXISTS t_parallel_build CASCADE;
CREATE TABLE t_parallel_build (k int4, v int4);

-- Insert 1 million rows to encourage parallel build
INSERT INTO t_parallel_build 
SELECT i, i*10 FROM generate_series(1, 1000000) i;

-- Build index (should use parallel workers if enabled)
CREATE INDEX t_parallel_build_idx ON t_parallel_build USING smol(k);

-- Verify it worked
SELECT count(*) FROM t_parallel_build WHERE k > 500000;

-- Cleanup
DROP TABLE t_parallel_build CASCADE;
