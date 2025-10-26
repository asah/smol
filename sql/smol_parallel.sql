-- smol_parallel.sql: Parallel scan and build tests
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- Parallel Sequential Scan
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_seq CASCADE;
CREATE TABLE t_parallel_seq (k int4, v text);
INSERT INTO t_parallel_seq SELECT i, 'value_' || i FROM generate_series(1, 10000) i;
CREATE INDEX t_parallel_seq_idx ON t_parallel_seq USING smol(k);

SET max_parallel_workers_per_gather = 2;
SET min_parallel_table_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

-- Force parallel scan
SELECT count(*) FROM t_parallel_seq WHERE k > 5000;
SELECT count(*) FROM t_parallel_seq WHERE k BETWEEN 2000 AND 8000;

-- ============================================================================
-- Parallel Index Scan
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_idx CASCADE;
CREATE TABLE t_parallel_idx (k int4, v int4);
INSERT INTO t_parallel_idx SELECT i, i * 2 FROM generate_series(1, 20000) i;
CREATE INDEX t_parallel_idx_idx ON t_parallel_idx USING smol(k);

SET enable_seqscan = off;

-- Parallel index scan
SELECT count(*) FROM t_parallel_idx WHERE k > 10000;
SELECT sum(v) FROM t_parallel_idx WHERE k BETWEEN 5000 AND 15000;

-- ============================================================================
-- Parallel Index Build
-- ============================================================================
DROP TABLE IF EXISTS t_parallel_build CASCADE;
CREATE TABLE t_parallel_build (k int4);
INSERT INTO t_parallel_build SELECT i FROM generate_series(1, 30000) i;

SET max_parallel_maintenance_workers = 4;

CREATE INDEX t_parallel_build_idx ON t_parallel_build USING smol(k);

SELECT count(*) FROM t_parallel_build WHERE k < 10000;

-- Reset parallel settings
RESET max_parallel_workers_per_gather;
RESET min_parallel_table_scan_size;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET max_parallel_maintenance_workers;
RESET enable_seqscan;

-- Cleanup
DROP TABLE t_parallel_seq CASCADE;
DROP TABLE t_parallel_idx CASCADE;
DROP TABLE t_parallel_build CASCADE;
