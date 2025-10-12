-- Test text columns with INCLUDE and GUC to force tall trees
-- Covers lines 4489 (text INCLUDE build) and 5159 (text stream build)
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Force small pages to trigger GUC paths
SET smol.test_max_tuples_per_page = 50;

-- Test 1: Text key with INCLUDE columns
DROP TABLE IF EXISTS t_text_inc_guc CASCADE;
CREATE UNLOGGED TABLE t_text_inc_guc (
    k text COLLATE "C",
    i1 int4,
    i2 int4
);

-- Insert enough data to trigger GUC limiting
INSERT INTO t_text_inc_guc
SELECT 'key_' || i, i, i * 2
FROM generate_series(1, 1000) i;

-- Create index with text key and INCLUDE columns
-- This should hit line 4489 in smol_build_text_inc_from_sorted
CREATE INDEX t_text_inc_guc_idx ON t_text_inc_guc USING smol(k) INCLUDE (i1, i2);

-- Verify it created multiple pages
SELECT total_pages >= 10 AS has_multiple_pages
FROM smol_inspect('t_text_inc_guc_idx'::regclass);

-- Test 2: Text-only index (no INCLUDE)
DROP TABLE IF EXISTS t_text_only_guc CASCADE;
CREATE UNLOGGED TABLE t_text_only_guc (k text COLLATE "C");

INSERT INTO t_text_only_guc
SELECT 'value_' || lpad(i::text, 6, '0')
FROM generate_series(1, 1000) i;

-- This should hit line 5159 in smol_build_text_stream_from_tuplesort
CREATE INDEX t_text_only_guc_idx ON t_text_only_guc USING smol(k);

-- Verify multiple pages created
SELECT total_pages >= 10 AS has_multiple_pages
FROM smol_inspect('t_text_only_guc_idx'::regclass);

-- Test a query
SELECT COUNT(*) FROM t_text_only_guc WHERE k >= 'value_000500' AND k <= 'value_000510';

-- Reset GUC
RESET smol.test_max_tuples_per_page;

-- Cleanup
DROP TABLE t_text_inc_guc CASCADE;
DROP TABLE t_text_only_guc CASCADE;
