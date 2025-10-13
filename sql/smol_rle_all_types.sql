SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test RLE compression for all supported datatypes
-- This test verifies that RLE compression works correctly across
-- different data types and that it provides actual space savings


-- ============================================================================
-- Test 1: int2 RLE
-- ============================================================================
\echo 'Test 1: int2 with heavy duplicates (RLE compression)'

DROP TABLE IF EXISTS test_int2_rle CASCADE;
CREATE TABLE test_int2_rle(k int2, v int4);

-- Insert 10K rows with only 10 distinct values (1000 copies each)
INSERT INTO test_int2_rle
SELECT (i % 10)::int2, i
FROM generate_series(1, 10000) i
ORDER BY 1, 2;

CREATE INDEX test_int2_rle_idx ON test_int2_rle USING smol(k);

-- Verify RLE is used: smol_inspect returns OUT parameters including key_rle_pages
-- We expect key_rle_pages > 0
SELECT
    total_pages,
    leaf_pages,
    key_rle_pages,
    compression_pct,
    CASE WHEN key_rle_pages > 0 THEN 'RLE USED' ELSE 'RLE NOT USED' END AS format_check
FROM smol_inspect('test_int2_rle_idx');

-- Test queries
SELECT count(*) FROM test_int2_rle WHERE k >= 5;
SELECT count(*) FROM test_int2_rle WHERE k = 0;
SELECT min(k), max(k), count(*) FROM test_int2_rle;

-- ============================================================================
-- Test 2: int8 RLE
-- ============================================================================
\echo 'Test 2: int8 with heavy duplicates (RLE compression)'

DROP TABLE IF EXISTS test_int8_rle CASCADE;
CREATE TABLE test_int8_rle(k int8, v int4);

-- Insert 10K rows with 20 distinct values
INSERT INTO test_int8_rle
SELECT (i % 20)::int8, i
FROM generate_series(1, 10000) i
ORDER BY 1, 2;

CREATE INDEX test_int8_rle_idx ON test_int8_rle USING smol(k);

-- Verify RLE is used
SELECT
    total_pages,
    leaf_pages,
    key_rle_pages,
    compression_pct,
    CASE WHEN key_rle_pages > 0 THEN 'RLE USED' ELSE 'RLE NOT USED' END AS format_check
FROM smol_inspect('test_int8_rle_idx');

-- Test queries
SELECT count(*) FROM test_int8_rle WHERE k >= 10;
SELECT count(*) FROM test_int8_rle WHERE k = 5;

-- ============================================================================
-- Test 3: UUID RLE
-- ============================================================================
\echo 'Test 3: UUID with duplicates (RLE compression)'

DROP TABLE IF EXISTS test_uuid_rle CASCADE;
CREATE TABLE test_uuid_rle(k uuid, v int4);

-- Insert 10K rows with 10 distinct UUIDs
INSERT INTO test_uuid_rle
SELECT
  ('00000000-0000-0000-0000-00000000000' || (i % 10)::text)::uuid,
  i
FROM generate_series(1, 10000) i
ORDER BY 1, 2;

CREATE INDEX test_uuid_rle_idx ON test_uuid_rle USING smol(k);

-- Verify RLE is used
SELECT
    total_pages,
    leaf_pages,
    key_rle_pages,
    compression_pct,
    CASE WHEN key_rle_pages > 0 THEN 'RLE USED' ELSE 'RLE NOT USED' END AS format_check
FROM smol_inspect('test_uuid_rle_idx');

-- Test queries
SELECT count(*) FROM test_uuid_rle WHERE k >= '00000000-0000-0000-0000-000000000005'::uuid;
SELECT count(DISTINCT k) FROM test_uuid_rle;

-- ============================================================================
-- Test 4: Text RLE (8-byte cap)
-- ============================================================================
\echo 'Test 4: Text (8-byte) with duplicates (RLE compression)'

DROP TABLE IF EXISTS test_text8_rle CASCADE;
CREATE TABLE test_text8_rle(k text COLLATE "C", v int4);

-- Insert 10K rows with 5 distinct short strings
INSERT INTO test_text8_rle
SELECT CASE (i % 5)
  WHEN 0 THEN 'admin'
  WHEN 1 THEN 'client'
  WHEN 2 THEN 'guest'
  WHEN 3 THEN 'user'
  ELSE 'manager'
END, i
FROM generate_series(1, 10000) i
ORDER BY 1, 2;

CREATE INDEX test_text8_rle_idx ON test_text8_rle USING smol(k);

-- Verify RLE is used
SELECT
    total_pages,
    leaf_pages,
    key_rle_pages,
    compression_pct,
    CASE WHEN key_rle_pages > 0 THEN 'RLE USED' ELSE 'RLE NOT USED' END AS format_check
FROM smol_inspect('test_text8_rle_idx');

-- Test queries
SELECT count(*) FROM test_text8_rle WHERE k >= 'guest';
SELECT count(*) FROM test_text8_rle WHERE k = 'admin';

-- ============================================================================
-- Test 5: Text RLE (16-byte cap)
-- ============================================================================
\echo 'Test 5: Text (16-byte) with duplicates (RLE compression)'

DROP TABLE IF EXISTS test_text16_rle CASCADE;
CREATE TABLE test_text16_rle(k text COLLATE "C", v int4);

-- Insert 10K rows with longer strings (up to 12 chars)
INSERT INTO test_text16_rle
SELECT CASE (i % 3)
  WHEN 0 THEN 'administrator'
  WHEN 1 THEN 'poweruser'
  ELSE 'guestuser'
END, i
FROM generate_series(1, 10000) i
ORDER BY 1, 2;

CREATE INDEX test_text16_rle_idx ON test_text16_rle USING smol(k);

-- Verify RLE is used
SELECT
    total_pages,
    leaf_pages,
    key_rle_pages,
    compression_pct,
    CASE WHEN key_rle_pages > 0 THEN 'RLE USED' ELSE 'RLE NOT USED' END AS format_check
FROM smol_inspect('test_text16_rle_idx');

-- Test queries
SELECT count(*) FROM test_text16_rle WHERE k >= 'guestuser';
SELECT count(*) FROM test_text16_rle WHERE k = 'administrator';

-- ============================================================================
-- Test 6: Verify RLE correctness with backward scans
-- ============================================================================
\echo 'Test 6: RLE with backward scans'

SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Forward scan
SELECT k, count(*) FROM test_int2_rle GROUP BY k ORDER BY k LIMIT 5;

-- Backward scan (should also work with RLE)
SELECT k, count(*) FROM test_int2_rle GROUP BY k ORDER BY k DESC LIMIT 5;

-- ============================================================================
-- Test 7: RLE space savings verification
-- ============================================================================
\echo 'Test 7: Verify RLE provides space savings'

-- Create identical table without RLE (unique keys)
DROP TABLE IF EXISTS test_int4_norle CASCADE;
CREATE TABLE test_int4_norle(k int4, v int4);
INSERT INTO test_int4_norle SELECT i, i FROM generate_series(1, 10000) i;
CREATE INDEX test_int4_norle_idx ON test_int4_norle USING smol(k);

-- Create table with RLE (10 distinct values)
DROP TABLE IF EXISTS test_int4_rle CASCADE;
CREATE TABLE test_int4_rle(k int4, v int4);
INSERT INTO test_int4_rle SELECT (i % 10)::int4, i FROM generate_series(1, 10000) i ORDER BY 1, 2;
CREATE INDEX test_int4_rle_idx ON test_int4_rle USING smol(k);

-- Compare sizes: RLE should be significantly smaller
SELECT
    'without_rle' AS case_name,
    pg_size_pretty(pg_relation_size('test_int4_norle_idx')) AS index_size,
    leaf_pages AS pages,
    key_rle_pages AS rle_pages
FROM smol_inspect('test_int4_norle_idx')
UNION ALL
SELECT
    'with_rle' AS case_name,
    pg_size_pretty(pg_relation_size('test_int4_rle_idx')) AS index_size,
    leaf_pages AS pages,
    key_rle_pages AS rle_pages
FROM smol_inspect('test_int4_rle_idx')
ORDER BY case_name;

-- Verify RLE version has fewer pages
SELECT
    CASE WHEN (SELECT leaf_pages FROM smol_inspect('test_int4_rle_idx')) <
              (SELECT leaf_pages FROM smol_inspect('test_int4_norle_idx'))
    THEN 'RLE SAVES SPACE'
    ELSE 'RLE NO SAVINGS'
    END AS space_check;

\echo 'RLE tests completed successfully!'
