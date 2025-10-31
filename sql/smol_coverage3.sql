-- Test file for additional coverage: upper bounds, ranges, additional data types, and bloom filters
-- Consolidates: smol_bloom_coverage, smol_final_bloom, smol_int2_coverage, smol_int8_coverage,
--               smol_empty_page_coverage, smol_bloom_disabled_coverage, smol_invalid_nhash_coverage,
--               smol_bloom_rejection_coverage

-- Test 1: Upper bound queries with int4
CREATE UNLOGGED TABLE upper_int4(k int4);
INSERT INTO upper_int4 SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX upper_int4_idx ON upper_int4 USING smol(k);

-- Test < operator (strict upper bound)
SET enable_seqscan = off;
SELECT count(*) FROM upper_int4 WHERE k < 500;

-- Test <= operator (non-strict upper bound)
SELECT count(*) FROM upper_int4 WHERE k <= 500;

-- Test BETWEEN (both bounds)
SELECT count(*) FROM upper_int4 WHERE k BETWEEN 100 AND 200;

DROP TABLE upper_int4 CASCADE;

-- Test 2: Upper bound queries with int2
CREATE UNLOGGED TABLE upper_int2(k int2);
INSERT INTO upper_int2 SELECT i::int2 FROM generate_series(1, 1000) i;
CREATE INDEX upper_int2_idx ON upper_int2 USING smol(k);

SELECT count(*) FROM upper_int2 WHERE k < 500::int2;
SELECT count(*) FROM upper_int2 WHERE k <= 500::int2;
SELECT count(*) FROM upper_int2 WHERE k BETWEEN 100::int2 AND 200::int2;

DROP TABLE upper_int2 CASCADE;

-- Test 3: Upper bound queries with int8
CREATE UNLOGGED TABLE upper_int8(k int8);
INSERT INTO upper_int8 SELECT i::int8 FROM generate_series(1, 1000) i;
CREATE INDEX upper_int8_idx ON upper_int8 USING smol(k);

SELECT count(*) FROM upper_int8 WHERE k < 500::int8;
SELECT count(*) FROM upper_int8 WHERE k <= 500::int8;
SELECT count(*) FROM upper_int8 WHERE k BETWEEN 100::int8 AND 200::int8;

DROP TABLE upper_int8 CASCADE;

-- Test 4: UUID type (use deterministic UUIDs for testing)
CREATE UNLOGGED TABLE upper_uuid(k uuid);
-- Create deterministic UUIDs by padding integers to proper UUID format
INSERT INTO upper_uuid SELECT (lpad(i::text, 8, '0') || '-0000-0000-0000-000000000000')::uuid
  FROM generate_series(1, 500) i;
CREATE INDEX upper_uuid_idx ON upper_uuid USING smol(k);

-- Query should return consistent results with deterministic data
SELECT count(*) FROM upper_uuid WHERE k > '00000100-0000-0000-0000-000000000000'::uuid;

DROP TABLE upper_uuid CASCADE;

-- Test 5: Text upper bounds  (skip position scan for text, use regular scan)
CREATE UNLOGGED TABLE upper_text(k text COLLATE "C");
INSERT INTO upper_text SELECT 'value_' || lpad(i::text, 5, '0') FROM generate_series(1, 500) i;
CREATE INDEX upper_text_idx ON upper_text USING smol(k);

-- Text queries use regular scan (not position scan)
SELECT count(*) FROM upper_text WHERE k > 'value_00200';
SELECT count(*) FROM upper_text WHERE k < 'value_00300';

DROP TABLE upper_text CASCADE;

-- ========================================
-- BLOOM FILTER TESTS (consolidated)
-- ========================================

-- Enable bloom filters for all bloom tests
SET smol.bloom_filters = on;
SET smol.build_bloom_filters = on;
SET smol.bloom_nhash = 2;
SET enable_seqscan = off;

-- Test 6: INT2 bloom filter coverage
CREATE UNLOGGED TABLE test_int2(k int2);
INSERT INTO test_int2 SELECT (i % 100)::int2 FROM generate_series(1, 1000) i;
CREATE INDEX test_int2_idx ON test_int2 USING smol(k);
SELECT count(*) FROM test_int2 WHERE k = 50;
DROP TABLE test_int2 CASCADE;

-- Test 7: INT8 bloom filter coverage
CREATE UNLOGGED TABLE test_int8(k int8);
INSERT INTO test_int8 SELECT (i % 100)::int8 FROM generate_series(1, 1000) i;
CREATE INDEX test_int8_idx ON test_int8 USING smol(k);
SELECT count(*) FROM test_int8 WHERE k = 50;
DROP TABLE test_int8 CASCADE;

-- Test 8: Empty page coverage
CREATE UNLOGGED TABLE test_empty(k int4);
CREATE INDEX test_empty_idx ON test_empty USING smol(k);
SELECT count(*) FROM test_empty WHERE k = 1;
DROP TABLE test_empty CASCADE;

-- Test 9: Bloom disabled coverage (build_bloom_filters=off)
SET smol.build_bloom_filters = off;
CREATE UNLOGGED TABLE test_bloom_disabled(k int4);
INSERT INTO test_bloom_disabled SELECT (i % 100) FROM generate_series(1, 1000) i;
CREATE INDEX test_bloom_disabled_idx ON test_bloom_disabled USING smol(k);
SELECT count(*) FROM test_bloom_disabled WHERE k = 50;
DROP TABLE test_bloom_disabled CASCADE;
SET smol.build_bloom_filters = on;

-- Test 10: Invalid nhash coverage (using test GUC)
CREATE UNLOGGED TABLE test_invalid_nhash(k int4);
INSERT INTO test_invalid_nhash SELECT (i % 10) FROM generate_series(1, 1000) i;
CREATE INDEX test_invalid_nhash_idx ON test_invalid_nhash USING smol(k);
SET smol.test_force_invalid_nhash = on;
SELECT count(*) FROM test_invalid_nhash WHERE k = 5;
SET smol.test_force_invalid_nhash = off;
DROP TABLE test_invalid_nhash CASCADE;

-- Test 11: Bloom rejection coverage (using test GUC)
CREATE UNLOGGED TABLE test_bloom_reject(k int4);
INSERT INTO test_bloom_reject SELECT (i % 10) FROM generate_series(1, 1000) i;
CREATE INDEX test_bloom_reject_idx ON test_bloom_reject USING smol(k);
SET smol.test_force_bloom_rejection = on;
SELECT count(*) FROM test_bloom_reject WHERE k = 5;
SET smol.test_force_bloom_rejection = off;
DROP TABLE test_bloom_reject CASCADE;

-- Test 12: Bloom page skipping with disjoint pages
CREATE UNLOGGED TABLE bloom_reject(k int4);
INSERT INTO bloom_reject SELECT (i % 100) + 1 FROM generate_series(1, 3000) i;
INSERT INTO bloom_reject SELECT (i % 100) + 101 FROM generate_series(1, 3000) i;
INSERT INTO bloom_reject SELECT (i % 100) + 201 FROM generate_series(1, 3000) i;
CREATE INDEX bloom_reject_idx ON bloom_reject USING smol(k);
SELECT count(*) FROM bloom_reject WHERE k = 50;
SELECT count(*) FROM bloom_reject WHERE k = 150;
DROP TABLE bloom_reject CASCADE;

-- Test 13: Comprehensive bloom filter test with multiple scenarios
CREATE UNLOGGED TABLE bloom_comprehensive(k int4);
INSERT INTO bloom_comprehensive
  SELECT CASE WHEN i <= 5000 THEN 500 ELSE ((i - 5000) % 1000) + 1 END::int4
  FROM generate_series(1, 10000) i;
CREATE INDEX bloom_comprehensive_idx ON bloom_comprehensive USING smol(k);
SELECT count(*) FROM bloom_comprehensive WHERE k = 500;
SELECT count(*) FROM bloom_comprehensive WHERE k = 1;
DROP TABLE bloom_comprehensive CASCADE;

\echo 'Test PASSED: Advanced coverage tests with bloom filters'
-- Test NUMERIC support in INCLUDE columns
-- This test exercises NUMERIC conversion for INCLUDE columns only

-- Test 1: Basic NUMERIC INCLUDE column (two-column index)
CREATE TABLE t_numeric_basic (id int, cat int, val numeric(10,2));
INSERT INTO t_numeric_basic VALUES (1, 100, 123.45), (2, 100, 234.56), (3, 200, -345.67);
CREATE INDEX t_numeric_basic_idx ON t_numeric_basic USING smol(id, cat) INCLUDE (val);

SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Verify values
SELECT * FROM t_numeric_basic ORDER BY id;

-- Test 2: Multiple NUMERIC INCLUDE columns with different scales
CREATE TABLE t_numeric_multi (id int, cat int, v0 numeric(10,0), v2 numeric(10,2), v4 numeric(18,4));
INSERT INTO t_numeric_multi VALUES (1, 100, 12345, 123.45, 1234.5678);
CREATE INDEX t_numeric_multi_idx ON t_numeric_multi USING smol(id, cat) INCLUDE (v0, v2, v4);

SELECT * FROM t_numeric_multi;

-- Test 3: Different precision requiring different storage sizes
-- int16 (precision <= 4): numeric(4,2)
-- int32 (precision <= 9): numeric(9,4)
-- int64 (precision <= 18): numeric(18,4)
CREATE TABLE t_numeric_sizes (id int, cat int, small numeric(4,2), medium numeric(9,4), large numeric(18,4));
INSERT INTO t_numeric_sizes VALUES (1, 100, 12.34, 12345.6789, 123456789012.3456);
CREATE INDEX t_numeric_sizes_idx ON t_numeric_sizes USING smol(id, cat) INCLUDE (small, medium, large);

SELECT * FROM t_numeric_sizes;

-- Test 4: Edge cases (zero, negative, maximum values)
CREATE TABLE t_numeric_edge (id int, cat int, val numeric(15,4));
INSERT INTO t_numeric_edge VALUES
    (1, 1, 0),
    (2, 2, -123.4567),
    (3, 3, 999999999.9999),
    (4, 4, -999999999.9999);
CREATE INDEX t_numeric_edge_idx ON t_numeric_edge USING smol(id, cat) INCLUDE (val);

SELECT * FROM t_numeric_edge ORDER BY id;

-- Test 5: WHERE clause on NUMERIC INCLUDE column
SELECT * FROM t_numeric_basic WHERE val > 200;
SELECT * FROM t_numeric_basic WHERE val < 0;

-- Test 6: Multiple rows
CREATE TABLE t_numeric_many (id int, cat int, val numeric(10,2));
INSERT INTO t_numeric_many SELECT i, i%10, (i*1.5)::numeric(10,2) FROM generate_series(1,50) i;
CREATE INDEX t_numeric_many_idx ON t_numeric_many USING smol(id, cat) INCLUDE (val);

SELECT COUNT(*) FROM t_numeric_many;
SELECT * FROM t_numeric_many WHERE id BETWEEN 10 AND 15 ORDER BY id;

-- Test 7: Single-column index with NUMERIC INCLUDE (should fail - not supported)
\set ON_ERROR_STOP 0
CREATE TABLE t_numeric_single (id int, val numeric(10,2));
CREATE INDEX t_numeric_single_idx ON t_numeric_single USING smol(id) INCLUDE (val);
\set ON_ERROR_STOP 1

-- Test 8: NUMERIC as key column (should fail - not supported)
\set ON_ERROR_STOP 0
CREATE TABLE t_numeric_key (id numeric(10,2), cat int);
CREATE INDEX t_numeric_key_idx ON t_numeric_key USING smol(id, cat);
\set ON_ERROR_STOP 1

-- Test 9: NUMERIC without explicit precision (uses default NUMERIC(18,4))
CREATE TABLE t_numeric_noprec (id int, cat int, val numeric);

-- Insert data BEFORE creating index (SMOL is read-only)
INSERT INTO t_numeric_noprec VALUES
    (1, 100, 123.456789),       -- More than 4 decimal places (should be rounded)
    (2, 100, -999.9999),         -- Negative with exact 4 decimals
    (3, 100, 1000000.12),        -- Large value
    (4, 100, 0.0001);            -- Small value

-- Now create the index (should see NOTICE about default NUMERIC(18,4))
CREATE INDEX t_numeric_noprec_idx ON t_numeric_noprec USING smol(id, cat) INCLUDE (val);

-- Query using the index to verify data is correctly stored and retrieved
SELECT id, cat, val FROM t_numeric_noprec WHERE id >= 1 AND cat = 100 ORDER BY id;

-- Verify the values show proper scaling (rounded to 4 decimals)
SELECT id, val::text FROM t_numeric_noprec WHERE id = 1;

-- Test 10: NUMERIC with precision > 38 (should fail)
\set ON_ERROR_STOP 0
CREATE TABLE t_numeric_huge (id int, cat int, val numeric(50,2));
CREATE INDEX t_numeric_huge_idx ON t_numeric_huge USING smol(id, cat) INCLUDE (val);
\set ON_ERROR_STOP 1

-- Cleanup
DROP TABLE t_numeric_basic CASCADE;
DROP TABLE t_numeric_multi CASCADE;
DROP TABLE t_numeric_sizes CASCADE;
DROP TABLE t_numeric_edge CASCADE;
DROP TABLE t_numeric_many CASCADE;
DROP TABLE IF EXISTS t_numeric_single CASCADE;
DROP TABLE IF EXISTS t_numeric_key CASCADE;
DROP TABLE IF EXISTS t_numeric_noprec CASCADE;
DROP TABLE IF EXISTS t_numeric_huge CASCADE;

-- Additional coverage tests for NUMERIC edge cases

-- Test 11: Mix TEXT and NUMERIC INCLUDE columns (covers TEXT+NUMERIC fast path)
CREATE TABLE t_numeric_text_mix (id int, cat int, name text, price numeric(10,2));
INSERT INTO t_numeric_text_mix VALUES (1, 100, 'item1', 123.45), (2, 100, 'item2', 234.56);
CREATE INDEX t_numeric_text_mix_idx ON t_numeric_text_mix USING smol(id, cat) INCLUDE (name, price);

SELECT id, cat, name, price FROM t_numeric_text_mix ORDER BY id;

-- Test 12: Test with NaN (should fail)
\set ON_ERROR_STOP 0
CREATE TABLE t_numeric_nan (id int, cat int, val numeric(10,2));
INSERT INTO t_numeric_nan VALUES (1, 100, 'NaN'::numeric);
CREATE INDEX t_numeric_nan_idx ON t_numeric_nan USING smol(id, cat) INCLUDE (val);
SELECT * FROM t_numeric_nan;
\set ON_ERROR_STOP 1

-- Test 13: Large table to trigger parallel scan
CREATE TABLE t_numeric_parallel (id int, cat int, val numeric(10,2));
INSERT INTO t_numeric_parallel SELECT i, i%100, (i*1.5)::numeric(10,2) FROM generate_series(1,10000) i;
CREATE INDEX t_numeric_parallel_idx ON t_numeric_parallel USING smol(id, cat) INCLUDE (val);

-- Force parallel scan (expected to error - not fully supported yet)
\set ON_ERROR_STOP 0
SET max_parallel_workers_per_gather = 2;
SET min_parallel_index_scan_size = 0;
SET parallel_tuple_cost = 0;
SET parallel_setup_cost = 0;

SELECT COUNT(*) FROM t_numeric_parallel WHERE val > 5000;
\set ON_ERROR_STOP 1

-- Cleanup
DROP TABLE t_numeric_text_mix CASCADE;
DROP TABLE IF EXISTS t_numeric_nan CASCADE;
DROP TABLE t_numeric_parallel CASCADE;
