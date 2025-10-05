-- Test zero-copy format for unique-key workloads
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test 1: Force enable zero-copy
SET smol.enable_zero_copy = on;
DROP TABLE IF EXISTS zero_on_test CASCADE;
CREATE UNLOGGED TABLE zero_on_test(k int4);
INSERT INTO zero_on_test SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX zero_on_test_idx ON zero_on_test USING smol(k);

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SELECT count(*) FROM zero_on_test WHERE k >= 5000;
SELECT sum(k)::bigint FROM zero_on_test WHERE k >= 5000;

-- Test 2: Force disable zero-copy
SET smol.enable_zero_copy = off;
DROP TABLE IF EXISTS zero_off_test CASCADE;
CREATE UNLOGGED TABLE zero_off_test(k int4);
INSERT INTO zero_off_test SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX zero_off_test_idx ON zero_off_test USING smol(k);

SELECT count(*) FROM zero_off_test WHERE k >= 5000;
SELECT sum(k)::bigint FROM zero_off_test WHERE k >= 5000;

-- Test 3: Auto mode with small unique index (should trigger ON)
SET smol.enable_zero_copy = auto;
SET smol.zero_copy_threshold_mb = 10;
SET smol.rle_uniqueness_threshold = 0.95;
DROP TABLE IF EXISTS auto_on_test CASCADE;
CREATE UNLOGGED TABLE auto_on_test(k int4);
INSERT INTO auto_on_test SELECT i FROM generate_series(1, 5000) i;
CREATE INDEX auto_on_test_idx ON auto_on_test USING smol(k);

SELECT count(*) FROM auto_on_test WHERE k >= 2500;
SELECT sum(k)::bigint FROM auto_on_test WHERE k >= 2500;

-- Test 4: Auto mode with large index (should trigger OFF due to size)
SET smol.enable_zero_copy = auto;
SET smol.zero_copy_threshold_mb = 1;  -- Very small threshold (1MB)
DROP TABLE IF EXISTS auto_off_size_test CASCADE;
CREATE UNLOGGED TABLE auto_off_size_test(k int4);
INSERT INTO auto_off_size_test SELECT i FROM generate_series(1, 100000) i;
CREATE INDEX auto_off_size_test_idx ON auto_off_size_test USING smol(k);

SELECT count(*) FROM auto_off_size_test WHERE k >= 50000;
SELECT sum(k)::bigint FROM auto_off_size_test WHERE k >= 50000;

-- Test 5: Auto mode with duplicate keys (should trigger OFF due to low uniqueness)
SET smol.enable_zero_copy = auto;
SET smol.zero_copy_threshold_mb = 10;
SET smol.rle_uniqueness_threshold = 0.95;
DROP TABLE IF EXISTS auto_off_dup_test CASCADE;
CREATE UNLOGGED TABLE auto_off_dup_test(k int4);
INSERT INTO auto_off_dup_test SELECT (i % 100) FROM generate_series(1, 10000) i;
CREATE INDEX auto_off_dup_test_idx ON auto_off_dup_test USING smol(k);

SELECT count(*) FROM auto_off_dup_test WHERE k >= 50;
SELECT sum(k)::bigint FROM auto_off_dup_test WHERE k >= 50;

-- Cleanup
DROP TABLE zero_on_test, zero_off_test, auto_on_test, auto_off_size_test, auto_off_dup_test CASCADE;
