-- Cover INT8OID case in smol_bloom_build_page (smol_utils.c:1021-1026)
-- Strategy: Like bloom_skip_coverage but with INT8 data type
SET smol.bloom_filters = on;
SET smol.build_bloom_filters = on;
SET smol.profile = on;

-- Create multi-page INT8 table with rightlink traversal
CREATE UNLOGGED TABLE test_int8_bloom_skip(k int8);

-- Value 1 at beginning
INSERT INTO test_int8_bloom_skip SELECT 1::int8 FROM generate_series(1, 625);

-- Many different values to create middle pages
INSERT INTO test_int8_bloom_skip SELECT ((i % 10000) + 2)::int8 FROM generate_series(1, 125000) i;

-- Value 1 at end to force rightlink traversal
INSERT INTO test_int8_bloom_skip SELECT 1::int8 FROM generate_series(1, 625);

-- Force small pages
SET smol.test_max_tuples_per_page = 10;

-- Create index with bloom
CREATE INDEX test_int8_bloom_skip_idx ON test_int8_bloom_skip USING smol(k);

-- Force bloom rejection to trigger skip logic
SET smol.test_force_bloom_rejection = on;
SET enable_seqscan = off;

-- Scan - will call smol_bloom_build_page with INT8OID
SELECT count(*) FROM test_int8_bloom_skip WHERE k = 1;

-- Cleanup
SET smol.test_force_bloom_rejection = off;
DROP TABLE test_int8_bloom_skip CASCADE;
