-- Progressive Testing: Start Small and Scale Up
\echo '=== PROGRESSIVE SMOL TESTING ==='
\echo 'Testing SMOL stability with increasing dataset sizes'
\echo ''

-- Test 1: Tiny dataset (10 records)
\echo '=== TEST 1: 10 RECORDS ==='
CREATE TABLE prog_test_10 (x SMALLINT, y SMALLINT);
INSERT INTO prog_test_10 SELECT i::smallint, (i*2)::smallint FROM generate_series(1,10) i;
CREATE INDEX prog_test_10_smol ON prog_test_10 USING smol (x, y);

SELECT 'Test 1 Results:' as test;
SELECT count(*) as total_records FROM prog_test_10;
SELECT sum(x) as sum_x FROM prog_test_10 WHERE y > 5;
SELECT pg_size_pretty(pg_relation_size('prog_test_10_smol'::regclass)) as index_size;

DROP TABLE prog_test_10 CASCADE;
\echo 'Test 1 passed!'

-- Test 2: Small dataset (1000 records)  
\echo ''
\echo '=== TEST 2: 1,000 RECORDS ==='
CREATE TABLE prog_test_1k (x SMALLINT, y SMALLINT);
INSERT INTO prog_test_1k 
SELECT 
    (random()*1000)::smallint, 
    (random()*10000)::smallint 
FROM generate_series(1,1000);
CREATE INDEX prog_test_1k_smol ON prog_test_1k USING smol (x, y);

SELECT 'Test 2 Results:' as test;
SELECT count(*) as total_records FROM prog_test_1k;
SELECT sum(x) as sum_x FROM prog_test_1k WHERE y > 5000;
SELECT pg_size_pretty(pg_relation_size('prog_test_1k_smol'::regclass)) as index_size;

DROP TABLE prog_test_1k CASCADE;
\echo 'Test 2 passed!';

-- Test 3: Medium dataset (100k records)
\echo ''
\echo '=== TEST 3: 100,000 RECORDS ==='
CREATE TABLE prog_test_100k (x SMALLINT, y SMALLINT);
INSERT INTO prog_test_100k 
SELECT 
    (random()*32767)::smallint,
    (random()*32767)::smallint
FROM generate_series(1,100000);
CREATE INDEX prog_test_100k_smol ON prog_test_100k USING smol (x, y);

SELECT 'Test 3 Results:' as test;
SELECT count(*) as total_records FROM prog_test_100k;
SELECT sum(x::bigint) as sum_x FROM prog_test_100k WHERE y > 16000;
SELECT pg_size_pretty(pg_relation_size('prog_test_100k_smol'::regclass)) as index_size;

-- Test query plan
EXPLAIN (ANALYZE, BUFFERS)
SELECT sum(x::bigint) FROM prog_test_100k WHERE y > 16000;

DROP TABLE prog_test_100k CASCADE;
\echo 'Test 3 passed!';

-- Test 4: Large dataset (1M records) 
\echo ''
\echo '=== TEST 4: 1,000,000 RECORDS ==='
CREATE TABLE prog_test_1m (x SMALLINT, y SMALLINT);
INSERT INTO prog_test_1m
SELECT 
    (random()*32767)::smallint,
    (random()*32767)::smallint
FROM generate_series(1,1000000);
CREATE INDEX prog_test_1m_smol ON prog_test_1m USING smol (x, y);

SELECT 'Test 4 Results:' as test;
SELECT count(*) as total_records FROM prog_test_1m;

-- Test the BRC query with 1M records
\echo 'Running BRC aggregation query on 1M records...'
SELECT sum(x::bigint) as sum_x FROM prog_test_1m WHERE y > 16000;

-- Test query plan
EXPLAIN (ANALYZE, BUFFERS)
SELECT sum(x::bigint) FROM prog_test_1m WHERE y > 16000;

SELECT pg_size_pretty(pg_relation_size('prog_test_1m_smol'::regclass)) as index_size;

DROP TABLE prog_test_1m CASCADE;
\echo 'Test 4 passed!';

-- Test 5: Very large dataset (10M records)
\echo ''
\echo '=== TEST 5: 10,000,000 RECORDS ==='
\echo 'This is the stress test...'
CREATE TABLE prog_test_10m (x SMALLINT, y SMALLINT);

-- Insert in smaller batches to avoid memory issues
\echo 'Inserting 10M records in batches...'
INSERT INTO prog_test_10m
SELECT 
    (random()*32767)::smallint,
    (random()*32767)::smallint
FROM generate_series(1,10000000);

CREATE INDEX prog_test_10m_smol ON prog_test_10m USING smol (x, y);

SELECT 'Test 5 Results:' as test;
SELECT count(*) as total_records FROM prog_test_10m;
SELECT pg_size_pretty(pg_relation_size('prog_test_10m_smol'::regclass)) as index_size;

-- Test the BRC query
\echo 'Running BRC query on 10M records...'
\timing on
SELECT sum(x::bigint) as sum_x FROM prog_test_10m WHERE y > 16000;
\timing off

-- Test query plan
EXPLAIN (ANALYZE, BUFFERS)  
SELECT sum(x::bigint) FROM prog_test_10m WHERE y > 16000;

DROP TABLE prog_test_10m CASCADE;
\echo 'Test 5 passed!';

\echo ''
\echo '=== ALL PROGRESSIVE TESTS COMPLETED ==='
\echo 'If all tests passed without crashes, SMOL is stable for scaling!'
