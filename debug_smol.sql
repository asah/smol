-- Debug SMOL: Find where it breaks between 10 and 1000 records
\echo '=== DEBUGGING SMOL IMPLEMENTATION ==='
\echo ''

-- Test different sizes to find the breaking point
\echo 'Testing incremental sizes to find where SMOL breaks...'

-- Test 50 records
CREATE TABLE debug_50 (x SMALLINT, y SMALLINT);
INSERT INTO debug_50 SELECT i::smallint, (i*10)::smallint FROM generate_series(1,50) i;
CREATE INDEX debug_50_smol ON debug_50 USING smol (x, y);

\echo 'Test 50 records:'
SELECT count(*) as count_50, sum(x) as sum_50 FROM debug_50 WHERE y > 250;
SELECT pg_relation_size('debug_50_smol'::regclass) as bytes_50;

-- Test 200 records
CREATE TABLE debug_200 (x SMALLINT, y SMALLINT);
INSERT INTO debug_200 SELECT i::smallint, (i*10)::smallint FROM generate_series(1,200) i;
CREATE INDEX debug_200_smol ON debug_200 USING smol (x, y);

\echo 'Test 200 records:'
SELECT count(*) as count_200, sum(x) as sum_200 FROM debug_200 WHERE y > 1000;
SELECT pg_relation_size('debug_200_smol'::regclass) as bytes_200;

-- Test 500 records
CREATE TABLE debug_500 (x SMALLINT, y SMALLINT);
INSERT INTO debug_500 SELECT i::smallint, (i*10)::smallint FROM generate_series(1,500) i;
CREATE INDEX debug_500_smol ON debug_500 USING smol (x, y);

\echo 'Test 500 records:'
SELECT count(*) as count_500, sum(x) as sum_500 FROM debug_500 WHERE y > 2500;
SELECT pg_relation_size('debug_500_smol'::regclass) as bytes_500;

-- Test 1000 records
CREATE TABLE debug_1000 (x SMALLINT, y SMALLINT);
INSERT INTO debug_1000 SELECT i::smallint, (i*10)::smallint FROM generate_series(1,1000) i;
CREATE INDEX debug_1000_smol ON debug_1000 USING smol (x, y);

\echo 'Test 1000 records:'
SELECT count(*) as count_1000, sum(x) as sum_1000 FROM debug_1000 WHERE y > 5000;
SELECT pg_relation_size('debug_1000_smol'::regclass) as bytes_1000;

\echo ''
\echo 'Index page information:'
SELECT 
    'debug_50' as test,
    relpages as pages,
    reltuples as estimated_tuples
FROM pg_class WHERE relname = 'debug_50_smol'
UNION ALL
SELECT 
    'debug_200',
    relpages,
    reltuples
FROM pg_class WHERE relname = 'debug_200_smol'  
UNION ALL
SELECT
    'debug_500', 
    relpages,
    reltuples
FROM pg_class WHERE relname = 'debug_500_smol'
UNION ALL
SELECT
    'debug_1000',
    relpages, 
    reltuples
FROM pg_class WHERE relname = 'debug_1000_smol';

-- Cleanup
DROP TABLE debug_50 CASCADE;
DROP TABLE debug_200 CASCADE;
DROP TABLE debug_500 CASCADE;
DROP TABLE debug_1000 CASCADE;

\echo ''
\echo 'Debug completed - checking where counts drop to 0'
