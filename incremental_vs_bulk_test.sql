-- Test: Incremental vs Bulk INSERT Theory
\echo '=== INCREMENTAL vs BULK INSERT TEST ==='

-- Test 1: Bulk insert then create index (current failing approach)
\echo 'Test 1: Bulk insert 500 records, then create index';
CREATE TABLE bulk_test (x SMALLINT, y SMALLINT);
INSERT INTO bulk_test SELECT i::smallint, (i*10)::smallint FROM generate_series(1,500) i;
CREATE INDEX bulk_smol ON bulk_test USING smol (x, y);

SELECT 'Bulk approach:' as method, count(*) as results FROM bulk_test WHERE y > 2500;

-- Test 2: Create index first, then incremental inserts
\echo 'Test 2: Create index first, then incremental inserts';
CREATE TABLE incremental_test (x SMALLINT, y SMALLINT);
CREATE INDEX incremental_smol ON incremental_test USING smol (x, y);

-- Insert in batches
INSERT INTO incremental_test SELECT i::smallint, (i*10)::smallint FROM generate_series(1,100) i;
INSERT INTO incremental_test SELECT i::smallint, (i*10)::smallint FROM generate_series(101,200) i; 
INSERT INTO incremental_test SELECT i::smallint, (i*10)::smallint FROM generate_series(201,300) i;
INSERT INTO incremental_test SELECT i::smallint, (i*10)::smallint FROM generate_series(301,400) i;
INSERT INTO incremental_test SELECT i::smallint, (i*10)::smallint FROM generate_series(401,500) i;

SELECT 'Incremental approach:' as method, count(*) as results FROM incremental_test WHERE y > 2500;

-- Check sizes
SELECT 
    'bulk' as approach,
    pg_relation_size('bulk_smol'::regclass) as bytes,
    relpages as pages,
    reltuples as estimated
FROM pg_class WHERE relname = 'bulk_smol'
UNION ALL
SELECT 
    'incremental',
    pg_relation_size('incremental_smol'::regclass),
    relpages,
    reltuples  
FROM pg_class WHERE relname = 'incremental_smol';

DROP TABLE bulk_test CASCADE;
DROP TABLE incremental_test CASCADE;

\echo 'Theory test completed!';
