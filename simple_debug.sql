-- Simple debug to trace SMOL execution
\echo '=== SIMPLE SMOL EXECUTION TRACE ==='

-- Start with working case
CREATE TABLE trace_test (x SMALLINT, y SMALLINT);
INSERT INTO trace_test VALUES (1, 100), (2, 200), (3, 300);
CREATE INDEX trace_smol ON trace_test USING smol (x, y);

\echo 'Testing 3 records (should work):';
SELECT x, y FROM trace_test WHERE x = 2;

-- Add more data incrementally
INSERT INTO trace_test SELECT i::smallint, (i*100)::smallint FROM generate_series(4, 50) i;

\echo 'Testing 50 records (should work):';  
SELECT count(*) FROM trace_test WHERE y > 1000;

-- Add more to push to page boundary
INSERT INTO trace_test SELECT i::smallint, (i*100)::smallint FROM generate_series(51, 300) i;

\echo 'Testing 300 records (boundary case):';
SELECT count(*) FROM trace_test WHERE y > 1000;

-- Check index structure
SELECT 
    pg_relation_size('trace_smol'::regclass) as bytes,
    relpages as pages,
    reltuples as estimated
FROM pg_class WHERE relname = 'trace_smol';

-- Check if smolcanreturn works
\echo 'Testing attribute return capability:';
EXPLAIN (COSTS OFF)
SELECT x FROM trace_test WHERE x = 100;

EXPLAIN (COSTS OFF)  
SELECT y FROM trace_test WHERE x = 100;

EXPLAIN (COSTS OFF)
SELECT x, y FROM trace_test WHERE x = 100;

DROP TABLE trace_test CASCADE;
