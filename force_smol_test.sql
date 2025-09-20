-- Force SMOL usage test
\echo '=== FORCE SMOL USAGE TEST ==='

CREATE TABLE force_test (x SMALLINT, y SMALLINT);
INSERT INTO force_test SELECT i::smallint, (i*10)::smallint FROM generate_series(1,500) i;

-- Create only SMOL index (no competing indexes)
CREATE INDEX force_smol_only ON force_test USING smol (x, y);

-- Force index usage
SET enable_seqscan = off;
SET enable_bitmapscan = off;

\echo 'Testing forced SMOL usage:';

-- Simple query
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT x FROM force_test WHERE x = 100;

-- Check what the planner thinks
SELECT 
    'Index capabilities' as info,
    pg_size_pretty(pg_relation_size('force_smol_only'::regclass)) as index_size,
    relpages as pages,
    reltuples as tuples
FROM pg_class WHERE relname = 'force_smol_only';

-- Try without WHERE clause
SELECT 'Full scan test:' as test;
SELECT count(*) FROM force_test;

-- Try with explicit index hint if possible
SET enable_seqscan = on;
SET enable_indexscan = on;
SET enable_indexonlyscan = on;

EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM force_test;

DROP TABLE force_test CASCADE;
