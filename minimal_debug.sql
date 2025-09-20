-- Minimal Debug: Find exact breaking point
\echo '=== MINIMAL SMOL DEBUG ==='

CREATE TABLE minimal_test (x SMALLINT, y SMALLINT);

-- Test exactly at the boundary where it breaks
-- 200 works, 500 fails, so test 300, 350, 400
INSERT INTO minimal_test SELECT i::smallint, (i*10)::smallint FROM generate_series(1,300) i;
CREATE INDEX minimal_smol ON minimal_test USING smol (x, y);

\echo 'Test 300 records:';
SELECT count(*) as count_300 FROM minimal_test WHERE y > 1500;

-- Add more records to same table and index
INSERT INTO minimal_test SELECT i::smallint, (i*10)::smallint FROM generate_series(301,350) i;

\echo 'Test 350 records:';
SELECT count(*) as count_350 FROM minimal_test WHERE y > 1500;

-- Add more
INSERT INTO minimal_test SELECT i::smallint, (i*10)::smallint FROM generate_series(351,400) i;

\echo 'Test 400 records:';
SELECT count(*) as count_400 FROM minimal_test WHERE y > 1500;

-- Check index stats  
SELECT 
    pg_relation_size('minimal_smol'::regclass) as bytes,
    relpages as pages,
    reltuples as estimated_tuples
FROM pg_class WHERE relname = 'minimal_smol';

-- Try forcing index usage
SET enable_seqscan = off;

\echo 'Forced index scan test:';
EXPLAIN (ANALYZE, BUFFERS)
SELECT x, y FROM minimal_test WHERE x = 200;

DROP TABLE minimal_test CASCADE;

\echo 'Minimal debug completed';
