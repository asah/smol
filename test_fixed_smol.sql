-- Test Fixed SMOL Implementation
\echo '=== TESTING FIXED SMOL IMPLEMENTATION ==='
\echo ''

CREATE TABLE test_fix (
    x SMALLINT,
    y SMALLINT  
);

-- Insert test data
INSERT INTO test_fix VALUES (1, 10), (2, 20), (3, 30), (4, 5000), (5, 6000);

\echo 'Test data inserted. Creating SMOL index...'

CREATE INDEX test_fix_smol_idx ON test_fix USING smol (x, y);

\echo 'Index created. Testing basic functionality:'

-- Test simple select
SELECT 'Basic select test:' as test;
SELECT x, y FROM test_fix WHERE x = 2;

-- Test aggregation  
SELECT 'Aggregation test:' as test;
SELECT sum(x) FROM test_fix WHERE y > 25;

-- Test index-only scan plan
SELECT 'Query plan test:' as test;
EXPLAIN (ANALYZE, BUFFERS)
SELECT x, y FROM test_fix WHERE y > 25;

\echo ''
\echo 'If these queries return correct data, SMOL is fixed!'

DROP TABLE test_fix CASCADE;
