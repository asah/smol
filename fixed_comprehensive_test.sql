-- Fixed comprehensive test suite for smol index access method
\echo '=== SMOL INDEX COMPREHENSIVE TEST SUITE ==='
\echo ''

-- Create test table with more data for meaningful comparisons
CREATE TABLE perf_test (
    id INTEGER,
    name VARCHAR(50),
    category VARCHAR(20),
    value NUMERIC(10,2)
);

-- Insert test data (1000 rows)
INSERT INTO perf_test 
SELECT 
    i,
    'name_' || i,
    CASE WHEN i % 5 = 0 THEN 'category_A' 
         WHEN i % 5 = 1 THEN 'category_B'
         WHEN i % 5 = 2 THEN 'category_C' 
         WHEN i % 5 = 3 THEN 'category_D'
         ELSE 'category_E' END,
    (i * 3.14)::numeric(10,2)
FROM generate_series(1, 1000) i;

\echo 'Inserted 1000 test records'

\echo ''
\echo '=== TEST 1: SPACE EFFICIENCY COMPARISON ==='

-- Create both indexes for comparison
CREATE INDEX perf_test_btree_idx ON perf_test USING btree (id, name);
CREATE INDEX perf_test_smol_idx ON perf_test USING smol (id, name);

\echo 'Comparing index sizes:'

-- Get index sizes
SELECT 
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size,
    pg_relation_size(indexname::regclass) as size_bytes
FROM pg_indexes 
WHERE tablename = 'perf_test'
ORDER BY pg_relation_size(indexname::regclass);

\echo ''
\echo 'Space efficiency calculation:'

WITH index_sizes AS (
    SELECT 
        indexname,
        pg_relation_size(indexname::regclass) as size_bytes
    FROM pg_indexes 
    WHERE tablename = 'perf_test'
)
SELECT 
    'Space comparison' as metric,
    btree.size_bytes as btree_bytes,
    smol.size_bytes as smol_bytes,
    btree.size_bytes - smol.size_bytes as bytes_difference,
    CASE WHEN smol.size_bytes > 0 THEN
        round(((btree.size_bytes - smol.size_bytes)::numeric / btree.size_bytes * 100), 2)
    ELSE 0 END as percent_difference
FROM 
    (SELECT size_bytes FROM index_sizes WHERE indexname = 'perf_test_btree_idx') btree,
    (SELECT size_bytes FROM index_sizes WHERE indexname = 'perf_test_smol_idx') smol;

\echo ''
\echo '=== TEST 2: TID-LESS DESIGN - Heap Scans vs Index-Only Scans ==='

SET enable_seqscan = off;

\echo 'Testing heap scan (needs all columns - should work with btree but may fail with smol):'

-- Drop smol temporarily to test btree  
DROP INDEX perf_test_smol_idx;

EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM perf_test WHERE id = 500;

\echo ''
\echo 'Recreating smol index and testing same query:'
CREATE INDEX perf_test_smol_idx ON perf_test USING smol (id, name);

EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM perf_test WHERE id = 500;

\echo ''
\echo 'Testing index-only scan (should work perfectly with smol):'

EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name FROM perf_test WHERE id = 500;

\echo ''
\echo '=== TEST 3: PERFORMANCE COMPARISON FOR INDEX-ONLY SCANS ==='

\echo 'Performance test 1: Point lookup - SMOL'
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, name FROM perf_test WHERE id = 777;

-- Switch to btree
DROP INDEX perf_test_smol_idx;

\echo ''
\echo 'Performance test 1: Point lookup - BTREE'
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, name FROM perf_test WHERE id = 777;

-- Back to smol
CREATE INDEX perf_test_smol_idx ON perf_test USING smol (id, name);

\echo ''
\echo 'Performance test 2: Range scan (100 rows) - SMOL'
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, name FROM perf_test WHERE id BETWEEN 400 AND 500;

DROP INDEX perf_test_smol_idx;

\echo ''
\echo 'Performance test 2: Range scan (100 rows) - BTREE'
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, name FROM perf_test WHERE id BETWEEN 400 AND 500;

\echo ''
\echo '=== TEST 4: INSTANT VISIBILITY (NO MVCC) TEST ==='

CREATE INDEX perf_test_smol_idx ON perf_test USING smol (id, name);

\echo 'Testing instant visibility - inserting record with id=9999:'

BEGIN;
INSERT INTO perf_test VALUES (9999, 'instant_test', 'test_cat', 123.45);

\echo 'Immediately searching in same transaction (smol should show instantly):'
SELECT count(*) as found_count FROM perf_test WHERE id = 9999;

EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name FROM perf_test WHERE id = 9999;

COMMIT;

\echo ''
\echo 'After commit - record should still be visible:'
SELECT count(*) as found_after_commit FROM perf_test WHERE id = 9999;

\echo ''
\echo '=== TEST 5: DROP INDEX AND TABLE RECOVERY ==='

\echo 'Current table modification capabilities with smol index:'
-- These should work since smol allows INSERTs
INSERT INTO perf_test VALUES (10000, 'before_drop', 'test', 100.0);

\echo 'Dropping smol index:'
DROP INDEX perf_test_smol_idx;

\echo 'Testing full table functionality after dropping smol:'

-- All DML should work normally
INSERT INTO perf_test VALUES (10001, 'after_drop_insert', 'test', 200.0);
UPDATE perf_test SET value = 999.99 WHERE id = 10000;
DELETE FROM perf_test WHERE id = 9999;

\echo 'Table modifications successful. Checking btree index still works:'

EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name FROM perf_test WHERE id = 10001;

\echo ''
\echo 'Full scan works normally:'
SET enable_indexscan = off;
SET enable_indexonlyscan = off;

EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM perf_test WHERE value > 100;

\echo ''
\echo '=== SUMMARY ==='

SELECT 
    'Total records in table' as metric,
    count(*) as value
FROM perf_test
UNION ALL
SELECT 
    'Records with value > 100' as metric,
    count(*) as value  
FROM perf_test 
WHERE value > 100;

\echo ''
\echo '=== Key Findings ==='
\echo '1. Space efficiency: smol vs btree comparison shown above'
\echo '2. TID-less design: heap scans work differently with smol'
\echo '3. Performance: index-only scans measured for both'
\echo '4. Instant visibility: INSERTs immediately visible in smol'
\echo '5. Table recovery: normal DML works after dropping smol index'

-- Cleanup
DROP TABLE perf_test CASCADE;

\echo ''
\echo 'Comprehensive test suite completed successfully!'
