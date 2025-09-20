-- Debug and comprehensive test suite for smol index
\echo '=== SMOL INDEX DEBUG AND COMPREHENSIVE TEST ==='
\echo ''

-- Create test table
CREATE TABLE debug_test (
    id INTEGER,
    name VARCHAR(50),
    value NUMERIC(10,2)
);

\echo '=== STEP 1: Test Basic Functionality ==='

-- Insert small amount of data first
INSERT INTO debug_test VALUES (1, 'test1', 10.5), (2, 'test2', 20.0), (3, 'test3', 30.25);

\echo 'Data inserted. Checking table contents:'
SELECT * FROM debug_test ORDER BY id;

\echo ''
\echo 'Creating btree index first (for comparison):'
CREATE INDEX debug_btree_idx ON debug_test USING btree (id, name);

\echo 'Btree index size:'
SELECT pg_size_pretty(pg_relation_size('debug_btree_idx'::regclass)) as btree_size,
       pg_relation_size('debug_btree_idx'::regclass) as btree_bytes;

\echo ''
\echo 'Creating smol index:'
CREATE INDEX debug_smol_idx ON debug_test USING smol (id, name);

\echo 'Smol index size:'
SELECT pg_size_pretty(pg_relation_size('debug_smol_idx'::regclass)) as smol_size,
       pg_relation_size('debug_smol_idx'::regclass) as smol_bytes;

\echo ''
\echo '=== STEP 2: Test Index Usage ==='

\echo 'Testing btree index scan:'
SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_indexonlyscan = on;

-- Force btree usage
DROP INDEX debug_smol_idx;

EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name FROM debug_test WHERE id = 2;

\echo ''
\echo 'Recreating smol index and testing:'
CREATE INDEX debug_smol_idx ON debug_test USING smol (id, name);

EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name FROM debug_test WHERE id = 2;

\echo ''
\echo '=== STEP 3: Test INSERT After Index Creation (Instant Visibility) ==='

\echo 'Current time and inserting new record:'
SELECT now() as insert_time;
INSERT INTO debug_test VALUES (99, 'instant_test', 99.99);

\echo 'Immediately querying for new record (should be instantly visible):'
SELECT now() as query_time;

-- This should find the record immediately with smol (no MVCC)
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name FROM debug_test WHERE id = 99;

\echo 'Checking if record appears in regular query:'
SELECT * FROM debug_test WHERE id = 99;

\echo ''
\echo '=== STEP 4: Test That Regular Heap Scans Fail ==='

\echo 'Testing query that needs heap access (should return no data or fail):'

-- This should fail/return no data because smol has no TIDs for heap access
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM debug_test WHERE id = 2;

\echo ''
\echo '=== STEP 5: Test DROP INDEX and Table Recovery ==='

\echo 'Dropping smol index:'
DROP INDEX debug_smol_idx;

\echo 'Testing that table becomes fully functional again:'

-- These should all work normally after dropping smol index
INSERT INTO debug_test VALUES (100, 'after_drop', 100.00);
UPDATE debug_test SET value = 999.99 WHERE id = 1;
DELETE FROM debug_test WHERE id = 3;

\echo 'Table contents after modifications:'
SELECT * FROM debug_test ORDER BY id;

\echo ''
\echo 'Testing btree index still works normally:'
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name FROM debug_test WHERE id = 100;

\echo ''
\echo 'Full table scan works:'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM debug_test WHERE value > 50;

\echo ''
\echo '=== STEP 6: Recreate Smol and Test INSERT Behavior ==='

\echo 'Recreating smol index on current data:'
CREATE INDEX debug_smol_idx ON debug_test USING smol (id, name);

\echo 'Index sizes after recreation:'
SELECT 
    'btree' as index_type,
    pg_size_pretty(pg_relation_size('debug_btree_idx'::regclass)) as size,
    pg_relation_size('debug_btree_idx'::regclass) as bytes
UNION ALL
SELECT 
    'smol' as index_type,
    pg_size_pretty(pg_relation_size('debug_smol_idx'::regclass)) as size,
    pg_relation_size('debug_smol_idx'::regclass) as bytes;

\echo ''
\echo 'Testing if smol index sees existing data:'
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, name FROM debug_test WHERE id = 100;

\echo ''
\echo 'Testing new INSERT with smol index:'
INSERT INTO debug_test VALUES (200, 'new_with_smol', 200.00);

EXPLAIN (ANALYZE, BUFFERS)  
SELECT id, name FROM debug_test WHERE id = 200;

\echo ''
\echo '=== STEP 7: MVCC Comparison Test ==='

\echo 'Starting transaction to test MVCC behavior...'

BEGIN;
INSERT INTO debug_test VALUES (300, 'mvcc_test', 300.00);

\echo 'In same transaction - should see record:'
SELECT id, name FROM debug_test WHERE id = 300;

-- In a different session, this record wouldn't be visible with normal MVCC
-- But with smol (no MVCC), it should be visible immediately
COMMIT;

\echo 'After commit - record should be visible:'
SELECT id, name FROM debug_test WHERE id = 300;

\echo ''
\echo '=== DIAGNOSTIC INFORMATION ==='

\echo 'Current indexes on table:'
SELECT schemaname, tablename, indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'debug_test';

\echo ''
\echo 'Index statistics:'
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as scans_used,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes 
WHERE tablename = 'debug_test';

-- Cleanup
DROP TABLE debug_test CASCADE;

\echo ''
\echo '=== DEBUG TEST COMPLETED ==='
\echo 'If smol index shows 0 bytes, the insert/storage logic needs fixing.'
\echo 'If queries return 0 rows, the scan logic needs fixing.'
\echo 'If MVCC test fails, transaction handling needs review.'
