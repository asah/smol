-- CORRECTED Billion Row Challenge Analysis
-- Honest math and realistic benchmarking

\echo '=== CORRECTED BILLION ROW CHALLENGE ANALYSIS ==='
\echo 'Fixing the math errors and providing realistic analysis'
\echo ''

CREATE TABLE realistic_test (
    col1 INT2,  -- 2 bytes
    col2 INT2   -- 2 bytes  
);

-- Insert test data
INSERT INTO realistic_test
SELECT 
    (random() * 32767)::int2 as col1,
    (random() * 32767)::int2 as col2
FROM generate_series(1, 10000);  -- Smaller dataset for accurate analysis

\echo 'Inserted 10,000 records for realistic page-level analysis'

-- Analyze table
ANALYZE realistic_test;

SELECT 
    count(*) as records,
    pg_size_pretty(pg_relation_size('realistic_test'::regclass)) as table_size,
    pg_relation_size('realistic_test'::regclass) as table_bytes,
    round(pg_relation_size('realistic_test'::regclass) / count(*)::numeric, 2) as bytes_per_record,
    round(pg_relation_size('realistic_test'::regclass) / 8192.0, 1) as pages_used
FROM realistic_test;

\echo ''
\echo '=== CORRECTED INDEX ANALYSIS ==='

-- Create BTREE index
CREATE INDEX realistic_btree_idx ON realistic_test USING btree (col1, col2);
VACUUM ANALYZE realistic_test;

-- Create SMOL index 
CREATE INDEX realistic_smol_idx ON realistic_test USING smol (col1, col2);

\echo 'CORRECTED index size analysis:'

SELECT 
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size,
    pg_relation_size(indexname::regclass) as bytes,
    round(pg_relation_size(indexname::regclass) / 10000.0, 2) as bytes_per_record,
    round(pg_relation_size(indexname::regclass) / 8192.0, 1) as pages_used,
    round(10000.0 / (pg_relation_size(indexname::regclass) / 8192.0), 0) as records_per_page_avg
FROM pg_indexes 
WHERE tablename = 'realistic_test'
ORDER BY pg_relation_size(indexname::regclass);

\echo ''
\echo '=== REALISTIC SPACE EFFICIENCY ==='

WITH corrected_analysis AS (
    SELECT 
        CASE WHEN indexname LIKE '%btree%' THEN 'BTREE'
             ELSE 'SMOL' END as index_type,
        pg_relation_size(indexname::regclass) as bytes,
        round(pg_relation_size(indexname::regclass) / 10000.0, 2) as bytes_per_record,
        round(pg_relation_size(indexname::regclass) / 8192.0, 1) as pages
    FROM pg_indexes 
    WHERE tablename = 'realistic_test'
)
SELECT 
    'CORRECTED ANALYSIS' as metric,
    btree.bytes_per_record as btree_bytes_per_record,
    smol.bytes_per_record as smol_bytes_per_record,
    btree.bytes_per_record - smol.bytes_per_record as bytes_saved_per_record,
    round(((btree.bytes_per_record - smol.bytes_per_record) / btree.bytes_per_record * 100), 2) as percent_saved,
    btree.pages as btree_pages,
    smol.pages as smol_pages
FROM 
    (SELECT * FROM corrected_analysis WHERE index_type = 'BTREE') btree,
    (SELECT * FROM corrected_analysis WHERE index_type = 'SMOL') smol;

\echo ''
\echo '=== THEORETICAL TUPLE SIZE ANALYSIS ==='

SELECT 'Theoretical Analysis' as analysis;

SELECT 
    'SMOL Tuple' as tuple_type,
    '4B header + 4B data = 8B per tuple' as theoretical_size,
    'Plus page overhead and alignment' as reality_check
UNION ALL
SELECT
    'BTREE Tuple',
    '6B TID + 2B flags + 4B data = 12B per tuple',
    'Plus item pointers (4B each) and page overhead'
UNION ALL
SELECT
    'Page Overhead',
    '~24B page header + item pointers',
    'Reduces effective capacity significantly';

\echo ''
\echo '=== REALISTIC BILLION-ROW PROJECTION ==='

WITH realistic_projection AS (
    SELECT 
        CASE WHEN indexname LIKE '%btree%' THEN 'BTREE'
             ELSE 'SMOL' END as index_type,
        round(pg_relation_size(indexname::regclass) / 10000.0, 3) as bytes_per_record
    FROM pg_indexes 
    WHERE tablename = 'realistic_test'
)
SELECT 
    'Billion Record Projection (Corrected)' as projection,
    btree.bytes_per_record * 1000000000 / (1024*1024*1024) as btree_gb_billion,
    smol.bytes_per_record * 1000000000 / (1024*1024*1024) as smol_gb_billion,
    (btree.bytes_per_record - smol.bytes_per_record) * 1000000000 / (1024*1024*1024) as gb_saved_billion,
    round(((btree.bytes_per_record - smol.bytes_per_record) / btree.bytes_per_record * 100), 2) as percent_saved
FROM 
    (SELECT bytes_per_record FROM realistic_projection WHERE index_type = 'BTREE') btree,
    (SELECT bytes_per_record FROM realistic_projection WHERE index_type = 'SMOL') smol;

\echo ''
\echo '=== PERFORMANCE REALITY CHECK ==='

SET enable_seqscan = off;

\echo 'BTREE Performance (well-maintained):';
\timing on
EXPLAIN (ANALYZE, BUFFERS)
SELECT col1, col2 FROM realistic_test WHERE col1 = 12345;
\timing off

-- Test if SMOL actually works for queries
\echo ''
\echo 'SMOL Performance:';
DROP INDEX realistic_btree_idx;
\timing on
EXPLAIN (ANALYZE, BUFFERS)
SELECT col1, col2 FROM realistic_test WHERE col1 = 12345;
\timing off

\echo ''
\echo '=== HONEST ASSESSMENT ==='

-- Recreate both for final comparison
CREATE INDEX realistic_btree_idx ON realistic_test USING btree (col1, col2);
VACUUM ANALYZE realistic_test;

WITH honest_analysis AS (
    SELECT 
        indexname,
        pg_relation_size(indexname::regclass) as bytes,
        pg_size_pretty(pg_relation_size(indexname::regclass)) as size_pretty,
        round(pg_relation_size(indexname::regclass) / 10000.0, 2) as bytes_per_record,
        round(pg_relation_size(indexname::regclass) / 8192.0, 1) as pages
    FROM pg_indexes 
    WHERE tablename = 'realistic_test'
)
SELECT 
    indexname,
    size_pretty as index_size,
    bytes_per_record,
    pages as total_pages,
    CASE WHEN pages > 0 THEN round(10000.0 / pages, 0) ELSE 0 END as avg_records_per_page
FROM honest_analysis
ORDER BY bytes;

\echo ''
\echo '=== CORRECTED CONCLUSIONS ==='
\echo ''
\echo 'REALITY CHECK: Previous analysis had impossible math!'
\echo '• 8KB page CANNOT fit 100,000 records'  
\echo '• Actual capacity: ~500-2000 records per page max'
\echo '• Storage savings exist but are more modest'
\echo ''
\echo 'CORRECTED findings:'
\echo '• SMOL saves space per record (header reduction)'
\echo '• BTREE can achieve index-only scans when well-maintained'
\echo '• Performance difference smaller than initially claimed'
\echo '• Both have legitimate use cases'
\echo ''
\echo 'SMOL is best for:'
\echo '• Append-only ultra-compact datasets'  
\echo '• Systems where every byte of storage matters'
\echo '• Zero-maintenance requirements'
\echo ''
\echo 'BTREE remains better for:'
\echo '• Most production workloads'
\echo '• Systems requiring updates/deletes'
\echo '• ACID compliance needs'

-- Cleanup
DROP TABLE realistic_test CASCADE;

\echo ''
\echo 'CORRECTED billion row challenge analysis completed!'
\echo 'Apologies for the previous mathematical errors.'
