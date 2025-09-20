-- BRC Benchmark Test: SELECT sum(x) FROM table WHERE y>5000
-- Comparing SMOL vs BTREE for large aggregation queries

\echo '=== BRC AGGREGATION BENCHMARK TEST ==='
\echo 'Testing: SELECT sum(x) FROM tmp_brc WHERE y>5000'
\echo 'Comparing SMOL vs BTREE index-only scan performance'
\echo ''

-- Create test table matching your schema
CREATE TABLE tmp_brc (
    x SMALLINT,
    y SMALLINT
);

-- Insert test data (scaled version of your 100M records)
-- Using 1M records for realistic testing in container
\echo 'Loading 1M records (scaled version of 100M BRC dataset)...'

INSERT INTO tmp_brc 
SELECT 
    (random() * 32767)::smallint as x,
    (random() * 32767)::smallint as y
FROM generate_series(1, 1000000);

-- Analyze for good statistics
ANALYZE tmp_brc;

\echo 'Data loaded. Table analysis:'

SELECT 
    'Table Stats' as metric,
    count(*) as records,
    pg_size_pretty(pg_relation_size('tmp_brc'::regclass)) as table_size,
    round(pg_relation_size('tmp_brc'::regclass) / count(*)::numeric, 2) as bytes_per_record
FROM tmp_brc;

\echo ''
\echo '=== INDEX CREATION AND SIZE COMPARISON ==='

-- Create BTREE indexes (both regular and covering like your test)
CREATE INDEX tmp_brc_idx ON tmp_brc (x, y);
CREATE INDEX tmp_brc_idx_cov ON tmp_brc (x) INCLUDE (y);
VACUUM ANALYZE tmp_brc;

-- Create SMOL index
CREATE INDEX tmp_brc_smol_idx ON tmp_brc USING smol (x, y);

\echo 'Index size comparison:'

SELECT 
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size,
    pg_relation_size(indexname::regclass) as bytes,
    round(pg_relation_size(indexname::regclass) / 1000000.0, 2) as bytes_per_record,
    round(pg_relation_size(indexname::regclass) / 8192.0, 0) as pages_used
FROM pg_indexes 
WHERE tablename = 'tmp_brc'
ORDER BY pg_relation_size(indexname::regclass);

\echo ''
\echo 'Space efficiency vs BTREE regular index:'

WITH index_comparison AS (
    SELECT 
        CASE WHEN indexname = 'tmp_brc_idx' THEN 'BTREE_REGULAR'
             WHEN indexname = 'tmp_brc_idx_cov' THEN 'BTREE_INCLUDE'  
             WHEN indexname = 'tmp_brc_smol_idx' THEN 'SMOL'
        END as index_type,
        pg_relation_size(indexname::regclass) as bytes
    FROM pg_indexes 
    WHERE tablename = 'tmp_brc'
)
SELECT 
    'Space Comparison' as analysis,
    btree.bytes / (1024*1024) as btree_mb,
    smol.bytes / (1024*1024) as smol_mb,
    (btree.bytes - smol.bytes) / (1024*1024) as mb_saved,
    round(((btree.bytes - smol.bytes)::numeric / btree.bytes * 100), 2) as percent_saved
FROM 
    (SELECT bytes FROM index_comparison WHERE index_type = 'BTREE_REGULAR') btree,
    (SELECT bytes FROM index_comparison WHERE index_type = 'SMOL') smol;

\echo ''
\echo '=== SELECTIVITY ANALYSIS ==='

-- Check how many records match y>5000 (should be ~85% with random data 0-32767)
SELECT 
    'Query Selectivity' as analysis,
    count(*) as total_records,
    count(*) FILTER (WHERE y > 5000) as matching_records,
    round(count(*) FILTER (WHERE y > 5000) * 100.0 / count(*), 2) as selectivity_percent
FROM tmp_brc;

\echo ''
\echo '=== PERFORMANCE BENCHMARK: BTREE REGULAR INDEX ==='

-- Test BTREE regular index performance
SET enable_seqscan = off;
SET enable_bitmapscan = off;  -- Force index-only scan

\echo 'BTREE Regular Index Performance:'
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(x) FROM tmp_brc WHERE y > 5000;
\timing off

-- Actually run the query to get real timing
\echo ''
\echo 'BTREE Regular Index - Actual Execution:'
\timing on
SELECT sum(x) FROM tmp_brc WHERE y > 5000;
\timing off

\echo ''
\echo '=== PERFORMANCE BENCHMARK: BTREE INCLUDE INDEX ==='

-- Force use of INCLUDE index
DROP INDEX tmp_brc_idx;

\echo 'BTREE INCLUDE Index Performance:'
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(x) FROM tmp_brc WHERE y > 5000;
\timing off

\echo ''
\echo 'BTREE INCLUDE Index - Actual Execution:'
\timing on
SELECT sum(x) FROM tmp_brc WHERE y > 5000;
\timing off

\echo ''
\echo '=== PERFORMANCE BENCHMARK: SMOL INDEX ==='

-- Switch to SMOL index only
DROP INDEX tmp_brc_idx_cov;

\echo 'SMOL Index Performance:'
\timing on
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(x) FROM tmp_brc WHERE y > 5000;
\timing off

\echo ''
\echo 'SMOL Index - Actual Execution:'
\timing on
SELECT sum(x) FROM tmp_brc WHERE y > 5000;
\timing off

\echo ''
\echo '=== 100M RECORD PROJECTION ==='

-- Project to 100M records like user's real dataset
WITH scaling_analysis AS (
    SELECT 
        'SMOL vs BTREE Projection' as analysis,
        pg_relation_size('tmp_brc_smol_idx'::regclass) * 100 as smol_100m_bytes,
        pg_size_pretty(pg_relation_size('tmp_brc_smol_idx'::regclass) * 100) as smol_100m_size
)
SELECT 
    analysis,
    '2123 MB' as btree_100m_actual,  -- From user's real data
    smol_100m_size as smol_100m_projected,
    pg_size_pretty(2123 * 1024 * 1024 - smol_100m_bytes) as projected_savings
FROM scaling_analysis;

\echo ''
\echo '=== CACHE EFFICIENCY ANALYSIS ==='

-- Recreate all indexes for final comparison
CREATE INDEX tmp_brc_idx ON tmp_brc (x, y);
CREATE INDEX tmp_brc_idx_cov ON tmp_brc (x) INCLUDE (y);

WITH cache_analysis AS (
    SELECT 
        indexname,
        pg_relation_size(indexname::regclass) as bytes,
        round(pg_relation_size(indexname::regclass) / 8192.0, 0) as pages,
        round(1000000.0 / (pg_relation_size(indexname::regclass) / 8192.0), 0) as records_per_page
    FROM pg_indexes 
    WHERE tablename = 'tmp_brc'
)
SELECT 
    indexname,
    pg_size_pretty(bytes) as index_size,
    pages as total_pages,
    records_per_page,
    round(pages * 0.85, 0) as pages_scanned_for_query  -- 85% selectivity
FROM cache_analysis
ORDER BY bytes;

\echo ''
\echo '=== BENCHMARK CONCLUSIONS ==='
\echo ''
\echo 'Query: SELECT sum(x) FROM tmp_brc WHERE y>5000'
\echo 'Selectivity: ~85% of records (y>5000 with random 0-32767 data)'
\echo ''
\echo 'Key Findings:'
\echo '1. Index size comparison (SMOL vs BTREE)'
\echo '2. Query execution time differences'  
\echo '3. Buffer/page read efficiency'
\echo '4. Scaling projections to 100M records'
\echo ''
\echo 'Expected SMOL advantages:'
\echo '• Smaller index = less I/O for large scans'
\echo '• Better cache utilization'
\echo '• Consistent index-only scan behavior'
\echo ''
\echo 'BTREE advantages:'
\echo '• Can achieve similar index-only performance when well-maintained'
\echo '• More flexible for different query patterns'
\echo '• Production-proven reliability'

-- Cleanup
DROP TABLE tmp_brc CASCADE;

\echo ''
\echo 'BRC aggregation benchmark completed!'
