-- Scaled BRC Benchmark: 20M records with accurate size measurement
-- Focus on actual measurements, not theoretical calculations

\echo '=== SCALED BRC BENCHMARK: 20M RECORDS ==='
\echo 'Rigorous testing with actual size measurements'
\echo ''

CREATE TABLE scaled_brc (
    x SMALLINT,
    y SMALLINT
);

\echo 'Loading 20M records (20x scale of original test)...'
\echo 'This may take a few minutes...'

-- Insert 20M records in batches for better performance
INSERT INTO scaled_brc 
SELECT 
    (random() * 32767)::smallint as x,
    (random() * 32767)::smallint as y
FROM generate_series(1, 20000000);

ANALYZE scaled_brc;

\echo 'Data loaded. Baseline analysis:'

SELECT 
    'Table Analysis' as metric,
    count(*) as records,
    pg_size_pretty(pg_relation_size('scaled_brc'::regclass)) as table_size,
    pg_relation_size('scaled_brc'::regclass) as table_bytes,
    round(pg_relation_size('scaled_brc'::regclass) / count(*)::numeric, 2) as bytes_per_record
FROM scaled_brc;

\echo ''
\echo '=== STEP 1: BTREE INDEX CREATION ==='

-- Create BTREE index first
\echo 'Creating BTREE index on 20M records...'
CREATE INDEX scaled_brc_btree_idx ON scaled_brc (x, y);
VACUUM ANALYZE scaled_brc;

SELECT 
    'BTREE Index' as index_type,
    pg_size_pretty(pg_relation_size('scaled_brc_btree_idx'::regclass)) as size,
    pg_relation_size('scaled_brc_btree_idx'::regclass) as bytes,
    round(pg_relation_size('scaled_brc_btree_idx'::regclass) / 20000000.0, 2) as bytes_per_record,
    round(pg_relation_size('scaled_brc_btree_idx'::regclass) / 8192.0, 0) as pages
FROM pg_indexes WHERE indexname = 'scaled_brc_btree_idx';

\echo ''
\echo '=== STEP 2: SMOL INDEX CREATION ==='

-- Create SMOL index
\echo 'Creating SMOL index on 20M records...'
CREATE INDEX scaled_brc_smol_idx ON scaled_brc USING smol (x, y);

-- Multiple ways to check SMOL index size
\echo 'SMOL Index Size Analysis (multiple measurement methods):'

-- Method 1: pg_relation_size
SELECT 
    'Method 1: pg_relation_size' as measurement_method,
    pg_size_pretty(pg_relation_size('scaled_brc_smol_idx'::regclass)) as size,
    pg_relation_size('scaled_brc_smol_idx'::regclass) as bytes,
    round(pg_relation_size('scaled_brc_smol_idx'::regclass) / 20000000.0, 4) as bytes_per_record
WHERE pg_relation_size('scaled_brc_smol_idx'::regclass) > 0;

-- Method 2: pg_stat_user_indexes
SELECT 
    'Method 2: pg_stat_user_indexes' as measurement_method,
    schemaname, indexrelname,
    pg_size_pretty(pg_relation_size(indexrelid)) as size,
    pg_relation_size(indexrelid) as bytes
FROM pg_stat_user_indexes 
WHERE indexrelname = 'scaled_brc_smol_idx';

-- Method 3: Direct file system check via pg_ls_dir if possible
SELECT 
    'Method 3: pg_indexes catalog' as measurement_method,
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as size,
    pg_relation_size(indexname::regclass) as bytes
FROM pg_indexes 
WHERE indexname = 'scaled_brc_smol_idx';

\echo ''
\echo '=== STEP 3: DETAILED SIZE COMPARISON ==='

WITH size_comparison AS (
    SELECT 
        'BTREE' as index_type,
        pg_relation_size('scaled_brc_btree_idx'::regclass) as bytes
    UNION ALL
    SELECT 
        'SMOL' as index_type,
        pg_relation_size('scaled_brc_smol_idx'::regclass) as bytes
)
SELECT 
    btree.bytes / (1024*1024) as btree_mb,
    smol.bytes / (1024*1024) as smol_mb,
    CASE WHEN smol.bytes > 0 THEN
        (btree.bytes - smol.bytes) / (1024*1024)
    ELSE 
        NULL 
    END as mb_saved,
    CASE WHEN smol.bytes > 0 THEN
        round(((btree.bytes - smol.bytes)::numeric / btree.bytes * 100), 2)
    ELSE 
        NULL
    END as percent_saved,
    btree.bytes / 20000000.0 as btree_bytes_per_record,
    CASE WHEN smol.bytes > 0 THEN 
        smol.bytes / 20000000.0 
    ELSE 
        NULL 
    END as smol_bytes_per_record
FROM 
    (SELECT bytes FROM size_comparison WHERE index_type = 'BTREE') btree,
    (SELECT bytes FROM size_comparison WHERE index_type = 'SMOL') smol;

\echo ''
\echo '=== STEP 4: PERFORMANCE BENCHMARK ==='

-- Test query selectivity first
SELECT 
    'Query Selectivity Analysis' as analysis,
    count(*) as total_records,
    count(*) FILTER (WHERE y > 5000) as matching_records,
    round(count(*) FILTER (WHERE y > 5000) * 100.0 / count(*), 2) as percent_matching
FROM scaled_brc;

SET enable_seqscan = off;
SET enable_bitmapscan = off;

\echo ''
\echo 'BTREE Performance Test:'
\timing on
EXPLAIN (ANALYZE, BUFFERS)
SELECT sum(x::bigint) FROM scaled_brc WHERE y > 5000;
\timing off

\echo ''
\echo 'BTREE Actual Query Execution:'
\timing on  
SELECT 'BTREE Result' as test, sum(x::bigint) FROM scaled_brc WHERE y > 5000;
\timing off

-- Switch to SMOL only
DROP INDEX scaled_brc_btree_idx;

\echo ''
\echo 'SMOL Performance Test:'
\timing on
EXPLAIN (ANALYZE, BUFFERS) 
SELECT sum(x::bigint) FROM scaled_brc WHERE y > 5000;
\timing off

\echo ''
\echo 'SMOL Actual Query Execution:'
\timing on
SELECT 'SMOL Result' as test, sum(x::bigint) FROM scaled_brc WHERE y > 5000;
\timing off

\echo ''
\echo '=== STEP 5: SCALING ANALYSIS ==='

-- Recreate BTREE for comparison
CREATE INDEX scaled_brc_btree_idx ON scaled_brc (x, y);

\echo 'Scaling factors to real-world scenarios:'

WITH scaling_factors AS (
    SELECT 
        'Current Test' as scenario,
        20 as million_records,
        pg_relation_size('scaled_brc_btree_idx'::regclass) / (1024*1024) as btree_mb,
        CASE WHEN pg_relation_size('scaled_brc_smol_idx'::regclass) > 0 THEN
            pg_relation_size('scaled_brc_smol_idx'::regclass) / (1024*1024)
        ELSE 
            0
        END as smol_mb
    UNION ALL
    SELECT 
        '100M Records (User Scale)',
        100,
        pg_relation_size('scaled_brc_btree_idx'::regclass) / (1024*1024) * 5 as btree_mb,
        CASE WHEN pg_relation_size('scaled_brc_smol_idx'::regclass) > 0 THEN
            pg_relation_size('scaled_brc_smol_idx'::regclass) / (1024*1024) * 5
        ELSE 
            0
        END as smol_mb
    UNION ALL
    SELECT 
        '1B Records (BRC)',
        1000,
        pg_relation_size('scaled_brc_btree_idx'::regclass) / (1024*1024) * 50 as btree_mb,
        CASE WHEN pg_relation_size('scaled_brc_smol_idx'::regclass) > 0 THEN
            pg_relation_size('scaled_brc_smol_idx'::regclass) / (1024*1024) * 50
        ELSE 
            0
        END as smol_mb
)
SELECT 
    scenario,
    million_records || 'M' as scale,
    round(btree_mb, 0) || ' MB' as btree_size,
    CASE WHEN smol_mb > 0 THEN 
        round(smol_mb, 0) || ' MB'
    ELSE 
        'Unknown (measurement failed)'
    END as smol_size,
    CASE WHEN smol_mb > 0 THEN
        round(btree_mb - smol_mb, 0) || ' MB'
    ELSE 
        'Cannot calculate'
    END as savings
FROM scaling_factors;

\echo ''
\echo '=== DIAGNOSTIC INFORMATION ==='

-- Check if SMOL index actually exists and has data
SELECT 
    'Index Existence Check' as check_type,
    count(*) as index_count
FROM pg_indexes 
WHERE indexname = 'scaled_brc_smol_idx';

-- Check index pages allocated
SELECT 
    'Page Allocation Check' as check_type,
    relpages as pages_allocated,
    reltuples as tuples_estimated
FROM pg_class 
WHERE relname = 'scaled_brc_smol_idx';

\echo ''
\echo '=== CONCLUSIONS ==='
\echo ''
\echo 'Key Measurements (20M records):'
\echo '1. Actual index sizes (not theoretical)'
\echo '2. Real query performance differences'
\echo '3. Scaling projections based on measurements'
\echo ''
\echo 'If SMOL measurements show 0 or very small values:'
\echo '• SMOL may have indexing/storage implementation issues'
\echo '• Size calculation methods may not work for SMOL'
\echo '• Need to investigate SMOL internals'
\echo ''
\echo 'Performance conclusions will depend on actual size measurements'

-- Cleanup
DROP TABLE scaled_brc CASCADE;

\echo ''
\echo 'Scaled BRC benchmark completed with 20M records!'
