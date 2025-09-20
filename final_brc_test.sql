-- Final BRC Test with Fixed SMOL Implementation
\echo '=== FINAL BRC BENCHMARK: FIXED SMOL vs BTREE ==='
\echo ''

CREATE TABLE brc_final (
    x SMALLINT,
    y SMALLINT
);

-- Insert 1M records for comprehensive testing (scaled appropriately for container)
\echo 'Loading 1M records for BRC test...'

INSERT INTO brc_final
SELECT 
    (random() * 32767)::smallint as x,
    (random() * 32767)::smallint as y
FROM generate_series(1, 1000000);

ANALYZE brc_final;

\echo 'Data loaded. Table stats:'

SELECT 
    count(*) as records,
    pg_size_pretty(pg_relation_size('brc_final'::regclass)) as table_size,
    round(pg_relation_size('brc_final'::regclass) / count(*)::numeric, 2) as bytes_per_record
FROM brc_final;

\echo ''
\echo '=== INDEX CREATION AND SIZE MEASUREMENT ==='

-- Create BTREE index
CREATE INDEX brc_final_btree_idx ON brc_final (x, y);
VACUUM ANALYZE brc_final;

\echo 'BTREE index created. Size:'
SELECT 
    'BTREE' as index_type,
    pg_size_pretty(pg_relation_size('brc_final_btree_idx'::regclass)) as size,
    pg_relation_size('brc_final_btree_idx'::regclass) as bytes,
    round(pg_relation_size('brc_final_btree_idx'::regclass) / 1000000.0, 2) as bytes_per_record
FROM pg_indexes WHERE indexname = 'brc_final_btree_idx';

-- Create SMOL index
CREATE INDEX brc_final_smol_idx ON brc_final USING smol (x, y);

\echo 'SMOL index created. Size:'
SELECT 
    'SMOL' as index_type,
    pg_size_pretty(pg_relation_size('brc_final_smol_idx'::regclass)) as size,
    pg_relation_size('brc_final_smol_idx'::regclass) as bytes,
    round(pg_relation_size('brc_final_smol_idx'::regclass) / 1000000.0, 2) as bytes_per_record
FROM pg_indexes WHERE indexname = 'brc_final_smol_idx';

\echo ''
\echo '=== SPACE EFFICIENCY COMPARISON ==='

WITH sizes AS (
    SELECT 
        'BTREE' as type,
        pg_relation_size('brc_final_btree_idx'::regclass) as bytes
    UNION ALL
    SELECT 
        'SMOL' as type,
        pg_relation_size('brc_final_smol_idx'::regclass) as bytes
)
SELECT 
    btree.bytes / (1024*1024) as btree_mb,
    smol.bytes / (1024*1024) as smol_mb,
    (btree.bytes - smol.bytes) / (1024*1024) as mb_saved,
    round(((btree.bytes - smol.bytes)::numeric / btree.bytes * 100), 2) as percent_saved,
    round(btree.bytes / 1000000.0, 2) as btree_bytes_per_record,
    round(smol.bytes / 1000000.0, 2) as smol_bytes_per_record
FROM 
    (SELECT bytes FROM sizes WHERE type = 'BTREE') btree,
    (SELECT bytes FROM sizes WHERE type = 'SMOL') smol;

\echo ''
\echo '=== PERFORMANCE BENCHMARK: BRC AGGREGATION QUERY ==='

-- Test the actual BRC query: sum aggregation with filter
SELECT 
    'Query Selectivity' as test,
    count(*) as total_records,
    count(*) FILTER (WHERE y > 5000) as matching_records,
    round(count(*) FILTER (WHERE y > 5000) * 100.0 / count(*), 1) as percent_matching
FROM brc_final;

SET enable_seqscan = off;

\echo ''
\echo 'BTREE Performance (SELECT sum(x) FROM brc_final WHERE y > 5000):'

\timing on
EXPLAIN (ANALYZE, BUFFERS)
SELECT sum(x::bigint) FROM brc_final WHERE y > 5000;
\timing off

\echo ''
\echo 'BTREE Actual Query:'
\timing on
SELECT 'BTREE Result' as test_type, sum(x::bigint) as result 
FROM brc_final WHERE y > 5000;
\timing off

-- Switch to SMOL
DROP INDEX brc_final_btree_idx;

\echo ''
\echo 'SMOL Performance (SELECT sum(x) FROM brc_final WHERE y > 5000):'

\timing on
EXPLAIN (ANALYZE, BUFFERS)
SELECT sum(x::bigint) FROM brc_final WHERE y > 5000;  
\timing off

\echo ''
\echo 'SMOL Actual Query:'
\timing on
SELECT 'SMOL Result' as test_type, sum(x::bigint) as result
FROM brc_final WHERE y > 5000;
\timing off

\echo ''
\echo '=== SCALING TO 100M RECORDS PROJECTION ==='

-- Recreate BTREE for final comparison
CREATE INDEX brc_final_btree_idx ON brc_final (x, y);

WITH scaling AS (
    SELECT 
        'Scaling Analysis' as analysis,
        pg_relation_size('brc_final_btree_idx'::regclass) * 100 / (1024*1024) as btree_100m_mb,
        pg_relation_size('brc_final_smol_idx'::regclass) * 100 / (1024*1024) as smol_100m_mb
)
SELECT 
    analysis,
    '2123 MB (actual from user)' as user_btree_100m,
    round(btree_100m_mb, 0) || ' MB' as projected_btree_100m,
    round(smol_100m_mb, 0) || ' MB' as projected_smol_100m,
    round(btree_100m_mb - smol_100m_mb, 0) || ' MB' as projected_savings
FROM scaling;

\echo ''
\echo '=== FINAL CONCLUSIONS ==='
\echo ''
\echo 'BRC Aggregation Query Test Results:'
\echo '• Data: 1M records of 2x SMALLINT (4 bytes each)'
\echo '• Query: SELECT sum(x) WHERE y>5000 (85% selectivity)'
\echo '• Comparison: Fixed SMOL vs Well-Maintained BTREE'
\echo ''
\echo 'Key Findings:'
\echo '1. Actual space savings (SMOL vs BTREE)'
\echo '2. Query performance for large aggregations'  
\echo '3. Scaling projections to 100M records'
\echo ''
\echo 'Expected advantages for SMOL:'
\echo '• Smaller index = better cache utilization'
\echo '• Fewer pages to scan for aggregations'
\echo '• Zero maintenance overhead'

-- Cleanup
DROP TABLE brc_final CASCADE;

\echo ''
\echo 'Final BRC benchmark with fixed SMOL completed!'
