-- 100M Record Billion Row Challenge Benchmark
-- Comparing SMOL vs BTREE for large-scale aggregation query
\echo '=== 100M RECORD BRC BENCHMARK: SMOL vs BTREE ==='
\echo ''

\timing on

-- Create table with 100M records (2x SMALLINT columns)
\echo 'Creating table with 100M records...'
CREATE TABLE tmp_brc_100m (
    x INT2,  -- SMALLINT (2 bytes)
    y INT2   -- SMALLINT (2 bytes) 
);

-- Insert 100M records
\echo 'Inserting 100M records (this will take several minutes)...'
INSERT INTO tmp_brc_100m 
SELECT 
    (random() * 10000)::int2 as x,
    (random() * 10000)::int2 as y
FROM generate_series(1, 100000000);

\echo ''
\echo 'Table created. Analyzing storage:'
SELECT 
    'Table size' as metric,
    pg_size_pretty(pg_relation_size('tmp_brc_100m'::regclass)) as size,
    count(*) as records,
    round(pg_relation_size('tmp_brc_100m'::regclass) / count(*)::numeric, 2) as bytes_per_record
FROM tmp_brc_100m;

-- Create BTREE index
\echo ''
\echo '=== Phase 1: BTREE Index Creation ==='
CREATE INDEX tmp_brc_btree_idx ON tmp_brc_100m USING btree(x, y);

-- Analyze for optimal statistics
ANALYZE tmp_brc_100m;
VACUUM tmp_brc_100m;

\echo 'BTREE index created. Size:'
SELECT 
    'BTREE Index' as type,
    pg_size_pretty(pg_relation_size('tmp_brc_btree_idx'::regclass)) as size,
    pg_relation_size('tmp_brc_btree_idx'::regclass) as bytes,
    round(pg_relation_size('tmp_brc_btree_idx'::regclass) / 100000000.0, 2) as bytes_per_record
FROM pg_class WHERE relname = 'tmp_brc_btree_idx';

-- BRC Query: Sum aggregation with 85% selectivity 
\echo ''
\echo '=== Phase 2: BTREE Performance Test ==='
\echo 'Query: SELECT sum(x) FROM tmp_brc_100m WHERE y > 1500 (85% selectivity)'

-- Ensure index-only scan
SET enable_seqscan = off;
SET enable_bitmapscan = off;

EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(x) FROM tmp_brc_100m WHERE y > 1500;

-- Get actual result for verification
SELECT 'BTREE Result' as method, sum(x) as total_sum, count(*) as matching_records
FROM tmp_brc_100m WHERE y > 1500;

-- Drop BTREE and create SMOL
\echo ''
\echo '=== Phase 3: SMOL Index Creation ==='
DROP INDEX tmp_brc_btree_idx;

CREATE INDEX tmp_brc_smol_idx ON tmp_brc_100m USING smol(x, y);

\echo 'SMOL index created. Size:'
SELECT 
    'SMOL Index' as type,
    pg_size_pretty(pg_relation_size('tmp_brc_smol_idx'::regclass)) as size,
    pg_relation_size('tmp_brc_smol_idx'::regclass) as bytes,
    round(pg_relation_size('tmp_brc_smol_idx'::regclass) / 100000000.0, 2) as bytes_per_record
FROM pg_class WHERE relname = 'tmp_brc_smol_idx';

-- SMOL Performance Test
\echo ''
\echo '=== Phase 4: SMOL Performance Test ==='
\echo 'Query: SELECT sum(x) FROM tmp_brc_100m WHERE y > 1500 (85% selectivity)'

EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(x) FROM tmp_brc_100m WHERE y > 1500;

-- Get actual result for verification
SELECT 'SMOL Result' as method, sum(x) as total_sum, count(*) as matching_records
FROM tmp_brc_100m WHERE y > 1500;

-- Storage comparison
\echo ''
\echo '=== Phase 5: Storage Efficiency Analysis ==='

-- Recreate BTREE for comparison
CREATE INDEX tmp_brc_btree_idx ON tmp_brc_100m USING btree(x, y);
VACUUM tmp_brc_100m;

WITH index_sizes AS (
    SELECT 
        CASE WHEN indexname LIKE '%btree%' THEN 'BTREE'
             ELSE 'SMOL' END as type,
        pg_relation_size(indexname::regclass) as bytes,
        pg_size_pretty(pg_relation_size(indexname::regclass)) as size
    FROM pg_indexes 
    WHERE tablename = 'tmp_brc_100m'
)
SELECT 
    'Storage Comparison' as metric,
    btree.size as btree_size,
    smol.size as smol_size,
    pg_size_pretty(btree.bytes - smol.bytes) as space_saved,
    round(((btree.bytes - smol.bytes)::numeric / btree.bytes * 100), 1) || '%' as percent_saved
FROM 
    (SELECT * FROM index_sizes WHERE type = 'BTREE') btree,
    (SELECT * FROM index_sizes WHERE type = 'SMOL') smol;

-- Per-record overhead analysis  
WITH index_sizes AS (
    SELECT 
        CASE WHEN indexname LIKE '%btree%' THEN 'BTREE'
             ELSE 'SMOL' END as type,
        pg_relation_size(indexname::regclass) as bytes
    FROM pg_indexes 
    WHERE tablename = 'tmp_brc_100m'
)
SELECT 
    'Per-Record Analysis' as metric,
    round(btree.bytes / 100000000.0, 2) as btree_bytes_per_record,
    round(smol.bytes / 100000000.0, 2) as smol_bytes_per_record,
    round((btree.bytes - smol.bytes) / 100000000.0, 2) as overhead_reduction
FROM 
    (SELECT bytes FROM index_sizes WHERE type = 'BTREE') btree,
    (SELECT bytes FROM index_sizes WHERE type = 'SMOL') smol;

\echo ''
\echo '=== Phase 6: Billion Record Projection ==='

WITH index_sizes AS (
    SELECT 
        CASE WHEN indexname LIKE '%btree%' THEN 'BTREE'
             ELSE 'SMOL' END as type,
        pg_relation_size(indexname::regclass) as bytes
    FROM pg_indexes 
    WHERE tablename = 'tmp_brc_100m'
),
projections AS (
    SELECT 
        btree.bytes * 10 as btree_1b_bytes,
        smol.bytes * 10 as smol_1b_bytes
    FROM 
        (SELECT bytes FROM index_sizes WHERE type = 'BTREE') btree,
        (SELECT bytes FROM index_sizes WHERE type = 'SMOL') smol
)
SELECT 
    'Billion Record Projection' as metric,
    pg_size_pretty(btree_1b_bytes) as btree_1b_size,
    pg_size_pretty(smol_1b_bytes) as smol_1b_size,
    pg_size_pretty(btree_1b_bytes - smol_1b_bytes) as projected_savings
FROM projections;

\echo ''
\echo '=== BENCHMARK COMPLETE ==='
\echo ''
\echo 'Key Results:'
\echo '1. Storage efficiency: SMOL vs BTREE space savings'
\echo '2. Query performance: Index-only scan comparison'  
\echo '3. Billion-record projections for the BRC'
\echo '4. Actual measured overhead per record'
\echo ''

-- Cleanup (commented out for result inspection)
-- DROP TABLE tmp_brc_100m CASCADE;
