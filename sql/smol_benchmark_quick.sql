SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Quick benchmark suite comparing SMOL vs BTREE for common real-world scenarios
-- This test provides performance assertions while maintaining deterministic output
--
-- Performance benchmarks are optional and disabled by default (can be flaky in CI).
-- To enable: SMOL_ENABLE_PERF_TESTS=1 make installcheck REGRESS=smol_benchmark_quick
SET client_min_messages = warning;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET max_parallel_workers_per_gather = 0;

-- ============================================================================
-- Scenario 1: Lookup Table with UUIDs (User Profiles, Product Catalog)
-- ============================================================================
DROP TABLE IF EXISTS lookup_uuid CASCADE;
CREATE UNLOGGED TABLE lookup_uuid(
    id uuid,
    name text COLLATE "C",
    status text COLLATE "C"
);
INSERT INTO lookup_uuid
SELECT
    gen_random_uuid(),
    'user_' || lpad(i::text, 6, '0'),
    CASE WHEN i % 10 = 0 THEN 'inactive' ELSE 'active' END
FROM generate_series(1, 100000) i;
ALTER TABLE lookup_uuid SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) lookup_uuid;

-- BTREE baseline
CREATE INDEX lookup_uuid_btree ON lookup_uuid USING btree(id) INCLUDE (name, status);
CREATE TEMP TABLE lookup_uuid_btree_size AS
    SELECT pg_relation_size('lookup_uuid_btree') AS size_bytes;

-- SMOL index
DROP INDEX lookup_uuid_btree;
CREATE INDEX lookup_uuid_smol ON lookup_uuid USING smol(id) INCLUDE (name, status);
CREATE TEMP TABLE lookup_uuid_smol_size AS
    SELECT pg_relation_size('lookup_uuid_smol') AS size_bytes;

-- Assert: SMOL is smaller (no per-tuple overhead)
SELECT
    (SELECT size_bytes FROM lookup_uuid_btree_size)::float /
    (SELECT size_bytes FROM lookup_uuid_smol_size) > 1.2
AS lookup_uuid_compressed;

-- Correctness: Full scan count
SELECT count(*) FROM lookup_uuid;

-- ============================================================================
-- Scenario 2: Log Summary Table (Time-Series Analytics, Daily Metrics)
-- ============================================================================
DROP TABLE IF EXISTS log_summary CASCADE;
CREATE UNLOGGED TABLE log_summary(
    log_date date,
    user_count int4,
    total_revenue int8,
    avg_latency float8
);
-- 5 years of daily data with heavy duplicates (multiple entries per day)
INSERT INTO log_summary
SELECT
    ('2020-01-01'::date + (i % 1826)),  -- 1826 days = 5 years
    (random() * 10000)::int4,
    (random() * 100000)::int8,
    (random() * 1000)::float8
FROM generate_series(1, 500000) i;
ALTER TABLE log_summary SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) log_summary;

-- BTREE baseline
CREATE INDEX log_summary_btree ON log_summary USING btree(log_date)
    INCLUDE (user_count, total_revenue, avg_latency);
CREATE TEMP TABLE log_summary_btree_size AS
    SELECT pg_relation_size('log_summary_btree') AS size_bytes;

-- SMOL index
DROP INDEX log_summary_btree;
CREATE INDEX log_summary_smol ON log_summary USING smol(log_date)
    INCLUDE (user_count, total_revenue, avg_latency);
CREATE TEMP TABLE log_summary_smol_size AS
    SELECT pg_relation_size('log_summary_smol') AS size_bytes;

-- Assert: SMOL achieves strong compression on duplicate dates (RLE + dup-caching)
SELECT
    (SELECT size_bytes FROM log_summary_btree_size)::float /
    (SELECT size_bytes FROM log_summary_smol_size) > 1.5
AS log_summary_compressed;

-- Correctness: Range query for analytics
SET enable_indexscan = on;  -- Allow BTREE indexscan
CREATE TEMP TABLE log_summary_btree_baseline AS
SELECT count(*)::bigint AS days
FROM log_summary
WHERE log_date >= '2023-01-01';

DROP INDEX log_summary_smol;
CREATE INDEX log_summary_btree_verify ON log_summary USING btree(log_date)
    INCLUDE (user_count, total_revenue, avg_latency);
CREATE TEMP TABLE log_summary_btree_result AS
SELECT count(*)::bigint AS days
FROM log_summary
WHERE log_date >= '2023-01-01';

SET enable_indexscan = off;  -- Back to IOS for SMOL
DROP INDEX log_summary_btree_verify;
CREATE INDEX log_summary_smol_verify ON log_summary USING smol(log_date)
    INCLUDE (user_count, total_revenue, avg_latency);

SELECT
    (SELECT days FROM log_summary_btree_baseline) =
    (SELECT days FROM log_summary_btree_result)
AS log_summary_correctness;

-- ============================================================================
-- Scenario 3: Many-to-Many Join Table (User-Groups, Tags, Permissions)
-- ============================================================================
DROP TABLE IF EXISTS many_to_many CASCADE;
CREATE UNLOGGED TABLE many_to_many(
    entity_id int4,
    relation_id int4,
    created_at int8
);
-- Zipfian distribution: popular entities have many relations
INSERT INTO many_to_many
SELECT
    CASE
        WHEN i % 100 < 10 THEN (i % 10)        -- 10% hot entities (10 distinct)
        WHEN i % 100 < 50 THEN (i % 1000)      -- 40% warm entities (1K distinct)
        ELSE (i % 100000)                       -- 50% cold entities (100K distinct)
    END,
    (i % 50000)::int4,
    1704067200::int8 + (i % 365) * 86400::int8  -- Unix timestamp starting 2024-01-01
FROM generate_series(1, 500000) i;
ALTER TABLE many_to_many SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) many_to_many;

-- BTREE baseline (composite index without INCLUDE - SMOL doesn't support INCLUDE on two-col)
CREATE INDEX many_to_many_btree ON many_to_many USING btree(entity_id, relation_id);
CREATE TEMP TABLE many_to_many_btree_size AS
    SELECT pg_relation_size('many_to_many_btree') AS size_bytes;

-- SMOL index (two-column without INCLUDE)
DROP INDEX many_to_many_btree;
CREATE INDEX many_to_many_smol ON many_to_many USING smol(entity_id, relation_id);
CREATE TEMP TABLE many_to_many_smol_size AS
    SELECT pg_relation_size('many_to_many_smol') AS size_bytes;

-- Assert: SMOL's columnar layout + RLE compression advantage
SELECT
    (SELECT size_bytes FROM many_to_many_btree_size)::float /
    (SELECT size_bytes FROM many_to_many_smol_size) > 1.3
AS many_to_many_compressed;

-- Correctness: Hot entity lookup (two-column index)
SELECT count(*) FROM many_to_many WHERE entity_id = 5 AND relation_id >= 0;

-- ============================================================================
-- Scenario 4: High-Cardinality Analytics (int8 IDs, Log Aggregation)
-- ============================================================================
DROP TABLE IF EXISTS high_cardinality CASCADE;
CREATE UNLOGGED TABLE high_cardinality(
    trace_id int8,
    span_count int4,
    duration_ms float8
);
-- Monotonically increasing trace IDs (realistic for distributed tracing)
INSERT INTO high_cardinality
SELECT
    1000000000000::int8 + i::int8,
    (random() * 100)::int4,
    (random() * 5000)::float8
FROM generate_series(1, 200000) i;
ALTER TABLE high_cardinality SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) high_cardinality;

-- BTREE baseline
CREATE INDEX high_cardinality_btree ON high_cardinality USING btree(trace_id)
    INCLUDE (span_count, duration_ms);
CREATE TEMP TABLE high_cardinality_btree_size AS
    SELECT pg_relation_size('high_cardinality_btree') AS size_bytes;

-- SMOL index
DROP INDEX high_cardinality_btree;
CREATE INDEX high_cardinality_smol ON high_cardinality USING smol(trace_id)
    INCLUDE (span_count, duration_ms);
CREATE TEMP TABLE high_cardinality_smol_size AS
    SELECT pg_relation_size('high_cardinality_smol') AS size_bytes;

-- Assert: Even with unique keys, SMOL is smaller (zero per-tuple overhead)
SELECT
    (SELECT size_bytes FROM high_cardinality_btree_size)::float /
    (SELECT size_bytes FROM high_cardinality_smol_size) > 1.1
AS high_cardinality_compressed;

-- Correctness: Range query
SELECT count(*) FROM high_cardinality WHERE trace_id >= 1000000000000::int8 + 100000;

-- ============================================================================
-- Scenario 5: String Keys (Product SKUs, Category Codes)
-- ============================================================================
DROP TABLE IF EXISTS string_keys CASCADE;
CREATE UNLOGGED TABLE string_keys(
    sku text COLLATE "C",
    price int8,
    quantity int4
);
-- SKU format: "PROD-XXXXXX" with duplicates for popular items
INSERT INTO string_keys
SELECT
    'PROD-' || lpad((i % 50000)::text, 6, '0'),
    (random() * 100000)::int8,
    (random() * 100)::int4
FROM generate_series(1, 300000) i;
ALTER TABLE string_keys SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) string_keys;

-- BTREE baseline
CREATE INDEX string_keys_btree ON string_keys USING btree(sku)
    INCLUDE (price, quantity);
CREATE TEMP TABLE string_keys_btree_size AS
    SELECT pg_relation_size('string_keys_btree') AS size_bytes;

-- SMOL index
DROP INDEX string_keys_btree;
CREATE INDEX string_keys_smol ON string_keys USING smol(sku)
    INCLUDE (price, quantity);
CREATE TEMP TABLE string_keys_smol_size AS
    SELECT pg_relation_size('string_keys_smol') AS size_bytes;

-- Assert: SMOL compression on duplicate SKUs (lower threshold for text keys)
SELECT
    (SELECT size_bytes FROM string_keys_btree_size)::float /
    (SELECT size_bytes FROM string_keys_smol_size) > 1.1
AS string_keys_compressed;

-- Correctness: Prefix range query
SELECT count(*) FROM string_keys WHERE sku >= 'PROD-040000';

-- ============================================================================
-- Scenario 6: RLE Test - 0% Zero-Copy (Dense Sentinel Clustering)
-- ============================================================================
DROP TABLE IF EXISTS rle_full CASCADE;
CREATE UNLOGGED TABLE rle_full(
    event_date date,
    user_id int4,
    event_type text COLLATE "C"
);
-- Create data with heavy clustering: 95% epoch dates (NULL equivalent), sorted
INSERT INTO rle_full
SELECT
    CASE WHEN i % 100 < 95 THEN '1970-01-01'::date ELSE '2024-01-01'::date + (i % 365) END,
    (i % 1000)::int4,
    CASE WHEN i % 100 < 95 THEN 'unknown' ELSE 'event_' || (i % 10)::text END
FROM generate_series(1, 200000) i
ORDER BY 1, 2;  -- Sort to maximize clustering
ALTER TABLE rle_full SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) rle_full;

-- BTREE baseline
CREATE INDEX rle_full_btree ON rle_full USING btree(event_date) INCLUDE (user_id, event_type);
CREATE TEMP TABLE rle_full_btree_size AS
    SELECT pg_relation_size('rle_full_btree') AS size_bytes;

-- SMOL index
DROP INDEX rle_full_btree;
CREATE INDEX rle_full_smol ON rle_full USING smol(event_date) INCLUDE (user_id, event_type);
CREATE TEMP TABLE rle_full_smol_size AS
    SELECT pg_relation_size('rle_full_smol') AS size_bytes;

-- Verify RLE compression triggered (should be low zero-copy, high RLE)
SELECT
    'rle_full' AS scenario,
    zerocopy_pct,
    compression_pct,
    zerocopy_pct < 10 AS rle_triggered,
    compression_pct > 85 AS high_rle
FROM smol_inspect('rle_full_smol');

-- ============================================================================
-- Scenario 7: RLE Test - Small % Zero-Copy (Mostly Clustered)
-- ============================================================================
DROP TABLE IF EXISTS rle_mostly CASCADE;
CREATE UNLOGGED TABLE rle_mostly(
    price_cents int4,
    product_id int4,
    quantity int4
);
-- 90% sentinel values ($0.00 price = free items), tightly clustered by sorting
INSERT INTO rle_mostly
SELECT
    CASE WHEN i % 100 < 90 THEN 0 ELSE (i % 50) * 100 END,  -- 90% $0.00
    CASE WHEN i % 100 < 90 THEN -1 ELSE i % 5000 END,       -- 90% unknown product
    CASE WHEN i % 100 < 90 THEN 0 ELSE i % 100 END          -- 90% zero quantity
FROM generate_series(1, 200000) i
ORDER BY 1, 2, 3;  -- Sort to maximize per-page clustering
ALTER TABLE rle_mostly SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) rle_mostly;

-- BTREE baseline
CREATE INDEX rle_mostly_btree ON rle_mostly USING btree(price_cents) INCLUDE (product_id, quantity);
CREATE TEMP TABLE rle_mostly_btree_size AS
    SELECT pg_relation_size('rle_mostly_btree') AS size_bytes;

-- SMOL index
DROP INDEX rle_mostly_btree;
CREATE INDEX rle_mostly_smol ON rle_mostly USING smol(price_cents) INCLUDE (product_id, quantity);
CREATE TEMP TABLE rle_mostly_smol_size AS
    SELECT pg_relation_size('rle_mostly_smol') AS size_bytes;

-- Verify mostly RLE with some zero-copy pages
SELECT
    'rle_mostly' AS scenario,
    zerocopy_pct,
    compression_pct,
    zerocopy_pct < 30 AS mostly_rle,
    compression_pct > 65 AS high_compression
FROM smol_inspect('rle_mostly_smol');

-- ============================================================================
-- Scenario 8: RLE Test - Moderate Zero-Copy (Partial Clustering)
-- ============================================================================
DROP TABLE IF EXISTS rle_mixed CASCADE;
CREATE UNLOGGED TABLE rle_mixed(
    error_code int4,
    timestamp_val int8,
    user_id int4
);
-- Strategy: 70% sentinel values (densely packed) + 30% diverse (spread across pages)
-- This creates a mix where some pages are RLE and some are zero-copy
INSERT INTO rle_mixed
SELECT
    CASE WHEN i % 100 < 70 THEN 0 ELSE 200 + (i % 100) END,  -- 70% error code 0 (success)
    CASE WHEN i % 100 < 70 THEN 0::int8 ELSE 1704067200::int8 + (i % 10000)::int8 END,  -- 70% epoch
    CASE WHEN i % 100 < 70 THEN -1 ELSE i % 50000 END  -- 70% unknown user
FROM generate_series(1, 200000) i
ORDER BY 1, 2, 3;  -- Sort to create page-level clustering
ALTER TABLE rle_mixed SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) rle_mixed;

-- BTREE baseline
CREATE INDEX rle_mixed_btree ON rle_mixed USING btree(error_code) INCLUDE (timestamp_val, user_id);
CREATE TEMP TABLE rle_mixed_btree_size AS
    SELECT pg_relation_size('rle_mixed_btree') AS size_bytes;

-- SMOL index
DROP INDEX rle_mixed_btree;
CREATE INDEX rle_mixed_smol ON rle_mixed USING smol(error_code) INCLUDE (timestamp_val, user_id);
CREATE TEMP TABLE rle_mixed_smol_size AS
    SELECT pg_relation_size('rle_mixed_smol') AS size_bytes;

-- Verify moderate RLE usage (should show some zero-copy, mostly RLE)
SELECT
    'rle_mixed' AS scenario,
    zerocopy_pct,
    compression_pct,
    zerocopy_pct BETWEEN 0 AND 25 AS moderate_zerocopy,
    compression_pct > 70 AS high_rle
FROM smol_inspect('rle_mixed_smol');

-- ============================================================================
-- Table 1: Compression & Size Summary (All Scenarios)
-- ============================================================================
SELECT
    'lookup_uuid' AS scenario,
    (SELECT size_bytes FROM lookup_uuid_btree_size) AS btree_bytes,
    (SELECT size_bytes FROM lookup_uuid_smol_size) AS smol_bytes,
    ((SELECT size_bytes FROM lookup_uuid_btree_size)::float /
     (SELECT size_bytes FROM lookup_uuid_smol_size))::numeric(5,2) AS compression_ratio,
    (SELECT zerocopy_pct FROM smol_inspect('lookup_uuid_smol'))::numeric(5,1) AS zerocopy_pct
UNION ALL
SELECT
    'log_summary',
    (SELECT size_bytes FROM log_summary_btree_size),
    (SELECT size_bytes FROM log_summary_smol_size),
    ((SELECT size_bytes FROM log_summary_btree_size)::float /
     (SELECT size_bytes FROM log_summary_smol_size))::numeric(5,2),
    (SELECT zerocopy_pct FROM smol_inspect('log_summary_smol_verify'))::numeric(5,1)
UNION ALL
SELECT
    'many_to_many',
    (SELECT size_bytes FROM many_to_many_btree_size),
    (SELECT size_bytes FROM many_to_many_smol_size),
    ((SELECT size_bytes FROM many_to_many_btree_size)::float /
     (SELECT size_bytes FROM many_to_many_smol_size))::numeric(5,2),
    (SELECT zerocopy_pct FROM smol_inspect('many_to_many_smol'))::numeric(5,1)
UNION ALL
SELECT
    'high_cardinality',
    (SELECT size_bytes FROM high_cardinality_btree_size),
    (SELECT size_bytes FROM high_cardinality_smol_size),
    ((SELECT size_bytes FROM high_cardinality_btree_size)::float /
     (SELECT size_bytes FROM high_cardinality_smol_size))::numeric(5,2),
    (SELECT zerocopy_pct FROM smol_inspect('high_cardinality_smol'))::numeric(5,1)
UNION ALL
SELECT
    'string_keys',
    (SELECT size_bytes FROM string_keys_btree_size),
    (SELECT size_bytes FROM string_keys_smol_size),
    ((SELECT size_bytes FROM string_keys_btree_size)::float /
     (SELECT size_bytes FROM string_keys_smol_size))::numeric(5,2),
    (SELECT zerocopy_pct FROM smol_inspect('string_keys_smol'))::numeric(5,1)
UNION ALL
SELECT
    'rle_full',
    (SELECT size_bytes FROM rle_full_btree_size),
    (SELECT size_bytes FROM rle_full_smol_size),
    ((SELECT size_bytes FROM rle_full_btree_size)::float /
     (SELECT size_bytes FROM rle_full_smol_size))::numeric(5,2),
    (SELECT zerocopy_pct FROM smol_inspect('rle_full_smol'))::numeric(5,1)
UNION ALL
SELECT
    'rle_mostly',
    (SELECT size_bytes FROM rle_mostly_btree_size),
    (SELECT size_bytes FROM rle_mostly_smol_size),
    ((SELECT size_bytes FROM rle_mostly_btree_size)::float /
     (SELECT size_bytes FROM rle_mostly_smol_size))::numeric(5,2),
    (SELECT zerocopy_pct FROM smol_inspect('rle_mostly_smol'))::numeric(5,1)
UNION ALL
SELECT
    'rle_mixed',
    (SELECT size_bytes FROM rle_mixed_btree_size),
    (SELECT size_bytes FROM rle_mixed_smol_size),
    ((SELECT size_bytes FROM rle_mixed_btree_size)::float /
     (SELECT size_bytes FROM rle_mixed_smol_size))::numeric(5,2),
    (SELECT zerocopy_pct FROM smol_inspect('rle_mixed_smol'))::numeric(5,1)
ORDER BY scenario;

-- Assert: Zero-copy percentage is appropriate for each scenario
-- Note: These synthetic datasets use zero-copy+dup-caching rather than RLE because:
-- 1. Duplicates are well-distributed across pages (high per-page uniqueness)
-- 2. No sentinel values (NULL equivalents like epoch, -1, $0.00) that cluster
-- Real-world data with common sentinel values would show more RLE usage
SELECT
    (SELECT zerocopy_pct FROM smol_inspect('lookup_uuid_smol')) = 100 AS lookup_uuid_zerocopy,
    (SELECT zerocopy_pct FROM smol_inspect('high_cardinality_smol')) = 100 AS high_cardinality_zerocopy,
    (SELECT zerocopy_pct + compression_pct FROM smol_inspect('log_summary_smol_verify')) = 100 AS log_summary_covered,
    (SELECT zerocopy_pct + compression_pct FROM smol_inspect('many_to_many_smol')) = 100 AS many_to_many_covered,
    (SELECT zerocopy_pct + compression_pct FROM smol_inspect('string_keys_smol')) = 100 AS string_keys_covered;

-- ============================================================================
-- Table 2: Performance - Warm Cache (Speedup Ratio vs BTREE, 3 runs averaged)
-- ============================================================================
-- Skip performance tests unless SMOL_ENABLE_PERF_TESTS=1
\if :{?SMOL_ENABLE_PERF_TESTS}

-- Create BTREE indexes for performance testing
CREATE INDEX lookup_uuid_btree_perf ON lookup_uuid USING btree(id) INCLUDE (name, status);
CREATE INDEX log_summary_btree_perf ON log_summary USING btree(log_date) INCLUDE (user_count, total_revenue, avg_latency);
CREATE INDEX rle_full_btree_perf ON rle_full USING btree(event_date) INCLUDE (user_id, event_type);
CREATE INDEX rle_mostly_btree_perf ON rle_mostly USING btree(price_cents) INCLUDE (product_id, quantity);

-- Note: pg_prewarm extension not loaded, relying on natural cache warming from prior queries

-- Performance results table
CREATE TEMP TABLE perf_warm (
    scenario text,
    pattern text,
    btree_ms numeric,
    smol_ms numeric
);

-- Benchmark function: runs query N times and returns average time in ms
CREATE OR REPLACE FUNCTION pg_temp.bench(query text, runs int DEFAULT 3) RETURNS numeric AS $$
DECLARE
    start_ts timestamptz;
    total_ms numeric := 0;
    i int;
BEGIN
    FOR i IN 1..runs LOOP
        start_ts := clock_timestamp();
        EXECUTE query;
        total_ms := total_ms + EXTRACT(EPOCH FROM (clock_timestamp() - start_ts)) * 1000;
    END LOOP;
    RETURN total_ms / runs;
END;
$$ LANGUAGE plpgsql;

-- Test 1: lookup_uuid - full scan (skip point/range due to UUID min() not available)
DO $$
DECLARE
    btree_time numeric;
    smol_time numeric;
BEGIN
    btree_time := pg_temp.bench('SELECT count(*) FROM lookup_uuid');
    DROP INDEX lookup_uuid_btree_perf;
    smol_time := pg_temp.bench('SELECT count(*) FROM lookup_uuid');
    INSERT INTO perf_warm VALUES ('lookup_uuid', 'full_scan', btree_time, smol_time);
END;
$$;

-- Test 2: log_summary - point lookup
DO $$
DECLARE
    btree_time numeric;
    smol_time numeric;
BEGIN
    btree_time := pg_temp.bench('SELECT count(*) FROM log_summary WHERE log_date >= ''2023-01-01'' LIMIT 100');
    DROP INDEX log_summary_btree_perf;
    smol_time := pg_temp.bench('SELECT count(*) FROM log_summary WHERE log_date >= ''2023-01-01'' LIMIT 100');
    INSERT INTO perf_warm VALUES ('log_summary', 'point_lookup', btree_time, smol_time);
    CREATE INDEX log_summary_btree_perf ON log_summary USING btree(log_date) INCLUDE (user_count, total_revenue, avg_latency);
END;
$$;

-- Test 3: log_summary - range scan
DO $$
DECLARE
    btree_time numeric;
    smol_time numeric;
BEGIN
    btree_time := pg_temp.bench('SELECT count(*) FROM log_summary WHERE log_date >= ''2023-01-01'' LIMIT 5000');
    DROP INDEX log_summary_btree_perf;
    smol_time := pg_temp.bench('SELECT count(*) FROM log_summary WHERE log_date >= ''2023-01-01'' LIMIT 5000');
    INSERT INTO perf_warm VALUES ('log_summary', 'range_scan', btree_time, smol_time);
    CREATE INDEX log_summary_btree_perf ON log_summary USING btree(log_date) INCLUDE (user_count, total_revenue, avg_latency);
END;
$$;

-- Test 4: log_summary - full scan
DO $$
DECLARE
    btree_time numeric;
    smol_time numeric;
BEGIN
    btree_time := pg_temp.bench('SELECT count(*) FROM log_summary');
    DROP INDEX log_summary_btree_perf;
    smol_time := pg_temp.bench('SELECT count(*) FROM log_summary');
    INSERT INTO perf_warm VALUES ('log_summary', 'full_scan', btree_time, smol_time);
END;
$$;

-- Test 5: rle_full - range scan (RLE scenario)
DO $$
DECLARE
    btree_time numeric;
    smol_time numeric;
BEGIN
    btree_time := pg_temp.bench('SELECT count(*) FROM rle_full WHERE event_date >= ''1970-01-01'' LIMIT 5000');
    DROP INDEX rle_full_btree_perf;
    smol_time := pg_temp.bench('SELECT count(*) FROM rle_full WHERE event_date >= ''1970-01-01'' LIMIT 5000');
    INSERT INTO perf_warm VALUES ('rle_full', 'range_scan', btree_time, smol_time);
    CREATE INDEX rle_full_btree_perf ON rle_full USING btree(event_date) INCLUDE (user_id, event_type);
END;
$$;

-- Test 6: rle_full - full scan
DO $$
DECLARE
    btree_time numeric;
    smol_time numeric;
BEGIN
    btree_time := pg_temp.bench('SELECT count(*) FROM rle_full');
    DROP INDEX rle_full_btree_perf;
    smol_time := pg_temp.bench('SELECT count(*) FROM rle_full');
    INSERT INTO perf_warm VALUES ('rle_full', 'full_scan', btree_time, smol_time);
END;
$$;

-- Test 7: rle_mostly - range scan (RLE scenario)
DO $$
DECLARE
    btree_time numeric;
    smol_time numeric;
BEGIN
    btree_time := pg_temp.bench('SELECT count(*) FROM rle_mostly WHERE price_cents >= 0 LIMIT 5000');
    DROP INDEX rle_mostly_btree_perf;
    smol_time := pg_temp.bench('SELECT count(*) FROM rle_mostly WHERE price_cents >= 0 LIMIT 5000');
    INSERT INTO perf_warm VALUES ('rle_mostly', 'range_scan', btree_time, smol_time);
    CREATE INDEX rle_mostly_btree_perf ON rle_mostly USING btree(price_cents) INCLUDE (product_id, quantity);
END;
$$;

-- Test 8: rle_mostly - full scan
DO $$
DECLARE
    btree_time numeric;
    smol_time numeric;
BEGIN
    btree_time := pg_temp.bench('SELECT count(*) FROM rle_mostly');
    DROP INDEX rle_mostly_btree_perf;
    smol_time := pg_temp.bench('SELECT count(*) FROM rle_mostly');
    INSERT INTO perf_warm VALUES ('rle_mostly', 'full_scan', btree_time, smol_time);
END;
$$;

-- Show results as speedup ratios (SMOL speedup = btree_ms / smol_ms)
-- Note: Actual timing values vary between runs, so we only assert performance is reasonable
-- Assert: SMOL should be competitive (within 0.9x-2.0x range)
SELECT
    bool_and(
        (btree_ms / smol_ms) >= 0.9 AND (btree_ms / smol_ms) <= 2.0
    ) AS smol_performance_reasonable
FROM perf_warm;

\endif

-- Cleanup
DROP TABLE lookup_uuid CASCADE;
DROP TABLE log_summary CASCADE;
DROP TABLE many_to_many CASCADE;
DROP TABLE high_cardinality CASCADE;
DROP TABLE string_keys CASCADE;
DROP TABLE rle_full CASCADE;
DROP TABLE rle_mostly CASCADE;
DROP TABLE rle_mixed CASCADE;
