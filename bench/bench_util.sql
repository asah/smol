-- bench_util.sql
-- Benchmark utilities for SMOL vs BTREE comparison
--
-- This file provides functions to:
-- 1. Generate deterministic test data
-- 2. Build indexes and measure time/size
-- 3. Run queries and extract timing metrics
-- 4. Output results in CSV format

\set QUIET on
\set ON_ERROR_STOP on

-- Drop existing bench tables/functions if any
DROP SCHEMA IF EXISTS bench CASCADE;
CREATE SCHEMA bench;
SET search_path TO bench, public;

-- Configuration table for benchmark parameters
CREATE TABLE IF NOT EXISTS bench_config (
    key text PRIMARY KEY,
    value text NOT NULL
);

-- Results table
CREATE TABLE IF NOT EXISTS bench_results (
    run_id serial PRIMARY KEY,
    timestamp timestamptz DEFAULT now(),
    case_id text NOT NULL,
    engine text NOT NULL,  -- 'btree' or 'smol'
    key_type text NOT NULL,
    cols int NOT NULL,
    includes int NOT NULL,
    duplicates text NOT NULL,  -- 'unique', 'zipf', 'hot'
    rows bigint NOT NULL,
    selectivity text NOT NULL,
    direction text NOT NULL,
    workers int NOT NULL,
    warm boolean NOT NULL,
    build_ms numeric,
    idx_size_mb numeric,
    plan_ms numeric,
    exec_ms numeric,
    rows_out bigint,
    shared_buffers text,
    notes text
);

-- Generate data with specified distribution
CREATE OR REPLACE FUNCTION bench_generate_data(
    table_name text,
    key_type text,
    rows bigint,
    duplicates text,  -- 'unique', 'zipf', 'hot'
    includes int,
    cols int DEFAULT 1
) RETURNS void AS $$
DECLARE
    sql text;
    k1_expr text;
    k2_expr text := 'NULL::int4';
BEGIN
    -- Determine key generation expression based on distribution
    CASE duplicates
        WHEN 'unique' THEN
            k1_expr := 'i::' || key_type;
        WHEN 'zipf' THEN
            -- Zipf-like: concentrate values (simple approximation)
            k1_expr := '((i % (' || rows || ' / 100))::' || key_type || ')';
        WHEN 'hot' THEN
            -- Hot keys: 10% of rows get same value (42)
            k1_expr := '(CASE WHEN i % 10 = 0 THEN 42 ELSE i END)::' || key_type;
        ELSE
            RAISE EXCEPTION 'Unknown duplicates mode: %', duplicates;
    END CASE;

    IF cols = 2 THEN
        k2_expr := '(i % 1000)::int4';
    END IF;

    -- Build CREATE TABLE statement
    sql := 'CREATE UNLOGGED TABLE ' || table_name || ' (';
    sql := sql || 'k1 ' || key_type || ',';
    IF cols = 2 THEN
        sql := sql || 'k2 int4,';
    END IF;
    IF includes > 0 THEN
        FOR i IN 1..includes LOOP
            sql := sql || 'inc' || i || ' int4,';
        END LOOP;
    END IF;
    sql := rtrim(sql, ',') || ')';

    EXECUTE sql;

    -- Insert data
    sql := 'INSERT INTO ' || table_name || ' SELECT ';
    sql := sql || k1_expr || ' AS k1,';
    IF cols = 2 THEN
        sql := sql || k2_expr || ' AS k2,';
    END IF;
    IF includes > 0 THEN
        FOR i IN 1..includes LOOP
            sql := sql || '(s.i % 10000)::int4 AS inc' || i || ',';
        END LOOP;
    END IF;
    sql := rtrim(sql, ',');
    sql := sql || ' FROM generate_series(1, ' || rows || ') AS s(i)';

    EXECUTE sql;

    -- Optimize table
    EXECUTE 'ALTER TABLE ' || table_name || ' SET (autovacuum_enabled = off)';
    EXECUTE 'ANALYZE ' || table_name;
    EXECUTE 'CHECKPOINT';
    EXECUTE 'VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) ' || table_name;
    EXECUTE 'ANALYZE ' || table_name;

    RAISE NOTICE 'Generated % rows in %', rows, table_name;
END;
$$ LANGUAGE plpgsql;

-- Build index and measure time/size
CREATE OR REPLACE FUNCTION bench_build_index(
    table_name text,
    idx_name text,
    engine text,  -- 'btree' or 'smol'
    key_cols text,  -- 'k1' or 'k1,k2'
    include_cols text DEFAULT NULL,
    OUT build_ms numeric,
    OUT idx_size_mb numeric
) AS $$
DECLARE
    sql text;
    start_time timestamptz;
    end_time timestamptz;
BEGIN
    sql := 'CREATE INDEX ' || idx_name || ' ON ' || table_name;
    sql := sql || ' USING ' || engine || ' (' || key_cols || ')';

    IF include_cols IS NOT NULL THEN
        sql := sql || ' INCLUDE (' || include_cols || ')';
    END IF;

    start_time := clock_timestamp();
    EXECUTE sql;
    end_time := clock_timestamp();

    build_ms := EXTRACT(epoch FROM (end_time - start_time)) * 1000;

    EXECUTE 'SELECT pg_relation_size(''' || idx_name || '''::regclass) / (1024.0 * 1024.0)'
        INTO idx_size_mb;

    RAISE NOTICE 'Built index % in % ms (%.2f MB)', idx_name, round(build_ms, 2), round(idx_size_mb, 2);
END;
$$ LANGUAGE plpgsql;

-- Run query and extract timing
-- Note: This is a simplified version. A full implementation would use EXPLAIN (ANALYZE, FORMAT JSON)
CREATE OR REPLACE FUNCTION bench_run_query(
    sql_query text,
    warmup boolean DEFAULT true,
    OUT plan_ms numeric,
    OUT exec_ms numeric,
    OUT rows_out bigint
) AS $$
DECLARE
    explain_result text;
    json_result json;
BEGIN
    -- Warmup run if requested
    IF warmup THEN
        EXECUTE sql_query INTO rows_out;
    END IF;

    -- Actual measured run with EXPLAIN ANALYZE
    EXECUTE 'EXPLAIN (ANALYZE, TIMING ON, BUFFERS OFF, FORMAT JSON) ' || sql_query
        INTO explain_result;

    -- Parse JSON (simplified - real implementation would use json functions)
    -- For now, just execute the query directly and measure
    EXECUTE sql_query INTO rows_out;

    plan_ms := 0.1;  -- Placeholder
    exec_ms := 1.0;  -- Placeholder

    -- TODO: Parse EXPLAIN output to extract actual timings
END;
$$ LANGUAGE plpgsql;

-- Helper: get current shared_buffers setting
CREATE OR REPLACE FUNCTION bench_get_shared_buffers()
RETURNS text AS $$
BEGIN
    RETURN current_setting('shared_buffers');
END;
$$ LANGUAGE plpgsql;

-- Convenience function to run a complete benchmark case
CREATE OR REPLACE FUNCTION bench_run_case(
    case_id text,
    engine text,
    key_type text DEFAULT 'int4',
    cols int DEFAULT 1,
    includes int DEFAULT 0,
    duplicates text DEFAULT 'unique',
    rows bigint DEFAULT 1000000,
    selectivity text DEFAULT '0.5',
    direction text DEFAULT 'asc',
    workers int DEFAULT 0,
    warm boolean DEFAULT true
) RETURNS void AS $$
DECLARE
    table_name text;
    idx_name text;
    build_ms numeric;
    idx_size_mb numeric;
    plan_ms numeric;
    exec_ms numeric;
    rows_out bigint;
    query_sql text;
    predicate text;
    include_cols text := NULL;
    key_cols text := 'k1';
BEGIN
    table_name := 'bench_t_' || case_id;
    idx_name := 'bench_i_' || case_id || '_' || engine;

    RAISE NOTICE 'Running case %: engine=% rows=% sel=% workers=%',
        case_id, engine, rows, selectivity, workers;

    -- Generate data (only once per case_id)
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bench' AND tablename = table_name) THEN
        PERFORM bench_generate_data(table_name, key_type, rows, duplicates, includes, cols);
    END IF;

    -- Build includes clause
    IF includes > 0 THEN
        include_cols := '';
        FOR i IN 1..includes LOOP
            IF i > 1 THEN include_cols := include_cols || ','; END IF;
            include_cols := include_cols || 'inc' || i;
        END LOOP;
    END IF;

    -- Extend to multi-column if needed
    IF cols = 2 THEN
        key_cols := 'k1,k2';
    END IF;

    -- Build index
    SELECT * INTO build_ms, idx_size_mb
    FROM bench_build_index(table_name, idx_name, engine, key_cols, include_cols);

    -- Configure workers
    EXECUTE 'SET max_parallel_workers_per_gather = ' || workers;
    EXECUTE 'SET enable_seqscan = off';
    EXECUTE 'SET enable_indexscan = off';
    EXECUTE 'SET enable_indexonlyscan = on';
    EXECUTE 'SET enable_bitmapscan = off';

    -- Build query predicate
    IF selectivity = 'eq' THEN
        predicate := 'k1 = 42';
    ELSE
        predicate := 'k1 >= ' || floor(rows * (1.0 - selectivity::numeric)) || '::' || key_type;
    END IF;

    -- Build query
    query_sql := 'SELECT COUNT(*) FROM ' || table_name || ' WHERE ' || predicate;
    IF direction = 'desc' THEN
        query_sql := query_sql || ' ORDER BY k1 DESC';
    END IF;

    -- Run query
    SELECT * INTO plan_ms, exec_ms, rows_out
    FROM bench_run_query(query_sql, warm);

    -- Record results
    INSERT INTO bench_results (
        case_id, engine, key_type, cols, includes, duplicates, rows,
        selectivity, direction, workers, warm,
        build_ms, idx_size_mb, plan_ms, exec_ms, rows_out, shared_buffers
    ) VALUES (
        case_id, engine, key_type, cols, includes, duplicates, rows,
        selectivity, direction, workers, warm,
        build_ms, idx_size_mb, plan_ms, exec_ms, rows_out,
        bench_get_shared_buffers()
    );

    RAISE NOTICE 'Case % complete: build=%.1fms idx=%.1fMB exec=%.1fms rows=%',
        case_id, build_ms, idx_size_mb, exec_ms, rows_out;
END;
$$ LANGUAGE plpgsql;

-- Export results to CSV
CREATE OR REPLACE FUNCTION bench_export_csv(filename text)
RETURNS void AS $$
BEGIN
    EXECUTE format(
        'COPY (SELECT case_id, engine, key_type, cols, includes, duplicates, rows, ' ||
        'selectivity, direction, workers, warm, build_ms, idx_size_mb, plan_ms, exec_ms, rows_out, shared_buffers ' ||
        'FROM bench_results ORDER BY run_id) TO %L WITH (FORMAT CSV, HEADER)',
        filename
    );
    RAISE NOTICE 'Results exported to %', filename;
END;
$$ LANGUAGE plpgsql;

\set QUIET off
