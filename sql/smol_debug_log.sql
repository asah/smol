-- Test debug logging for text types (lines 1959-1970)
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Enable debug logging
SET smol.debug_log = on;
SET client_min_messages = debug1;

-- Create table with text key and text INCLUDE
DROP TABLE IF EXISTS t_debug CASCADE;
SET client_min_messages = warning;  -- Suppress OID messages
CREATE UNLOGGED TABLE t_debug(k text COLLATE "C", v text COLLATE "C", i int4);
SET client_min_messages = debug1;

INSERT INTO t_debug VALUES
    ('key001', 'value001', 1),
    ('key002', 'value002', 2),
    ('key003', 'value003', 3);

-- Create SMOL index with text INCLUDE columns
-- Temporarily disable debug logging to avoid non-deterministic OIDs/timing in output
SET smol.debug_log = off;
SET client_min_messages = warning;
CREATE INDEX t_debug_smol ON t_debug USING smol(k) INCLUDE (v, i);
ANALYZE t_debug;
-- Re-enable debug logging for the actual test queries
SET smol.debug_log = on;
SET client_min_messages = debug1;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = on;

-- Query to trigger debug logging of varlena sizes
-- The debug log will show varlena sizes and scan behavior, but
-- we've disabled logging during CREATE INDEX to avoid non-deterministic OIDs/timing
SELECT k, v, i FROM t_debug WHERE k >= 'key002';

-- Reset
SET smol.debug_log = off;
SET client_min_messages = warning;
DROP TABLE t_debug CASCADE;
