-- Test copy function edge cases and build callback paths
-- Targets lines 4137, 4149, 4153, 4159, 4163, 4168

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- Test 1: char/bool (1-byte byval) types
-- Targets lines 4149 (byval len1=1) and 4159 (byval len2=1)
-- ============================================================================

DROP TABLE IF EXISTS t_char_bool CASCADE;
CREATE UNLOGGED TABLE t_char_bool (k "char", v bool);

INSERT INTO t_char_bool
SELECT
    (i % 128)::"char",
    (i % 2 = 0)::bool
FROM generate_series(1, 10000) i;

CREATE INDEX t_char_bool_smol ON t_char_bool USING smol(k, v);

SELECT k, v FROM t_char_bool WHERE k = '5'::"char" AND v = true LIMIT 5;
SELECT count(*) FROM t_char_bool WHERE k >= '0'::"char";

-- ============================================================================
-- Test 2: Single bool column
-- ============================================================================

DROP TABLE IF EXISTS t_bool_single CASCADE;
CREATE UNLOGGED TABLE t_bool_single (k bool, v int4);

INSERT INTO t_bool_single
SELECT
    (i % 2 = 0)::bool,
    i
FROM generate_series(1, 5000) i;

CREATE INDEX t_bool_single_smol ON t_bool_single USING smol(k, v);

SELECT k, count(*) FROM t_bool_single GROUP BY k ORDER BY k;

-- ============================================================================
-- Test 3: Linear growth path for pair builder (line 4137)
-- Need to trigger growth beyond 8M entries threshold
-- Use GUC to lower threshold for testing
-- ============================================================================

-- Lower growth threshold to trigger linear growth path
SET smol.growth_threshold_test = 1000;

DROP TABLE IF EXISTS t_large_growth CASCADE;
CREATE UNLOGGED TABLE t_large_growth (k int8, v int8);

-- Insert enough rows to trigger multiple doublings and then linear growth
-- With threshold=1000, we'll go: 0->1024, 1024->2048, 2048->4096 (linear)
INSERT INTO t_large_growth
SELECT i::int8, (i * 2)::int8
FROM generate_series(1, 5000) i;

CREATE INDEX t_large_growth_smol ON t_large_growth USING smol(k, v);

SELECT count(*) FROM t_large_growth WHERE k >= 2500;

-- Reset threshold
SET smol.growth_threshold_test = 0;

-- ============================================================================
-- Test 4: Progress logging (line 4168)
-- ============================================================================

SET smol.debug_log = on;
SET smol.progress_log_every = 1000;
SET client_min_messages = log;

DROP TABLE IF EXISTS t_progress CASCADE;
CREATE UNLOGGED TABLE t_progress (k int8, v int4);

INSERT INTO t_progress
SELECT i::int8, i::int4
FROM generate_series(1, 5000) i;

-- This should trigger progress logging during build
CREATE INDEX t_progress_smol ON t_progress USING smol(k, v);

SET client_min_messages = warning;
SET smol.debug_log = off;

-- ============================================================================
-- Test 5: Mixed 1-byte types
-- ============================================================================

DROP TABLE IF EXISTS t_mixed_byte CASCADE;
CREATE UNLOGGED TABLE t_mixed_byte (k "char", v "char");

INSERT INTO t_mixed_byte
SELECT
    ((i % 26) + 65)::"char",  -- A-Z
    ((i % 10) + 48)::"char"   -- 0-9
FROM generate_series(1, 2000) i;

CREATE INDEX t_mixed_byte_smol ON t_mixed_byte USING smol(k, v);

SELECT k, v FROM t_mixed_byte WHERE k >= 'M'::"char" ORDER BY k, v LIMIT 10;

-- Cleanup
DROP TABLE t_char_bool CASCADE;
DROP TABLE t_bool_single CASCADE;
DROP TABLE t_large_growth CASCADE;
DROP TABLE t_progress CASCADE;
DROP TABLE t_mixed_byte CASCADE;
