-- smol_null_errors.sql
-- Test that SMOL properly rejects NULL values at all entry points
-- SMOL does not support NULL - these tests verify early error detection
--
-- IMPORTANT: SMOL rejects NULL values during index build and in scan keys.
-- For IS NULL/IS NOT NULL queries, PostgreSQL may handle them as executor
-- filters rather than scan keys, so they return correct results (since the
-- index contains no NULLs by construction).

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- PART 1: NULL keys during index build (single-key indexes)
-- Entry point: ambuild -> build callbacks
-- Expected: ERROR at build time
-- ============================================================================

-- Test 1a: NULL in single-key INT4 index
DROP TABLE IF EXISTS t_null_build_int CASCADE;
CREATE UNLOGGED TABLE t_null_build_int (k int4, v int4);
INSERT INTO t_null_build_int VALUES (1, 10), (NULL, 20), (3, 30);

-- This should ERROR during index build
CREATE INDEX idx_null_build_int ON t_null_build_int USING smol(k);

DROP TABLE IF EXISTS t_null_build_int CASCADE;

-- Test 1b: NULL in single-key TEXT index
DROP TABLE IF EXISTS t_null_build_text CASCADE;
CREATE UNLOGGED TABLE t_null_build_text (k text COLLATE "C", v int4);
INSERT INTO t_null_build_text VALUES ('a', 10), (NULL, 20), ('c', 30);

-- This should ERROR during index build
CREATE INDEX idx_null_build_text ON t_null_build_text USING smol(k);

DROP TABLE IF EXISTS t_null_build_text CASCADE;

-- Test 1c: NULL in INT8 index
DROP TABLE IF EXISTS t_null_build_int8 CASCADE;
CREATE UNLOGGED TABLE t_null_build_int8 (k int8);
INSERT INTO t_null_build_int8 VALUES (1), (NULL), (3);

-- This should ERROR during index build
CREATE INDEX idx_null_build_int8 ON t_null_build_int8 USING smol(k);

DROP TABLE IF EXISTS t_null_build_int8 CASCADE;

-- ============================================================================
-- PART 2: NULL keys in two-column indexes
-- Entry point: ambuild -> smol_build_cb_pair
-- Expected: ERROR at build time
-- ============================================================================

-- Test 2a: NULL in first key column
DROP TABLE IF EXISTS t_null_twocol_k1 CASCADE;
CREATE UNLOGGED TABLE t_null_twocol_k1 (k1 int4, k2 int4);
INSERT INTO t_null_twocol_k1 VALUES (1, 1), (NULL, 2), (3, 3);

-- This should ERROR during index build
CREATE INDEX idx_null_twocol_k1 ON t_null_twocol_k1 USING smol(k1, k2);

DROP TABLE IF EXISTS t_null_twocol_k1 CASCADE;

-- Test 2b: NULL in second key column
DROP TABLE IF EXISTS t_null_twocol_k2 CASCADE;
CREATE UNLOGGED TABLE t_null_twocol_k2 (k1 int4, k2 int4);
INSERT INTO t_null_twocol_k2 VALUES (1, 1), (2, NULL), (3, 3);

-- This should ERROR during index build
CREATE INDEX idx_null_twocol_k2 ON t_null_twocol_k2 USING smol(k1, k2);

DROP TABLE IF EXISTS t_null_twocol_k2 CASCADE;

-- Test 2c: NULL in both key columns
DROP TABLE IF EXISTS t_null_twocol_both CASCADE;
CREATE UNLOGGED TABLE t_null_twocol_both (k1 int4, k2 int4);
INSERT INTO t_null_twocol_both VALUES (NULL, NULL);

-- This should ERROR during index build
CREATE INDEX idx_null_twocol_both ON t_null_twocol_both USING smol(k1, k2);

DROP TABLE IF EXISTS t_null_twocol_both CASCADE;

-- ============================================================================
-- PART 3: NULL in INCLUDE columns
-- Entry point: ambuild -> smol_build_cb_inc
-- Expected: ERROR at build time
-- ============================================================================

-- Test 3a: NULL in INCLUDE column (single INCLUDE)
DROP TABLE IF EXISTS t_null_include CASCADE;
CREATE UNLOGGED TABLE t_null_include (k int4, inc1 int4);
INSERT INTO t_null_include VALUES (1, 10), (2, NULL), (3, 30);

-- This should ERROR during index build
CREATE INDEX idx_null_include ON t_null_include USING smol(k) INCLUDE (inc1);

DROP TABLE IF EXISTS t_null_include CASCADE;

-- Test 3b: NULL in second INCLUDE column
DROP TABLE IF EXISTS t_null_include2 CASCADE;
CREATE UNLOGGED TABLE t_null_include2 (k int4, inc1 int4, inc2 text);
INSERT INTO t_null_include2 VALUES (1, 10, 'a'), (2, 20, NULL), (3, 30, 'c');

-- This should ERROR during index build
CREATE INDEX idx_null_include2 ON t_null_include2 USING smol(k) INCLUDE (inc1, inc2);

DROP TABLE IF EXISTS t_null_include2 CASCADE;

-- Test 3c: NULL in both key and INCLUDE
DROP TABLE IF EXISTS t_null_key_and_inc CASCADE;
CREATE UNLOGGED TABLE t_null_key_and_inc (k int4, inc1 int4);
INSERT INTO t_null_key_and_inc VALUES (NULL, NULL);

-- This should ERROR during index build (NULL key is checked first)
CREATE INDEX idx_null_key_and_inc ON t_null_key_and_inc USING smol(k) INCLUDE (inc1);

DROP TABLE IF EXISTS t_null_key_and_inc CASCADE;

-- ============================================================================
-- PART 4: Verify non-NULL data works correctly
-- Sanity check that our NULL checks don't break normal operation
-- ============================================================================

DROP TABLE IF EXISTS t_normal_operations CASCADE;
CREATE UNLOGGED TABLE t_normal_operations (k int4, inc text);
-- Insert only non-NULL values
INSERT INTO t_normal_operations SELECT i, 'value_' || i FROM generate_series(1, 100) i;

-- Index build should succeed
CREATE INDEX idx_normal ON t_normal_operations USING smol(k) INCLUDE (inc);

SET enable_seqscan = off;
SET enable_bitmapscan = off;

-- Normal queries should work
SELECT COUNT(*) FROM t_normal_operations WHERE k > 50;
SELECT COUNT(*) FROM t_normal_operations WHERE k = 42;
SELECT COUNT(*) FROM t_normal_operations WHERE k BETWEEN 10 AND 20;

-- IS NULL queries work but return 0 rows (no NULLs in index by construction)
-- Note: PostgreSQL may handle these as executor filters, not scan keys
SELECT COUNT(*) FROM t_normal_operations WHERE k IS NULL;

-- IS NOT NULL queries work and return all rows (all values are non-NULL)
SELECT COUNT(*) FROM t_normal_operations WHERE k IS NOT NULL;

DROP TABLE t_normal_operations CASCADE;

-- Reset
SET enable_seqscan = on;
SET enable_bitmapscan = on;

-- ============================================================================
-- Summary: SMOL NULL Handling Contract
-- ============================================================================
-- 1. NULL keys rejected at build time with clear error message
-- 2. NULL INCLUDE columns rejected at build time with clear error message
-- 3. Scan keys with SK_ISNULL/SK_SEARCHNULL/SK_SEARCHNOTNULL flags rejected
--    at scan time (if PostgreSQL generates them)
-- 4. IS NULL/IS NOT NULL in WHERE clauses may be handled as executor filters
--    by PostgreSQL, which work correctly since index contains no NULLs
-- ============================================================================
