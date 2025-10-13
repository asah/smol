-- Edge cases and error validation for SMOL
-- Tests uncovered error paths and boundary conditions
SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test 1: Invalid multi-column index (>2 columns)
DROP TABLE IF EXISTS t_3col CASCADE;
CREATE TABLE t_3col(a int, b int, c int);
INSERT INTO t_3col VALUES (1, 2, 3);

-- Should fail: 3+ columns not supported
\set ON_ERROR_STOP 0
CREATE INDEX t_3col_smol ON t_3col USING smol(a, b, c);
\set ON_ERROR_STOP 1

-- Test 2: Two-column + INCLUDE (now supported)
DROP TABLE IF EXISTS t_2col_inc CASCADE;
CREATE TABLE t_2col_inc(k1 int, k2 int, inc1 int);
INSERT INTO t_2col_inc VALUES (1, 2, 3);

-- This should now succeed
CREATE INDEX t_2col_inc_smol ON t_2col_inc USING smol(k1, k2) INCLUDE (inc1);
-- Verify it was created
SELECT COUNT(*) > 0 AS index_exists FROM pg_indexes WHERE indexname = 't_2col_inc_smol';

-- Test 3: Too many INCLUDE columns (>16)
DROP TABLE IF EXISTS t_many_inc CASCADE;
CREATE TABLE t_many_inc(
    k int,
    i1 int, i2 int, i3 int, i4 int,
    i5 int, i6 int, i7 int, i8 int,
    i9 int, i10 int, i11 int, i12 int,
    i13 int, i14 int, i15 int, i16 int,
    i17 int
);
INSERT INTO t_many_inc SELECT 1, 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1;

-- Should fail: >16 INCLUDE columns
\set ON_ERROR_STOP 0
CREATE INDEX t_many_inc_smol ON t_many_inc USING smol(k)
    INCLUDE (i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,i16,i17);
\set ON_ERROR_STOP 1

-- Test 4: Unsupported key type (variable length non-text)
DROP TABLE IF EXISTS t_varlen CASCADE;
CREATE TABLE t_varlen(k bytea);
INSERT INTO t_varlen VALUES ('\\x01020304'::bytea);

-- Should fail: bytea is variable-length and not text
\set ON_ERROR_STOP 0
CREATE INDEX t_varlen_smol ON t_varlen USING smol(k);
\set ON_ERROR_STOP 1

-- Test 5: char (1-byte) key type (supported, should work)
DROP TABLE IF EXISTS t_char CASCADE;
CREATE UNLOGGED TABLE t_char(k "char");  -- internal 1-byte type
INSERT INTO t_char SELECT (i % 127)::int::text::"char" FROM generate_series(1, 100) i;

CREATE INDEX t_char_smol ON t_char USING smol(k);

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET enable_indexscan = off;

-- Verify char type works
SELECT count(*) AS char_count FROM t_char WHERE k >= '5'::"char";

DROP INDEX t_char_smol;
DROP TABLE t_char;

-- Test 6: Backward scan (DESC ORDER BY)
DROP TABLE IF EXISTS t_backward CASCADE;
CREATE UNLOGGED TABLE t_backward(k int4, v int4);
INSERT INTO t_backward SELECT i, i*10 FROM generate_series(1, 1000) i;

CREATE INDEX t_backward_smol ON t_backward USING smol(k) INCLUDE (v);

-- Force backward scan with DESC
SELECT k FROM t_backward WHERE k >= 990 ORDER BY k DESC LIMIT 5;

-- Backward with INCLUDE
SELECT v FROM t_backward WHERE k >= 995 ORDER BY k DESC LIMIT 3;

DROP INDEX t_backward_smol;
DROP TABLE t_backward;

-- Test 7: Two-column with int8 second key
DROP TABLE IF EXISTS t_twocol_int8 CASCADE;
CREATE UNLOGGED TABLE t_twocol_int8(k1 int4, k2 int8);
INSERT INTO t_twocol_int8 SELECT (i % 100)::int4, i::int8 FROM generate_series(1, 1000) i;

CREATE INDEX t_twocol_int8_smol ON t_twocol_int8 USING smol(k1, k2);

-- Query with int8 second key equality
SELECT count(*) AS twocol_int8_count FROM t_twocol_int8 WHERE k1 >= 50 AND k2 = 555::int8;

DROP INDEX t_twocol_int8_smol;
DROP TABLE t_twocol_int8;

-- Test 8: Parallel scan with multiple workers
DROP TABLE IF EXISTS t_parallel CASCADE;
CREATE UNLOGGED TABLE t_parallel(k int4, v int4);
INSERT INTO t_parallel SELECT i, i*2 FROM generate_series(1, 100000) i;
ANALYZE t_parallel;

CREATE INDEX t_parallel_smol ON t_parallel USING smol(k) INCLUDE (v);

-- Enable parallel execution
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET min_parallel_index_scan_size = 0;

-- Force parallel index-only scan
SELECT count(*), sum(v)::bigint FROM t_parallel WHERE k >= 10000;

-- Reset parallel settings
SET max_parallel_workers_per_gather = 0;

DROP INDEX t_parallel_smol;
DROP TABLE t_parallel;

-- Cleanup
DROP TABLE IF EXISTS t_3col CASCADE;
DROP TABLE IF EXISTS t_2col_inc CASCADE;
DROP TABLE IF EXISTS t_many_inc CASCADE;
DROP TABLE IF EXISTS t_varlen CASCADE;
