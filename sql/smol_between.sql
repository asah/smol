SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test BETWEEN queries (upper bound coverage)
-- This exercises the have_upper_bound code paths that aren't covered by existing tests


-- Test 1: Single-column BETWEEN on int4
DROP TABLE IF EXISTS t_between CASCADE;
CREATE UNLOGGED TABLE t_between (a int4);
INSERT INTO t_between SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX idx_between_smol ON t_between USING smol(a);

SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Forward scan with BETWEEN (lower and upper bounds)
SELECT count(*) FROM t_between WHERE a BETWEEN 100 AND 200;
SELECT count(*) FROM t_between WHERE a >= 100 AND a < 200;
SELECT count(*) FROM t_between WHERE a > 100 AND a <= 200;
SELECT count(*) FROM t_between WHERE a > 100 AND a < 200;

-- Backward scan with BETWEEN
SELECT count(*) FROM (SELECT a FROM t_between WHERE a BETWEEN 100 AND 200 ORDER BY a DESC) s;

-- Test 2: Two-column index with upper bound
DROP TABLE IF EXISTS t_between2 CASCADE;
CREATE UNLOGGED TABLE t_between2 (a int4, b int4);
INSERT INTO t_between2 SELECT i % 100, i FROM generate_series(1, 5000) i;
CREATE INDEX idx_between2_smol ON t_between2 USING smol(a, b);

-- Two-column with upper bound
SELECT count(*) FROM t_between2 WHERE a BETWEEN 10 AND 20;
SELECT count(*) FROM t_between2 WHERE a >= 10 AND a < 20;

-- Test 3: Upper bound only (< without >=)
SELECT count(*) FROM t_between WHERE a < 500;
SELECT count(*) FROM t_between WHERE a <= 500;

-- Test 4: Text columns with BETWEEN
DROP TABLE IF EXISTS t_text_between CASCADE;
CREATE UNLOGGED TABLE t_text_between (s text COLLATE "C");
INSERT INTO t_text_between SELECT 'key_' || lpad(i::text, 6, '0') FROM generate_series(1, 1000) i;
CREATE INDEX idx_text_between ON t_text_between USING smol(s);

SELECT count(*) FROM t_text_between WHERE s BETWEEN 'key_000100' AND 'key_000200';
SELECT count(*) FROM t_text_between WHERE s >= 'key_000100' AND s < 'key_000200';

-- Test 5: Backward scan with upper bound only
DROP TABLE IF EXISTS t_back_upper CASCADE;
CREATE UNLOGGED TABLE t_back_upper (a int4);
INSERT INTO t_back_upper SELECT i FROM generate_series(1, 500) i;
CREATE INDEX idx_back_upper ON t_back_upper USING smol(a);

-- Backward scan starting from upper bound
SELECT count(*) FROM (SELECT a FROM t_back_upper WHERE a <= 400 ORDER BY a DESC) s;
SELECT count(*) FROM (SELECT a FROM t_back_upper WHERE a < 400 ORDER BY a DESC) s;

-- Test 6: INCLUDE columns with BETWEEN
DROP TABLE IF EXISTS t_inc_between CASCADE;
CREATE UNLOGGED TABLE t_inc_between (a int4, b int4, c int4);
INSERT INTO t_inc_between SELECT i, i*2, i*3 FROM generate_series(1, 1000) i;
CREATE INDEX idx_inc_between ON t_inc_between USING smol(a) INCLUDE (b, c);

SELECT sum(b), sum(c) FROM t_inc_between WHERE a BETWEEN 100 AND 200;

-- Test 7: Edge cases - bounds at extremes
SELECT count(*) FROM t_between WHERE a BETWEEN 1 AND 10;
SELECT count(*) FROM t_between WHERE a BETWEEN 990 AND 1000;
SELECT count(*) FROM t_between WHERE a BETWEEN 1 AND 1000;

-- Test 8: Empty result BETWEEN
SELECT count(*) FROM t_between WHERE a BETWEEN 2000 AND 3000;

-- Cleanup
DROP TABLE t_between CASCADE;
DROP TABLE t_between2 CASCADE;
DROP TABLE t_text_between CASCADE;
DROP TABLE t_back_upper CASCADE;
DROP TABLE t_inc_between CASCADE;
