-- Test RLE run limit >32000 (lines 3152, 3363)
-- A "run" in Include-RLE is a sequence of identical (key + all includes)
-- To hit the >32000 run limit, we need >32000 DIFFERENT (key,include) combinations
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

DROP TABLE IF EXISTS t_rle_limit CASCADE;
CREATE TABLE t_rle_limit (k int4, v int4);

-- Insert 35000 rows with SAME key (1) but DIFFERENT include values
-- This creates 35000 runs of length 1 each (since each row is unique)
-- When building the index, the Include-RLE algorithm will try to pack these
-- and hit the nr >= 32000 limit at line 3149/3360
INSERT INTO t_rle_limit
SELECT 1, i FROM generate_series(1, 35000) i;

CREATE INDEX t_rle_limit_idx ON t_rle_limit USING smol(k) INCLUDE (v);

-- Verify
SELECT count(*), count(DISTINCT v) FROM t_rle_limit;
SELECT count(*) FROM t_rle_limit WHERE k = 1 AND v > 30000;

DROP TABLE t_rle_limit CASCADE;

-- Test 2: Create data where runs are small but numerous
-- Pattern: 1,1, 2,2, 3,3, ... (2 of each value = many small runs)
DROP TABLE IF EXISTS t_many_runs CASCADE;
CREATE TABLE t_many_runs (k int4, v text);

INSERT INTO t_many_runs
SELECT i, 'val' || i FROM generate_series(1, 20000) i, generate_series(1, 2);

CREATE INDEX t_many_runs_idx ON t_many_runs USING smol(k) INCLUDE (v);

SELECT count(*) FROM t_many_runs WHERE k BETWEEN 10000 AND 15000;

DROP TABLE t_many_runs CASCADE;
