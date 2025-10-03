-- Test int2 (smallint) index to cover byval int2 code paths
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

DROP TABLE IF EXISTS t_int2 CASCADE;
CREATE TABLE t_int2 (k int2, v text);

-- Insert diverse int2 values including negative, zero, positive, and edge cases
INSERT INTO t_int2 SELECT 
    (i - 500)::int2,  -- Range from -500 to 499
    'value_' || i::text
FROM generate_series(1, 1000) i;

-- Create SMOL index on int2 column
CREATE INDEX t_int2_smol ON t_int2 USING smol(k);

-- Test queries
SELECT count(*) FROM t_int2 WHERE k < -400::int2;
SELECT count(*) FROM t_int2 WHERE k >= 0::int2;
SELECT count(*) FROM t_int2 WHERE k BETWEEN -100::int2 AND 100::int2;

-- Test backward scan with int2
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT k FROM t_int2 WHERE k > 400::int2 ORDER BY k DESC LIMIT 5;

-- Cleanup
DROP TABLE t_int2 CASCADE;
