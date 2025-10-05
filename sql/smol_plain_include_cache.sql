-- Coverage test for plain-page INCLUDE caching optimization
-- This test creates a single-column index with INCLUDE columns on unique data,
-- which triggers plain-page layout (tag 0x8001) and exercises the caching code.

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Create table with unique keys (no duplicates = plain layout)
DROP TABLE IF EXISTS plain_inc CASCADE;
CREATE UNLOGGED TABLE plain_inc(k int4, inc1 int4, inc2 int4);

-- Insert unique values (no duplicates)
INSERT INTO plain_inc SELECT i::int4, (i*2)::int4, (i*3)::int4 
FROM generate_series(1, 1000) i;

ALTER TABLE plain_inc SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) plain_inc;

-- Create single-column SMOL index with INCLUDE columns
-- Unique keys will trigger plain-page layout (tag 0x8001)
CREATE INDEX plain_inc_smol ON plain_inc USING smol(k) INCLUDE (inc1, inc2);
ANALYZE plain_inc;

-- Query to exercise the plain-page INCLUDE caching code
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET enable_indexscan = off;

-- Query should use cached INCLUDE pointers
SELECT sum(inc1)::bigint, sum(inc2)::bigint, count(*)::bigint 
FROM plain_inc WHERE k >= 500;

-- Verify correctness
SELECT
    (SELECT sum(inc1)::bigint FROM plain_inc WHERE k >= 500) = 751500::bigint AS inc1_correct,
    (SELECT sum(inc2)::bigint FROM plain_inc WHERE k >= 500) = 1127250::bigint AS inc2_correct,
    (SELECT count(*)::bigint FROM plain_inc WHERE k >= 500) = 501::bigint AS count_correct;

DROP TABLE plain_inc CASCADE;
