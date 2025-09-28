-- More micro-cases to exercise scan code paths
SET client_min_messages = warning;
\pset format unaligned
\pset tuples_only on
\pset footer off
\pset pager off
CREATE EXTENSION IF NOT EXISTS smol;

-- Case 1: single-key backward equality run
DROP TABLE IF EXISTS bkeq CASCADE;
CREATE UNLOGGED TABLE bkeq(a int2);
INSERT INTO bkeq SELECT 5::int2 FROM generate_series(1,20);
CREATE INDEX bkeq_a_smol ON bkeq USING smol(a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;
-- Expect three 5s
COPY (SELECT a FROM bkeq WHERE a=5 ORDER BY a DESC LIMIT 3) TO STDOUT;

-- Case 2: two-column scan with equality on second key (k2)
DROP TABLE IF EXISTS k2eq CASCADE;
CREATE UNLOGGED TABLE k2eq(a int2, b int2);
-- Build rows with b in [1..10], make a=42 when b>=6 to satisfy (b>5 AND a=42)
INSERT INTO k2eq SELECT CASE WHEN i>=6 THEN 42 ELSE i END::int2, i::int2 FROM generate_series(1,10) i;
CREATE INDEX k2eq_idx ON k2eq USING smol(b,a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;
-- Expect 5 rows (b=6..10, a=42)
COPY (SELECT count(*) FROM k2eq WHERE b>5 AND a=42) TO STDOUT;

-- Case 3: prefetch depth equivalence (0 vs 4)
DROP TABLE IF EXISTS pfd CASCADE;
CREATE UNLOGGED TABLE pfd(a int2);
INSERT INTO pfd SELECT (i%100)::int2 FROM generate_series(1,5000) i;
CREATE INDEX pfd_a_smol ON pfd USING smol(a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;
-- Run twice to ensure stable results
COPY (SELECT sum(a)::bigint FROM pfd WHERE a>=50) TO STDOUT;
COPY (SELECT sum(a)::bigint FROM pfd WHERE a>=50) TO STDOUT;
