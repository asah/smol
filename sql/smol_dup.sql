-- Duplicate-run coverage for single-key SMOL, with and without INCLUDE
SET client_min_messages = warning;
\pset format unaligned
\pset tuples_only on
\pset footer off
\pset pager off
CREATE EXTENSION IF NOT EXISTS smol;

DROP TABLE IF EXISTS d CASCADE;
CREATE UNLOGGED TABLE d(a int2, x int4);
-- Create 10 groups of 5 duplicates each: a in [1..10], each repeated 5 times
INSERT INTO d
SELECT g::int2, (g*100 + r)::int4
FROM generate_series(1,10) g, generate_series(1,5) r;

CREATE INDEX d_a_smol ON d USING smol(a) INCLUDE (x);

SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;


-- Forward run: expect 3 repeated 5, then 4 repeated
COPY (SELECT a FROM d WHERE a >= 3 ORDER BY a LIMIT 8) TO STDOUT;

-- Backward run (lower-bound supported): expect 10 repeated 5, then 9 repeated
COPY (SELECT a FROM d WHERE a >= 9 ORDER BY a DESC LIMIT 7) TO STDOUT;

-- INCLUDE path: force IOS to read include; sum for a=7 is 7*100*5 + sum(1..5) = 3515
COPY (SELECT sum(x)::bigint FROM d WHERE a = 7) TO STDOUT;

-- Repeat queries should produce identical outputs
COPY (SELECT a FROM d WHERE a >= 3 ORDER BY a LIMIT 8) TO STDOUT;
COPY (SELECT a FROM d WHERE a >= 9 ORDER BY a DESC LIMIT 7) TO STDOUT;
COPY (SELECT sum(x)::bigint FROM d WHERE a = 7) TO STDOUT;
\pset format unaligned
\pset tuples_only on
\pset footer off
\pset pager off
