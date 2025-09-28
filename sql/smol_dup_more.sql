-- Extended duplicate-run and GUC coverage
SET client_min_messages = warning;
\pset format unaligned
\pset tuples_only on
\pset footer off
\pset pager off
CREATE EXTENSION IF NOT EXISTS smol;

-- Build a multi-leaf duplicate run for single-key + INCLUDE
DROP TABLE IF EXISTS m CASCADE;
CREATE UNLOGGED TABLE m(a int2, x int8);
INSERT INTO m
SELECT 7::int2, i::int8 FROM generate_series(1,20000) i;

CREATE INDEX m_a_smol ON m USING smol(a) INCLUDE (x);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;

-- Baseline
COPY (SELECT count(*) FROM m WHERE a=7) TO STDOUT;
COPY (SELECT sum(x)::bigint FROM m WHERE a=7) TO STDOUT;
COPY (SELECT a FROM m WHERE a=7 ORDER BY a DESC LIMIT 5) TO STDOUT;

-- Repeat and verify stability
COPY (SELECT count(*) FROM m WHERE a=7) TO STDOUT;
COPY (SELECT sum(x)::bigint FROM m WHERE a=7) TO STDOUT;
COPY (SELECT a FROM m WHERE a=7 ORDER BY a DESC LIMIT 5) TO STDOUT;

-- Third repeat
COPY (SELECT count(*) FROM m WHERE a=7) TO STDOUT;
COPY (SELECT sum(x)::bigint FROM m WHERE a=7) TO STDOUT;
COPY (SELECT a FROM m WHERE a=7 ORDER BY a DESC LIMIT 5) TO STDOUT;
