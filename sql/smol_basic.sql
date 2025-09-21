-- basic regression for smol: ordered, read-only, index-only
CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE EXTENSION smol;
SET smol.debug_log = on;  -- enable verbose tracing during tests

-- base table without NULLs
CREATE TABLE t(a int, b int);
INSERT INTO t(a,b)
SELECT i, i*10 FROM generate_series(1,10) AS s(i);

-- build index (locks table via AM), then seal table from writes
CREATE INDEX t_a_smol ON t USING smol(a);
SELECT smol_seal_table('t'::regclass);

-- index-only scan: ensure results and order
SET enable_seqscan = off;
SELECT a FROM t WHERE a >= 7 ORDER BY a;

-- reject non-index-only scans
SET enable_indexonlyscan = off;  -- planner will try Index Scan
SET enable_indexscan = on;
SELECT a FROM t WHERE a >= 7;

-- verify NULLs unsupported at build time
CREATE TABLE t_null(a int);
INSERT INTO t_null VALUES (1), (NULL);
-- expect ERROR
CREATE INDEX t_null_idx ON t_null USING smol(a);
