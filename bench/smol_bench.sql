-- Generic SMOL benchmark: full scan or selective, any fixed-width integer type
-- Parameters (psql -v):
--   dtype        : int2 | int4 | int8 (default int2)
--   rows         : number of rows to generate (default 1000000)
--   par_workers  : parallel workers (default 2)
--   maxval       : random value upper bound (default 32767)
--   thr          : threshold for selective runs (default 5000)
--   nofilter     : if defined, run without WHERE (full scan)

\if :{?dtype}
\else
\set dtype int2
\endif
\if :{?rows}
\else
\set rows 1000000
\endif
\if :{?par_workers}
\else
\set par_workers 2
\endif
\if :{?maxval}
\else
\set maxval 32767
\endif
\if :{?thr}
\else
\set thr 5000
\endif

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;
SET search_path = public;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET max_parallel_workers_per_gather = :par_workers;
SET min_parallel_index_scan_size = 0;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

DROP TABLE IF EXISTS bench_tbl CASCADE;
CREATE UNLOGGED TABLE bench_tbl(a :dtype, b :dtype);
INSERT INTO bench_tbl
SELECT (random()*:maxval)::int:: :dtype,
       (random()*:maxval)::int:: :dtype
FROM generate_series(1, :rows);
ANALYZE bench_tbl;
ALTER TABLE bench_tbl SET (autovacuum_enabled = off);

-- BTREE IOS
CREATE INDEX bench_tbl_btree ON bench_tbl (b,a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) bench_tbl;
\timing on
\if :{?nofilter}
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM bench_tbl;
\else
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM bench_tbl WHERE b > :thr;
\endif
\timing off
DROP INDEX bench_tbl_btree;

-- SMOL IOS
CREATE INDEX bench_tbl_smol ON bench_tbl USING smol (b,a);
\timing on
\if :{?nofilter}
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM bench_tbl;
\else
EXPLAIN (ANALYZE, BUFFERS, COSTS off)
SELECT sum(a::bigint) FROM bench_tbl WHERE b > :thr;
\endif
\timing off
DROP TABLE bench_tbl CASCADE;

