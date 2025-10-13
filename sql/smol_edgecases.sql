SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- SMOL edge-case correctness checks (kept separate from REGRESS)
-- Run manually: psql -f sql/smol_edgecases.sql
-- All tests are small and fast; they exercise boundaries, duplicates,
-- page chains, backward scans, rescans, and two-column equality filters.

SET client_min_messages = warning;

-- 1) Empty and single-leaf trees (compare vs BTREE)
DROP TABLE IF EXISTS e1 CASCADE; CREATE UNLOGGED TABLE e1(a int);
CREATE INDEX e1_bt ON e1 USING btree(a);
SET enable_seqscan=off; SELECT count(*) AS empty_bt_count FROM e1 WHERE a >= 0; \gset
DROP INDEX e1_bt;
CREATE INDEX e1_smol ON e1 USING smol(a);
SET enable_seqscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=off;
SELECT count(*) AS empty_sm_count FROM e1 WHERE a >= 0; \gset
SELECT (:'empty_bt_count' = :'empty_sm_count') AS empty_match;
DROP INDEX e1_smol;

DROP TABLE IF EXISTS s1 CASCADE; CREATE UNLOGGED TABLE s1(a int);
INSERT INTO s1 SELECT i FROM generate_series(1,1000) i;  -- should fit 1 leaf
-- BTREE baseline
CREATE INDEX s1_bt ON s1 USING btree(a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
SELECT md5(array_to_string(array_agg(a ORDER BY a), ',')) AS s1_bt_asc FROM s1 WHERE a >= 990; \gset
SELECT md5(array_to_string(array_agg(a ORDER BY a DESC), ',')) AS s1_bt_desc FROM s1 WHERE a >= 990; \gset
DROP INDEX s1_bt;
-- SMOL
CREATE INDEX s1_smol ON s1 USING smol(a);
SET enable_indexscan=off;  -- SMOL requires IOS
SELECT md5(array_to_string(array_agg(a ORDER BY a), ',')) AS s1_sm_asc FROM s1 WHERE a >= 990; \gset
SELECT md5(array_to_string(array_agg(a ORDER BY a DESC), ',')) AS s1_sm_desc FROM s1 WHERE a >= 990; \gset
SELECT (:'s1_bt_asc' = :'s1_sm_asc') AS s1_asc_match,
       (:'s1_bt_desc' = :'s1_sm_desc') AS s1_desc_match;

-- 2) Boundaries/negatives (int2) vs BTREE
DROP TABLE IF EXISTS b1 CASCADE; CREATE UNLOGGED TABLE b1(a int2);
INSERT INTO b1 VALUES (('-32768')::int2),(-1::int2),(0::int2),(1::int2),(32767::int2);
-- BTREE
CREATE INDEX b1_bt ON b1 USING btree(a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
SELECT md5(array_to_string(array_agg(a ORDER BY a), ',')) AS b1_bt_ge_neg1 FROM b1 WHERE a >= -1; \gset
SELECT md5(array_to_string(array_agg(a ORDER BY a), ',')) AS b1_bt_gt_neg1 FROM b1 WHERE a > -1;  \gset
SELECT md5(array_to_string(array_agg(a ORDER BY a), ',')) AS b1_bt_ge_max  FROM b1 WHERE a >= 32767; \gset
SELECT count(*) AS b1_bt_gt_max0 FROM b1 WHERE a > 32767; \gset
DROP INDEX b1_bt;
-- SMOL
CREATE INDEX b1_smol ON b1 USING smol(a);
SET enable_indexscan=off;
SELECT md5(array_to_string(array_agg(a ORDER BY a), ',')) AS b1_sm_ge_neg1 FROM b1 WHERE a >= -1; \gset
SELECT md5(array_to_string(array_agg(a ORDER BY a), ',')) AS b1_sm_gt_neg1 FROM b1 WHERE a > -1;  \gset
SELECT md5(array_to_string(array_agg(a ORDER BY a), ',')) AS b1_sm_ge_max  FROM b1 WHERE a >= 32767; \gset
SELECT count(*) AS b1_sm_gt_max0 FROM b1 WHERE a > 32767; \gset
SELECT (:'b1_bt_ge_neg1' = :'b1_sm_ge_neg1') AS b1_ge_neg1_match,
       (:'b1_bt_gt_neg1' = :'b1_sm_gt_neg1') AS b1_gt_neg1_match,
       (:'b1_bt_ge_max'  = :'b1_sm_ge_max')  AS b1_ge_max_match,
       (:'b1_bt_gt_max0' = :'b1_sm_gt_max0') AS b1_gt_max0_match;

-- 3) Duplicates and page-crossing duplicates vs BTREE
DROP TABLE IF EXISTS d1 CASCADE; CREATE UNLOGGED TABLE d1(a int);
INSERT INTO d1 SELECT 42 FROM generate_series(1,20000);
INSERT INTO d1 SELECT 7  FROM generate_series(1,10000);
-- BTREE
CREATE INDEX d1_bt ON d1 USING btree(a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
SELECT count(*) AS d1_bt_ge7 FROM d1 WHERE a >= 7; \gset
SELECT count(*) AS d1_bt_eq42 FROM d1 WHERE a = 42; \gset
SELECT array_agg(a ORDER BY a DESC) AS d1_bt_top5 FROM (SELECT a FROM d1 WHERE a >= 7 ORDER BY a DESC LIMIT 5) s; \gset
DROP INDEX d1_bt;
-- SMOL
CREATE INDEX d1_smol ON d1 USING smol(a);
SET enable_indexscan=off;
SELECT count(*) AS d1_sm_ge7 FROM d1 WHERE a >= 7; \gset
SELECT count(*) AS d1_sm_eq42 FROM d1 WHERE a = 42; \gset
SELECT array_agg(a ORDER BY a DESC) AS d1_sm_top5 FROM (SELECT a FROM d1 WHERE a >= 7 ORDER BY a DESC LIMIT 5) s; \gset
SELECT (:'d1_bt_ge7' = :'d1_sm_ge7')   AS d1_ge7_match,
       (:'d1_bt_eq42' = :'d1_sm_eq42') AS d1_eq42_match,
       (:'d1_bt_top5' = :'d1_sm_top5') AS d1_top5_match;

-- 4) Two-column equality on second key vs BTREE
DROP TABLE IF EXISTS t2 CASCADE; CREATE UNLOGGED TABLE t2(a int2, b int2);
INSERT INTO t2 SELECT (i % 100)::int2, (i % 5)::int2 FROM generate_series(1,20000) i;
-- BTREE baseline (INCLUDE to match IOS behavior)
CREATE INDEX t2_bt ON t2 USING btree(b) INCLUDE (a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
DROP TABLE IF EXISTS res_t2; CREATE TEMP TABLE res_t2(s bigint);
INSERT INTO res_t2 SELECT sum(a)::bigint FROM t2 WHERE b > 2 AND a = 17;
DROP INDEX t2_bt;
-- SMOL
CREATE INDEX t2_smol ON t2 USING smol(b,a);
SET enable_indexscan=off;  -- SMOL IOS only
SELECT (SELECT s FROM res_t2 LIMIT 1) IS NOT DISTINCT FROM (SELECT sum(a)::bigint FROM t2 WHERE b > 2 AND a = 17) AS t2_eq_match;

-- 4b) Large-group equality stress: many rows share the same b, sparse equals on a
DROP TABLE IF EXISTS t2g CASCADE; CREATE UNLOGGED TABLE t2g(a int4, b int4);
-- Build large groups on b: repeat each b value many times; set a to i%10000 for spread
INSERT INTO t2g
SELECT (i % 10000)::int4 AS a, (i / 1000)::int4 AS b
FROM generate_series(0, 199999) AS i;  -- 200k rows, b has 200 groups of size ~1000
-- BTREE baseline
CREATE INDEX t2g_bt ON t2g USING btree(b) INCLUDE (a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
DROP TABLE IF EXISTS res_t2g; CREATE TEMP TABLE res_t2g(s bigint, c bigint);
INSERT INTO res_t2g SELECT COALESCE(sum(a),0)::bigint, count(*)::bigint FROM t2g WHERE b = 42 AND a = 777;
DROP INDEX t2g_bt;
-- SMOL
CREATE INDEX t2g_smol ON t2g USING smol(b,a);
SET enable_indexscan=off;  -- SMOL IOS only
SELECT ((SELECT s FROM res_t2g) = (SELECT COALESCE(sum(a),0)::bigint FROM t2g WHERE b = 42 AND a = 777)
        AND (SELECT c FROM res_t2g) = (SELECT count(*)::bigint FROM t2g WHERE b = 42 AND a = 777)) AS t2g_eq_match;

-- 5) No-match/out-of-range vs BTREE
DROP TABLE IF EXISTS nm CASCADE; CREATE UNLOGGED TABLE nm(a int8);
INSERT INTO nm SELECT i::bigint FROM generate_series(1,10000) i;
-- BTREE
CREATE INDEX nm_bt ON nm USING btree(a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
SELECT count(*) AS nm_bt_big FROM nm WHERE a > 1000000000000; \gset
SELECT count(*) AS nm_bt_neg FROM nm WHERE a = -1; \gset
DROP INDEX nm_bt;
-- SMOL
CREATE INDEX nm_smol ON nm USING smol(a);
SET enable_indexscan=off;
SELECT count(*) AS nm_sm_big FROM nm WHERE a > 1000000000000; \gset
SELECT count(*) AS nm_sm_neg FROM nm WHERE a = -1; \gset
SELECT (:'nm_bt_big' = :'nm_sm_big') AS nm_big_match,
       (:'nm_bt_neg' = :'nm_sm_neg') AS nm_neg_match;

-- 6) Rescan behavior via PREPARE (compare counts)
DROP TABLE IF EXISTS rs CASCADE; CREATE UNLOGGED TABLE rs(a int);
INSERT INTO rs SELECT i FROM generate_series(1,10000) i;
-- BTREE
CREATE INDEX rs_bt ON rs USING btree(a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
PREPARE qbt(int) AS SELECT count(*) FROM rs WHERE a >= $1;
EXECUTE qbt(9000); \gset
EXECUTE qbt(9500); \gset
EXECUTE qbt(1);    \gset
DROP INDEX rs_bt;
-- SMOL
CREATE INDEX rs_smol ON rs USING smol(a);
SET enable_indexscan=off; SET enable_indexonlyscan=on;
PREPARE qsm(int) AS SELECT count(*) FROM rs WHERE a >= $1;
EXECUTE qsm(9000); \gset
EXECUTE qsm(9500); \gset
EXECUTE qsm(1);    \gset
SELECT (:'count' = :'count') AS rs_9000_match;  -- last assignment reused; keep explicit checks below
-- Explicit matches for captured results
-- Re-run quickly to bind into named vars
SELECT count(*) AS rs_bt_9000 FROM rs WHERE a >= 9000; \gset
SELECT count(*) AS rs_sm_9000 FROM rs WHERE a >= 9000; \gset
SELECT count(*) AS rs_bt_9500 FROM rs WHERE a >= 9500; \gset
SELECT count(*) AS rs_sm_9500 FROM rs WHERE a >= 9500; \gset
SELECT count(*) AS rs_bt_1    FROM rs WHERE a >= 1;    \gset
SELECT count(*) AS rs_sm_1    FROM rs WHERE a >= 1;    \gset
SELECT (:'rs_bt_9000' = :'rs_sm_9000') AS rs_9000_match,
       (:'rs_bt_9500' = :'rs_sm_9500') AS rs_9500_match,
       (:'rs_bt_1'    = :'rs_sm_1')    AS rs_1_match;

-- 7) Backward scan parity vs BTREE
DROP TABLE IF EXISTS bw CASCADE; CREATE UNLOGGED TABLE bw(a int);
INSERT INTO bw SELECT i FROM generate_series(1,2000) i;
-- BTREE
CREATE INDEX bw_bt ON bw USING btree(a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
SELECT array_agg(a ORDER BY a DESC) AS bw_bt_back20 FROM (SELECT a FROM bw WHERE a >= 1500 ORDER BY a DESC LIMIT 20) s1; \gset
SELECT array_agg(a ORDER BY a ASC)  AS bw_bt_forw20 FROM (SELECT a FROM bw WHERE a >= 1500 ORDER BY a ASC LIMIT 20) s2;  \gset
DROP INDEX bw_bt;
-- SMOL
CREATE INDEX bw_smol ON bw USING smol(a);
SET enable_indexscan=off;
SELECT array_agg(a ORDER BY a DESC) AS bw_sm_back20 FROM (SELECT a FROM bw WHERE a >= 1500 ORDER BY a DESC LIMIT 20) s1; \gset
SELECT array_agg(a ORDER BY a ASC)  AS bw_sm_forw20 FROM (SELECT a FROM bw WHERE a >= 1500 ORDER BY a ASC LIMIT 20) s2;  \gset
SELECT (:'bw_bt_back20' = :'bw_sm_back20') AS bw_back_match,
       (:'bw_bt_forw20' = :'bw_sm_forw20') AS bw_forw_match;

-- 8) Non-leading-key plan safety (compare vs BTREE on (a,b) and (b,a))
DROP TABLE IF EXISTS nl CASCADE; CREATE UNLOGGED TABLE nl(a int, b int);
INSERT INTO nl SELECT (i % 10), i FROM generate_series(1,5000) i;
-- BTREE baseline (use (b,a) to best match query)
CREATE INDEX nl_bt ON nl USING btree(b,a);
SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
SELECT (SELECT b FROM nl WHERE b = 1234 ORDER BY a LIMIT 1) AS nl_bt_row; \gset
DROP INDEX nl_bt;
-- SMOL on (b,a)
CREATE INDEX nl_smol ON nl USING smol(b,a);
SET enable_indexscan=off; SET enable_indexonlyscan=on;
SELECT (SELECT b FROM nl WHERE b = 1234 ORDER BY a LIMIT 1) AS nl_sm_row; \gset
SELECT (:'nl_bt_row' = :'nl_sm_row') AS nl_nonleading_match;
