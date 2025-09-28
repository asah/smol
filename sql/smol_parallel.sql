-- Parallel two-column regression for int2,int4,int8 with deterministic data.
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Planner knobs to prefer parallel IOS
SET enable_seqscan=off;
SET enable_bitmapscan=off;
SET enable_indexonlyscan=on;
SET max_parallel_workers_per_gather=5;
SET max_parallel_workers=5;
SET parallel_setup_cost=0;
SET parallel_tuple_cost=0;
SET min_parallel_index_scan_size=0;
SET min_parallel_table_scan_size=0;

-- Helper: build BTREE baseline, store sum/count, then compare with SMOL
-- Case 1: int2
DROP TABLE IF EXISTS p2_i2 CASCADE;
CREATE UNLOGGED TABLE p2_i2(a int2, b int2);
INSERT INTO p2_i2 SELECT (i % 32767)::int2, (i % 1000)::int2 FROM generate_series(1,100000) AS s(i);
ANALYZE p2_i2;
ALTER TABLE p2_i2 SET (autovacuum_enabled = off);
DROP TABLE IF EXISTS res_i2;
CREATE TEMP TABLE res_i2(s bigint, c bigint);
DROP INDEX IF EXISTS p2_i2_btree;
CREATE INDEX p2_i2_btree ON p2_i2 USING btree(b) INCLUDE (a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) p2_i2;
ANALYZE p2_i2;
INSERT INTO res_i2 SELECT sum(a)::bigint, count(*)::bigint FROM p2_i2 WHERE b > 500;
DROP INDEX p2_i2_btree;
CREATE INDEX p2_i2_smol ON p2_i2 USING smol(b,a);
ANALYZE p2_i2;
SELECT 'int2' AS typ,
       ((SELECT s FROM res_i2) = (SELECT sum(a)::bigint FROM p2_i2 WHERE b > 500)
        AND (SELECT c FROM res_i2) = (SELECT count(*)::bigint FROM p2_i2 WHERE b > 500)) AS match;

-- Case 2: int4
DROP TABLE IF EXISTS p2_i4 CASCADE;
CREATE UNLOGGED TABLE p2_i4(a int4, b int4);
INSERT INTO p2_i4 SELECT (i % 1000000)::int4, (i % 1000000)::int4 FROM generate_series(1,100000) AS s(i);
ANALYZE p2_i4;
ALTER TABLE p2_i4 SET (autovacuum_enabled = off);
DROP TABLE IF EXISTS res_i4;
CREATE TEMP TABLE res_i4(s bigint, c bigint);
DROP INDEX IF EXISTS p2_i4_btree;
CREATE INDEX p2_i4_btree ON p2_i4 USING btree(b) INCLUDE (a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) p2_i4;
ANALYZE p2_i4;
INSERT INTO res_i4 SELECT sum(a)::bigint, count(*)::bigint FROM p2_i4 WHERE b > 50000;
DROP INDEX p2_i4_btree;
CREATE INDEX p2_i4_smol ON p2_i4 USING smol(b,a);
ANALYZE p2_i4;
SELECT 'int4' AS typ,
       ((SELECT s FROM res_i4) = (SELECT sum(a)::bigint FROM p2_i4 WHERE b > 50000)
        AND (SELECT c FROM res_i4) = (SELECT count(*)::bigint FROM p2_i4 WHERE b > 50000)) AS match;

-- Case 3: int8
DROP TABLE IF EXISTS p2_i8 CASCADE;
CREATE UNLOGGED TABLE p2_i8(a int8, b int8);
INSERT INTO p2_i8 SELECT (i % 1000000)::int8, (i % 1000000)::int8 FROM generate_series(1,100000) AS s(i);
ANALYZE p2_i8;
ALTER TABLE p2_i8 SET (autovacuum_enabled = off);
DROP TABLE IF EXISTS res_i8;
CREATE TEMP TABLE res_i8(s bigint, c bigint);
DROP INDEX IF EXISTS p2_i8_btree;
CREATE INDEX p2_i8_btree ON p2_i8 USING btree(b) INCLUDE (a);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) p2_i8;
ANALYZE p2_i8;
INSERT INTO res_i8 SELECT sum(a)::bigint, count(*)::bigint FROM p2_i8 WHERE b > 50000;
DROP INDEX p2_i8_btree;
CREATE INDEX p2_i8_smol ON p2_i8 USING smol(b,a);
ANALYZE p2_i8;
SELECT 'int8' AS typ,
       ((SELECT s FROM res_i8) = (SELECT sum(a)::bigint FROM p2_i8 WHERE b > 50000)
        AND (SELECT c FROM res_i8) = (SELECT count(*)::bigint FROM p2_i8 WHERE b > 50000)) AS match;

-- Additional int2 correctness checks (strict vs non-strict, equality)
-- Strict vs non-strict on the same threshold
DROP TABLE IF EXISTS res_i2_ge;
CREATE TEMP TABLE res_i2_ge(s bigint, c bigint);
INSERT INTO res_i2_ge SELECT sum(a)::bigint, count(*)::bigint FROM p2_i2 WHERE b >= 500;
SELECT 'int2_ge' AS typ,
       ((SELECT s FROM res_i2_ge) = (SELECT sum(a)::bigint FROM p2_i2 WHERE b >= 500)
        AND (SELECT c FROM res_i2_ge) = (SELECT count(*)::bigint FROM p2_i2 WHERE b >= 500)) AS match;

-- Equality filter on second key
DROP TABLE IF EXISTS res_i2_eq;
CREATE TEMP TABLE res_i2_eq(s bigint, c bigint);
INSERT INTO res_i2_eq SELECT COALESCE(sum(a),0)::bigint, count(*)::bigint FROM p2_i2 WHERE b > 500 AND a = 42;
DROP INDEX IF EXISTS p2_i2_smol;
CREATE INDEX p2_i2_smol ON p2_i2 USING smol(b,a);
ANALYZE p2_i2;
SELECT 'int2_eq' AS typ,
       ((SELECT s FROM res_i2_eq) = (SELECT COALESCE(sum(a),0)::bigint FROM p2_i2 WHERE b > 500 AND a = 42)
        AND (SELECT c FROM res_i2_eq) = (SELECT count(*)::bigint FROM p2_i2 WHERE b > 500 AND a = 42)) AS match;
