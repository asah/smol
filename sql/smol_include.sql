-- INCLUDE regression: single-key b with INCLUDE a1,a2
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

DROP TABLE IF EXISTS inc1 CASCADE;
CREATE UNLOGGED TABLE inc1(b int4, a1 int4, a2 int4);
INSERT INTO inc1 SELECT (i % 100000)::int4, (i % 1234)::int4, (i % 4321)::int4 FROM generate_series(1,100000) AS s(i);
ANALYZE inc1;
ALTER TABLE inc1 SET (autovacuum_enabled = off);

DROP INDEX IF EXISTS inc1_btree;
CREATE INDEX inc1_btree ON inc1 USING btree(b) INCLUDE (a1,a2);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) inc1;
ANALYZE inc1;

SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET enable_indexscan=on;
SELECT sum(a1)::bigint AS bt_sum1, sum(a2)::bigint AS bt_sum2, count(*)::bigint AS bt_cnt FROM inc1 WHERE b > 50000; \gset

DROP INDEX inc1_btree;
CREATE INDEX inc1_smol ON inc1 USING smol(b) INCLUDE (a1,a2);
ANALYZE inc1;
SET enable_indexscan=off;  -- SMOL IOS only
SELECT (SELECT sum(a1)::bigint FROM inc1 WHERE b > 50000) = :'bt_sum1' AS sum1_match,
       (SELECT sum(a2)::bigint FROM inc1 WHERE b > 50000) = :'bt_sum2' AS sum2_match,
       (SELECT count(*)::bigint FROM inc1 WHERE b > 50000) = :'bt_cnt'  AS cnt_match;
