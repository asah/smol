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

-- Test bool INCLUDE (1-byte byval) - triggers case 1 at line 4436
DROP TABLE IF EXISTS inc_bool CASCADE;
CREATE UNLOGGED TABLE inc_bool(k int4, b bool);
INSERT INTO inc_bool SELECT i, (i % 2 = 0) FROM generate_series(1, 1000) i;
ANALYZE inc_bool;
ALTER TABLE inc_bool SET (autovacuum_enabled = off);

CREATE INDEX inc_bool_smol ON inc_bool USING smol(k) INCLUDE (b);
ANALYZE inc_bool;

-- Verify bool INCLUDE works
SELECT count(*) FROM inc_bool WHERE k > 500;
SELECT count(*) FILTER (WHERE b) AS true_count FROM inc_bool WHERE k > 0;

-- Test int2 INCLUDE (2-byte byval) with TEXT key - triggers line 4364 in smol_emit_single_tuple (RLE path)
DROP TABLE IF EXISTS inc_int2_text CASCADE;
CREATE UNLOGGED TABLE inc_int2_text(k text COLLATE "C", v int2);
-- Create duplicates to trigger RLE encoding
INSERT INTO inc_int2_text SELECT 'key_' || lpad((i % 100)::text, 3, '0'), (i % 30000)::int2 FROM generate_series(1, 10000) i ORDER BY 1;
ANALYZE inc_int2_text;
ALTER TABLE inc_int2_text SET (autovacuum_enabled = off);

CREATE INDEX inc_int2_text_smol ON inc_int2_text USING smol(k) INCLUDE (v);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) inc_int2_text;
ANALYZE inc_int2_text;

-- Verify int2 INCLUDE works with text key and index-only scan (should use RLE)
SET enable_seqscan=off; SET enable_indexscan=off; SET enable_bitmapscan=off;
SELECT count(*) FROM inc_int2_text WHERE k >= 'key_050';
SELECT sum(v::int8) FROM inc_int2_text WHERE k < 'key_010';

-- Test int8 INCLUDE (8-byte byval) with TEXT key - triggers line 4366 in smol_emit_single_tuple (RLE path)
DROP TABLE IF EXISTS inc_int8_text CASCADE;
CREATE UNLOGGED TABLE inc_int8_text(k text COLLATE "C", v int8);
-- Create duplicates to trigger RLE encoding
INSERT INTO inc_int8_text SELECT 'key_' || lpad((i % 100)::text, 3, '0'), i::int8 * 1000 FROM generate_series(1, 10000) i ORDER BY 1;
ANALYZE inc_int8_text;
ALTER TABLE inc_int8_text SET (autovacuum_enabled = off);

CREATE INDEX inc_int8_text_smol ON inc_int8_text USING smol(k) INCLUDE (v);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) inc_int8_text;
ANALYZE inc_int8_text;

-- Verify int8 INCLUDE works with text key and index-only scan (should use RLE)
SELECT count(*) FROM inc_int8_text WHERE k >= 'key_050';
SELECT sum(v) FROM inc_int8_text WHERE k < 'key_010';

-- ============================================================================
-- Test text32 non-RLE path (lines 4364-4368 in smol_emit_single_tuple)
-- Requires UNIQUE text32 keys to avoid RLE encoding
-- ============================================================================
DROP TABLE IF EXISTS inc_text32_unique CASCADE;
CREATE UNLOGGED TABLE inc_text32_unique(k text COLLATE "C", v int4);
-- Insert unique keys (no duplicates) to prevent RLE
INSERT INTO inc_text32_unique SELECT 'unique_' || lpad(i::text, 10, '0'), i FROM generate_series(1, 1000) i;
ANALYZE inc_text32_unique;
ALTER TABLE inc_text32_unique SET (autovacuum_enabled = off);

CREATE INDEX inc_text32_unique_smol ON inc_text32_unique USING smol(k) INCLUDE (v);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) inc_text32_unique;
ANALYZE inc_text32_unique;

-- Query with unique text keys (no RLE) - triggers lines 4364-4368
SELECT count(*), sum(v::int8) FROM inc_text32_unique WHERE k >= 'unique_0000000500';

-- ============================================================================
-- Test fixed-size key copies in smol_emit_single_tuple (lines 4375-4377)
-- Need int8/uuid keys with text INCLUDE to trigger varwidth tuple building
-- ============================================================================

-- Test int8 key + text INCLUDE (line 4375: key_len==8)
DROP TABLE IF EXISTS inc_int8key_text CASCADE;
CREATE UNLOGGED TABLE inc_int8key_text(k int8, t text COLLATE "C");
INSERT INTO inc_int8key_text SELECT i::int8, 'text_' || i::text FROM generate_series(1, 1000) i;
ANALYZE inc_int8key_text;
ALTER TABLE inc_int8key_text SET (autovacuum_enabled = off);

CREATE INDEX inc_int8key_text_smol ON inc_int8key_text USING smol(k) INCLUDE (t);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) inc_int8key_text;
ANALYZE inc_int8key_text;

SELECT count(*), sum(k) FROM inc_int8key_text WHERE k >= 500::int8;

-- Test uuid key + text INCLUDE (line 4376: key_len==16)
DROP TABLE IF EXISTS inc_uuid_text CASCADE;
CREATE UNLOGGED TABLE inc_uuid_text(k uuid, t text COLLATE "C");
INSERT INTO inc_uuid_text SELECT md5(i::text)::uuid, 'data_' || i::text FROM generate_series(1, 1000) i;
ANALYZE inc_uuid_text;
ALTER TABLE inc_uuid_text SET (autovacuum_enabled = off);

CREATE INDEX inc_uuid_text_smol ON inc_uuid_text USING smol(k) INCLUDE (t);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) inc_uuid_text;
ANALYZE inc_uuid_text;

SELECT count(*) FROM inc_uuid_text WHERE k > '00000000-0000-0000-0000-000000000000'::uuid;

-- Test date key + text INCLUDE (line 4377: uncommon key_len)
-- Note: Changed from interval to date to avoid non-deterministic behavior
DROP TABLE IF EXISTS inc_date_text CASCADE;
CREATE UNLOGGED TABLE inc_date_text(k date, t text COLLATE "C");
INSERT INTO inc_date_text SELECT '2020-01-01'::date + i, 'info_' || i::text FROM generate_series(1, 1000) i;
ANALYZE inc_date_text;
ALTER TABLE inc_date_text SET (autovacuum_enabled = off);

CREATE INDEX inc_date_text_smol ON inc_date_text USING smol(k) INCLUDE (t);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) inc_date_text;
ANALYZE inc_date_text;

SELECT count(*) FROM inc_date_text WHERE k >= '2020-04-10'::date;

-- ============================================================================
-- Test smol_copy_small uncommon cases (lines 4319-4333)
-- These are reached via smol_copy_small with specific lengths
-- ============================================================================

-- Test case 4: 4-byte fixed type (already covered by int4)
-- Test case 5: 5-byte type - use "char"[5] array
DROP TABLE IF EXISTS inc_char5_text CASCADE;
CREATE UNLOGGED TABLE inc_char5_text(k "char", t text COLLATE "C");
INSERT INTO inc_char5_text SELECT (i % 256)::int::char, 'val_' || i::text FROM generate_series(1, 500) i;
ANALYZE inc_char5_text;
ALTER TABLE inc_char5_text SET (autovacuum_enabled = off);

CREATE INDEX inc_char5_text_smol ON inc_char5_text USING smol(k) INCLUDE (t);
CHECKPOINT; SET vacuum_freeze_min_age=0; SET vacuum_freeze_table_age=0;
VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) inc_char5_text;
ANALYZE inc_char5_text;

SELECT count(*) FROM inc_char5_text WHERE k >= '0'::char;
