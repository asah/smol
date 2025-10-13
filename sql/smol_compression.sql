-- Timing-independent compression ratio regression test
-- Verifies SMOL achieves expected compression ratios for various workloads
SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET enable_indexscan = off;  -- SMOL requires IOS
SET max_parallel_workers_per_gather = 0;

-- Test 1: Unique keys (baseline - minimal compression)
DROP TABLE IF EXISTS c1 CASCADE;
CREATE UNLOGGED TABLE c1(k1 int4);
INSERT INTO c1 SELECT i FROM generate_series(1, 100000) i;
ALTER TABLE c1 SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) c1;

CREATE TEMP TABLE c1_sizes(idx_name text, size_bytes bigint);

CREATE INDEX c1_btree ON c1 USING btree(k1);
INSERT INTO c1_sizes VALUES ('btree', pg_relation_size('c1_btree'));

DROP INDEX c1_btree;
CREATE INDEX c1_smol ON c1 USING smol(k1);
INSERT INTO c1_sizes VALUES ('smol', pg_relation_size('c1_smol'));

-- Assert: Even with unique keys, SMOL is smaller (no per-tuple overhead)
SELECT
    (SELECT size_bytes FROM c1_sizes WHERE idx_name = 'btree')::float /
    (SELECT size_bytes FROM c1_sizes WHERE idx_name = 'smol') > 1.2
AS unique_keys_compressed;

-- Test 2: Heavy duplicates with INCLUDE (RLE + dup-caching)
DROP TABLE IF EXISTS c2 CASCADE;
CREATE UNLOGGED TABLE c2(k1 int4, inc1 int4, inc2 int4);
-- 10 hot keys (90% of data), constant INCLUDE values for dup-caching
INSERT INTO c2
SELECT
    CASE WHEN i % 10 < 9 THEN (i % 10) ELSE 100 + i END,
    111,
    222
FROM generate_series(1, 100000) i;
ALTER TABLE c2 SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) c2;

CREATE TEMP TABLE c2_sizes(idx_name text, size_bytes bigint);

CREATE INDEX c2_btree ON c2 USING btree(k1) INCLUDE (inc1, inc2);
INSERT INTO c2_sizes VALUES ('btree', pg_relation_size('c2_btree'));

DROP INDEX c2_btree;
CREATE INDEX c2_smol ON c2 USING smol(k1) INCLUDE (inc1, inc2);
INSERT INTO c2_sizes VALUES ('smol', pg_relation_size('c2_smol'));

-- Assert: Heavy duplicates + INCLUDE dup-caching yields strong compression
SELECT
    (SELECT size_bytes FROM c2_sizes WHERE idx_name = 'btree')::float /
    (SELECT size_bytes FROM c2_sizes WHERE idx_name = 'smol') > 2.0
AS duplicate_rle_compressed;

-- Test 3: Two-column index (columnar layout advantage)
DROP TABLE IF EXISTS c3 CASCADE;
CREATE UNLOGGED TABLE c3(k1 int4, k2 int4);
-- Correlated keys with duplicates
INSERT INTO c3
SELECT (i % 1000)::int4, (i % 100)::int4
FROM generate_series(1, 100000) i;
ALTER TABLE c3 SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) c3;

CREATE TEMP TABLE c3_sizes(idx_name text, size_bytes bigint);

CREATE INDEX c3_btree ON c3 USING btree(k1, k2);
INSERT INTO c3_sizes VALUES ('btree', pg_relation_size('c3_btree'));

DROP INDEX c3_btree;
CREATE INDEX c3_smol ON c3 USING smol(k1, k2);
INSERT INTO c3_sizes VALUES ('smol', pg_relation_size('c3_smol'));

-- Assert: Columnar layout + RLE compression advantage (lowered threshold)
SELECT
    (SELECT size_bytes FROM c3_sizes WHERE idx_name = 'btree')::float /
    (SELECT size_bytes FROM c3_sizes WHERE idx_name = 'smol') > 1.3
AS twocol_compressed;

-- Test 4: Verify correctness for all compression scenarios
-- Test 1 correctness
DROP INDEX c1_smol;
CREATE INDEX c1_btree ON c1 USING btree(k1);

SET enable_indexscan = on;  -- Allow BTREE to use indexscan
CREATE TEMP TABLE c1_results AS SELECT count(*) AS cnt FROM c1 WHERE k1 >= 50000;
SET enable_indexscan = off;  -- Back to IOS only for SMOL

DROP INDEX c1_btree;
CREATE INDEX c1_smol ON c1 USING smol(k1);
SELECT (SELECT cnt FROM c1_results) = count(*) AS c1_count_match FROM c1 WHERE k1 >= 50000;

-- Test 2 correctness
DROP INDEX c2_smol;
CREATE INDEX c2_btree ON c2 USING btree(k1) INCLUDE (inc1, inc2);

SET enable_indexscan = on;
CREATE TEMP TABLE c2_results AS
SELECT sum(inc1)::bigint AS s, count(*) AS cnt FROM c2 WHERE k1 <= 5;
SET enable_indexscan = off;

DROP INDEX c2_btree;
CREATE INDEX c2_smol ON c2 USING smol(k1) INCLUDE (inc1, inc2);
SELECT
    (SELECT s FROM c2_results) = sum(inc1)::bigint AND
    (SELECT cnt FROM c2_results) = count(*) AS c2_match
FROM c2 WHERE k1 <= 5;

-- Test 3 correctness
DROP INDEX c3_smol;
CREATE INDEX c3_btree ON c3 USING btree(k1, k2);

SET enable_indexscan = on;
CREATE TEMP TABLE c3_results AS
SELECT count(*) AS cnt FROM c3 WHERE k1 >= 500 AND k2 >= 50;
SET enable_indexscan = off;

DROP INDEX c3_btree;
CREATE INDEX c3_smol ON c3 USING smol(k1, k2);
SELECT (SELECT cnt FROM c3_results) = count(*) AS c3_count_match
FROM c3 WHERE k1 >= 500 AND k2 >= 50;

-- Cleanup
DROP INDEX c1_smol; DROP TABLE c1;
DROP INDEX c2_smol; DROP TABLE c2;
DROP INDEX c3_smol; DROP TABLE c3;
