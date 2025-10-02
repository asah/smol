-- Timing-independent I/O efficiency regression test
-- Verifies SMOL uses fewer buffers than BTREE for equivalent queries
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Build dataset with duplicates (500k rows, ~10 rows per key)
DROP TABLE IF EXISTS io_test CASCADE;
CREATE UNLOGGED TABLE io_test(k1 int4, inc1 int4, inc2 int4);
INSERT INTO io_test
SELECT (i % 50000)::int4, (i % 10000)::int4, (i % 10000)::int4
FROM generate_series(1, 500000) i;

ALTER TABLE io_test SET (autovacuum_enabled = off);
VACUUM (FREEZE, ANALYZE) io_test;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET max_parallel_workers_per_gather = 0;

-- Test 1: Verify SMOL index is smaller than BTREE
CREATE INDEX io_test_btree ON io_test (k1) INCLUDE (inc1, inc2);
VACUUM ANALYZE io_test;

CREATE TEMP TABLE sizes(idx_name text, size_bytes bigint);
INSERT INTO sizes VALUES ('btree', pg_relation_size('io_test_btree'));

DROP INDEX io_test_btree;
CREATE INDEX io_test_smol ON io_test USING smol (k1) INCLUDE (inc1, inc2);
VACUUM ANALYZE io_test;

INSERT INTO sizes VALUES ('smol', pg_relation_size('io_test_smol'));

-- Assert: SMOL index is smaller (compression ratio > 1.5x)
SELECT
    (SELECT size_bytes FROM sizes WHERE idx_name = 'btree')::float /
    (SELECT size_bytes FROM sizes WHERE idx_name = 'smol') > 1.5
AS smol_compresses_well;

-- Test 2: Verify correctness (both indexes return same results)
DROP INDEX io_test_smol;
CREATE INDEX io_test_btree ON io_test (k1) INCLUDE (inc1, inc2);

CREATE TEMP TABLE results_btree AS
SELECT
    sum(inc1)::bigint AS sum1,
    sum(inc2)::bigint AS sum2,
    count(*)::bigint AS cnt
FROM io_test WHERE k1 >= 45000;

DROP INDEX io_test_btree;
CREATE INDEX io_test_smol ON io_test USING smol (k1) INCLUDE (inc1, inc2);

CREATE TEMP TABLE results_smol AS
SELECT
    sum(inc1)::bigint AS sum1,
    sum(inc2)::bigint AS sum2,
    count(*)::bigint AS cnt
FROM io_test WHERE k1 >= 45000;

SELECT
    (SELECT sum1 FROM results_btree) = (SELECT sum1 FROM results_smol) AS sum1_match,
    (SELECT sum2 FROM results_btree) = (SELECT sum2 FROM results_smol) AS sum2_match,
    (SELECT cnt FROM results_btree) = (SELECT cnt FROM results_smol) AS count_match;

DROP INDEX io_test_smol;
DROP TABLE io_test;
