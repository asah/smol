-- two-key generic row-major: (date, int4)
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

DROP TABLE IF EXISTS d2 CASCADE;
CREATE UNLOGGED TABLE d2(d date, b int4);
INSERT INTO d2 SELECT DATE '2020-01-01' + (i-1), (i % 10) FROM generate_series(1,50) i;
CREATE INDEX d2_idx ON d2 USING smol(d,b);

SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;
-- Lower bound at the 10th smallest date => expect 41 rows
WITH m AS (SELECT d AS md FROM d2 ORDER BY d LIMIT 1 OFFSET 9)
SELECT count(*) FROM d2, m WHERE d >= md;

