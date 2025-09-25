-- two-key generic row-major: (uuid, int4)
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

DROP TABLE IF EXISTS u2 CASCADE;
CREATE UNLOGGED TABLE u2(u uuid, b int4);
-- gen_random_uuid() is available in the image; pgcrypto is preinstalled
INSERT INTO u2 SELECT gen_random_uuid(), (i % 10) FROM generate_series(1,50) i;
CREATE INDEX u2_idx ON u2 USING smol(u,b);

SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;
-- Lower bound at minimal uuid ensures full-cardinality match
WITH m AS (SELECT u AS mu FROM u2 ORDER BY u LIMIT 1)
SELECT count(*) FROM u2, m WHERE u >= mu;
