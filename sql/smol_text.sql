-- text key support (<=32B, C collation), ordering and equality
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- basic text index
DROP TABLE IF EXISTS tt CASCADE;
CREATE UNLOGGED TABLE tt(b text COLLATE "C");
INSERT INTO tt(b) VALUES ('a'),('aa'),('ab'),('b'),('a');
CREATE INDEX tt_b_smol ON tt USING smol (b COLLATE "C");
SET enable_seqscan=off; SET enable_indexscan=off; SET enable_indexonlyscan=on;
-- order and eq
SELECT array_agg(b) FROM (SELECT b FROM tt WHERE b >= 'aa' ORDER BY b) s;
SELECT count(*) FROM tt WHERE b = 'a';

-- long text should ERROR on build (>32 bytes)
DROP TABLE IF EXISTS tlong CASCADE;
CREATE UNLOGGED TABLE tlong(b text COLLATE "C");
INSERT INTO tlong(b) VALUES (repeat('x',33));
-- expect ERROR
CREATE INDEX tlong_b_smol ON tlong USING smol (b COLLATE "C");

