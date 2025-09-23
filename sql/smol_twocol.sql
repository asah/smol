-- two-column SMOL correctness (int2,int2)
CREATE TABLE mm2(a int2, b int2);
INSERT INTO mm2 SELECT i::int2, (i % 5)::int2 FROM generate_series(1,10) AS s(i);
CREATE INDEX mm2_ba_smol ON mm2 USING smol(b,a);
SET enable_seqscan = off;
SELECT sum(a) FROM mm2 WHERE b >= 3;
