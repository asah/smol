-- Test multi-type operator family to cover validation continue (line 2568)
SET client_min_messages = warning;
DROP EXTENSION IF EXISTS smol CASCADE;
CREATE EXTENSION smol;

-- Add int2 to the int4 operator family to create a cross-type scenario
ALTER OPERATOR FAMILY integer_ops USING smol ADD
    OPERATOR 1 < (int2, int2),
    OPERATOR 2 <= (int2, int2),
    OPERATOR 3 = (int2, int2),
    OPERATOR 4 >= (int2, int2),
    OPERATOR 5 > (int2, int2),
    FUNCTION 1 btint2cmp(int2, int2);

-- Now create an int2 operator class in this family
CREATE OPERATOR CLASS int2_ops
    FOR TYPE int2 USING smol FAMILY integer_ops AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 btint2cmp(int2, int2);

-- Test it works
CREATE TABLE t_int2_multi (k int2);
INSERT INTO t_int2_multi SELECT i::int2 FROM generate_series(1, 100) i;
CREATE INDEX t_int2_multi_idx ON t_int2_multi USING smol(k);
SELECT count(*) FROM t_int2_multi WHERE k > 50::int2;

-- Cleanup
DROP TABLE t_int2_multi CASCADE;
DROP OPERATOR CLASS int2_ops USING smol;
ALTER OPERATOR FAMILY integer_ops USING smol DROP
    OPERATOR 1 (int2, int2),
    OPERATOR 2 (int2, int2),
    OPERATOR 3 (int2, int2),
    OPERATOR 4 (int2, int2),
    OPERATOR 5 (int2, int2),
    FUNCTION 1 (int2, int2);
