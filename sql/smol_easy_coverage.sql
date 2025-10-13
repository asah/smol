-- Comprehensive test for remaining easy coverage targets
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;
DROP EXTENSION IF EXISTS smol CASCADE;

-- TEST 1: Cost estimation without leading key (line 2697)
DROP TABLE IF EXISTS t_nokey CASCADE;
CREATE TABLE t_nokey (a int4, b int4);
CREATE INDEX t_nokey_idx ON t_nokey USING smol(a);
INSERT INTO t_nokey SELECT i, i*2 FROM generate_series(1, 1000) i;
-- Query without leading key constraint should hit line 2697
EXPLAIN SELECT * FROM t_nokey WHERE b > 500;
DROP TABLE t_nokey CASCADE;

-- TEST 2: Text32 key exceeds 32 bytes (line 4615)
DROP TABLE IF EXISTS t_longtext CASCADE;
CREATE TABLE t_longtext (k text);
INSERT INTO t_longtext VALUES (repeat('x', 33));  -- 33 bytes > 32
BEGIN;
DO $$
BEGIN
    EXECUTE 'CREATE INDEX t_longtext_idx ON t_longtext USING smol(k)';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Expected error: %', SQLERRM;
END;
$$;
ROLLBACK;
DROP TABLE t_longtext CASCADE;

-- TEST 3: Text varlena copy paths (lines 4507-4511, 4520, 4553-4554)
DROP TABLE IF EXISTS t_varlena CASCADE;
CREATE TABLE t_varlena (k text, v1 bytea, v2 int2, v3 text);
INSERT INTO t_varlena SELECT 
    'key' || i::text,  -- Variable length text keys
    E'\\x' || to_hex(i)::bytea,  -- Variable length bytea
    (i % 100)::int2,
    repeat('v', i % 20)  -- Variable length include
FROM generate_series(1, 1000) i;
CREATE INDEX t_varlena_idx ON t_varlena USING smol(k) INCLUDE (v1, v2, v3);
SELECT count(*) FROM t_varlena WHERE k >= 'key500';
DROP TABLE t_varlena CASCADE;

-- TEST 4: Multi-type operator family (line 2568) 
ALTER OPERATOR FAMILY integer_ops USING smol ADD
    OPERATOR 1 < (int2, int2),
    FUNCTION 1 btint2cmp(int2, int2);
CREATE OPERATOR CLASS int2_ops_test
    FOR TYPE int2 USING smol FAMILY integer_ops AS
    OPERATOR 1 <,
    FUNCTION 1 btint2cmp(int2, int2);
-- Validation should skip int4 entries when validating int2
DROP OPERATOR CLASS int2_ops_test USING smol;
ALTER OPERATOR FAMILY integer_ops USING smol DROP
    OPERATOR 1 (int2, int2),
    FUNCTION 1 (int2, int2);

-- TEST 5: smol_options (line 2496) - already tested in _PG_init but call explicitly
-- This function is exposed as SQL but just returns NULL
-- Already covered in synthetic tests

SELECT 'All easy coverage tests completed';
