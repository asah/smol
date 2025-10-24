-- Test smol_validate() with multi-type operator family to cover cross-type continue path
-- This test covers the "continue" statement when procform->amproclefttype != opcintype

-- Create a test operator family with support procedures for multiple types
CREATE OPERATOR FAMILY smol_multitype_family USING smol;

-- Add support function for int4 (this will be our opclass type)
CREATE FUNCTION smol_test_int4_cmp(int4, int4) RETURNS int4
AS 'btint4cmp' LANGUAGE internal IMMUTABLE STRICT;

-- Add support function for int8 (different type - will trigger continue in validation)
CREATE FUNCTION smol_test_int8_cmp(int8, int8) RETURNS int4
AS 'btint8cmp' LANGUAGE internal IMMUTABLE STRICT;

-- First add int8 support to the family
ALTER OPERATOR FAMILY smol_multitype_family USING smol ADD
    FUNCTION 1 (int8, int8) smol_test_int8_cmp(int8, int8);

-- Create operator class for int4 (the opclass will add int4 support function)
-- The family now has both int4 (from opclass) and int8 (from ALTER above) support functions
CREATE OPERATOR CLASS smol_multitype_int4_ops
    FOR TYPE int4 USING smol FAMILY smol_multitype_family AS
    OPERATOR 1 <,
    OPERATOR 2 <=,
    OPERATOR 3 =,
    OPERATOR 4 >=,
    OPERATOR 5 >,
    FUNCTION 1 smol_test_int4_cmp(int4, int4);

-- Get the OID and validate
-- This will loop through both int4 and int8 support procs
-- The int8 proc will have amproclefttype = int8oid != opcintype (int4oid)
-- This triggers the "continue" at line 388
SELECT smol_test_validate(oid) FROM pg_opclass WHERE opcname = 'smol_multitype_int4_ops';

-- Cleanup
DROP OPERATOR CLASS smol_multitype_int4_ops USING smol;
DROP FUNCTION smol_test_int4_cmp(int4, int4);
DROP FUNCTION smol_test_int8_cmp(int8, int8);
DROP OPERATOR FAMILY smol_multitype_family USING smol;
