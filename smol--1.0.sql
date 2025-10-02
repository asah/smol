-- smol access method extension
CREATE FUNCTION smol_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Test functions for coverage (call AM functions directly to bypass planner)
CREATE FUNCTION smol_test_backward_scan(regclass)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION smol_test_backward_scan(regclass, integer)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION smol_test_backward_scan(regclass) IS
'Test function to force backward scan execution (for code coverage)';
COMMENT ON FUNCTION smol_test_backward_scan(regclass, integer) IS
'Test function to force backward scan with bound (for code coverage)';

CREATE FUNCTION smol_test_error_non_ios(regclass)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION smol_test_no_movement(regclass)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION smol_test_error_non_ios(regclass) IS
'Test function to exercise non-index-only scan error path (for code coverage)';
COMMENT ON FUNCTION smol_test_no_movement(regclass) IS
'Test function to exercise NoMovementScanDirection path (for code coverage)';

-- Create the access method
CREATE ACCESS METHOD smol TYPE INDEX HANDLER smol_handler;

-- Comment on the access method
COMMENT ON ACCESS METHOD smol IS 'space-efficient index access method optimized for index-only scans';

-- Create operator classes for common data types
CREATE OPERATOR CLASS int4_ops
    DEFAULT FOR TYPE int4 USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btint4cmp(int4,int4),
        -- cross-type ops to aid qual matching
        OPERATOR        1       < (int4, int2),
        OPERATOR        2       <= (int4, int2),
        OPERATOR        3       = (int4, int2),
        OPERATOR        4       >= (int4, int2),
        OPERATOR        5       > (int4, int2),
        OPERATOR        1       < (int4, int8),
        OPERATOR        2       <= (int4, int8),
        OPERATOR        3       = (int4, int8),
        OPERATOR        4       >= (int4, int8),
        OPERATOR        5       > (int4, int8);

-- Fixed-width only: do not provide varlena opclasses (e.g., text, numeric)

CREATE OPERATOR CLASS int8_ops
    DEFAULT FOR TYPE int8 USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btint8cmp(int8,int8),
        -- cross-type ops to aid qual matching
        OPERATOR        1       < (int8, int2),
        OPERATOR        2       <= (int8, int2),
        OPERATOR        3       = (int8, int2),
        OPERATOR        4       >= (int8, int2),
        OPERATOR        5       > (int8, int2),
        OPERATOR        1       < (int8, int4),
        OPERATOR        2       <= (int8, int4),
        OPERATOR        3       = (int8, int4),
        OPERATOR        4       >= (int8, int4),
        OPERATOR        5       > (int8, int4);

CREATE OPERATOR CLASS int2_ops
    DEFAULT FOR TYPE int2 USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btint2cmp(int2,int2),
        -- cross-type ops for common literal types
        OPERATOR        1       < (int2, int4),
        OPERATOR        2       <= (int2, int4),
        OPERATOR        3       = (int2, int4),
        OPERATOR        4       >= (int2, int4),
        OPERATOR        5       > (int2, int4),
        OPERATOR        1       < (int2, int8),
        OPERATOR        2       <= (int2, int8),
        OPERATOR        3       = (int2, int8),
        OPERATOR        4       >= (int2, int8),
        OPERATOR        5       > (int2, int8);

-- Additional fixed-length builtin types (by-value up to 8 bytes)

CREATE OPERATOR CLASS oid_ops
    DEFAULT FOR TYPE oid USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btoidcmp(oid,oid);

CREATE OPERATOR CLASS float4_ops
    DEFAULT FOR TYPE float4 USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btfloat4cmp(float4,float4);

CREATE OPERATOR CLASS float8_ops
    DEFAULT FOR TYPE float8 USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btfloat8cmp(float8,float8);

CREATE OPERATOR CLASS date_ops
    DEFAULT FOR TYPE date USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       date_cmp(date,date);

CREATE OPERATOR CLASS timestamp_ops
    DEFAULT FOR TYPE timestamp USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       timestamp_cmp(timestamp,timestamp);

CREATE OPERATOR CLASS timestamptz_ops
    DEFAULT FOR TYPE timestamptz USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       timestamptz_cmp(timestamptz,timestamptz);

CREATE OPERATOR CLASS bool_ops
    DEFAULT FOR TYPE bool USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btboolcmp(bool,bool);

CREATE OPERATOR CLASS money_ops
    DEFAULT FOR TYPE money USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       cash_cmp(money,money);

-- Additional fixed-length builtin types

CREATE OPERATOR CLASS uuid_ops
    DEFAULT FOR TYPE uuid USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       uuid_cmp(uuid,uuid);

CREATE OPERATOR CLASS macaddr_ops
    DEFAULT FOR TYPE macaddr USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       macaddr_cmp(macaddr,macaddr);

CREATE OPERATOR CLASS macaddr8_ops
    DEFAULT FOR TYPE macaddr8 USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       macaddr8_cmp(macaddr8,macaddr8);

CREATE OPERATOR CLASS name_ops
    DEFAULT FOR TYPE name USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btnamecmp(name,name);

CREATE OPERATOR CLASS char_ops
    DEFAULT FOR TYPE "char" USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btcharcmp("char","char");

CREATE OPERATOR CLASS lsn_ops
    DEFAULT FOR TYPE pg_lsn USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       pg_lsn_cmp(pg_lsn,pg_lsn);

-- time/interval types (fixed-size)
CREATE OPERATOR CLASS time_ops
    DEFAULT FOR TYPE time USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       time_cmp(time,time);

CREATE OPERATOR CLASS timetz_ops
    DEFAULT FOR TYPE timetz USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       timetz_cmp(timetz,timetz);

CREATE OPERATOR CLASS interval_ops
    DEFAULT FOR TYPE interval USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       interval_cmp(interval,interval);

-- Short text (C collation, <=32 bytes) using binary order
-- Users should specify C/POSIX collation to ensure binary order semantics.
CREATE OPERATOR CLASS text_ops
    DEFAULT FOR TYPE text USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       bttextcmp(text,text);
