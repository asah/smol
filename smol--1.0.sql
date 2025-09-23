-- smol access method extension
CREATE FUNCTION smol_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

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
