-- smol access method extension
CREATE FUNCTION smol_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Create the access method
CREATE ACCESS METHOD smol TYPE INDEX HANDLER smol_handler;

-- Comment on the access method
COMMENT ON ACCESS METHOD smol IS 'read-only space-efficient index access method optimized for index-only scans';

-- Create operator classes for common data types
CREATE OPERATOR CLASS int4_ops
    DEFAULT FOR TYPE int4 USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btint4cmp(int4,int4);

CREATE OPERATOR CLASS text_ops
    DEFAULT FOR TYPE text USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       bttextcmp(text,text);

CREATE OPERATOR CLASS numeric_ops
    DEFAULT FOR TYPE numeric USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       numeric_cmp(numeric,numeric);

CREATE OPERATOR CLASS int2_ops
    DEFAULT FOR TYPE int2 USING smol AS
        OPERATOR        1       <  ,
        OPERATOR        2       <= ,
        OPERATOR        3       =  ,
        OPERATOR        4       >= ,
        OPERATOR        5       >  ,
        FUNCTION        1       btint2cmp(int2,int2);
