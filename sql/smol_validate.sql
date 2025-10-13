SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test smol_validate() function (line 2433)
-- This function is called by PostgreSQL during CREATE OPERATOR CLASS
-- It gets executed when the extension is created/loaded


-- Query all smol operator classes (validates they were created successfully)
SELECT opcname, opfmethod.amname
FROM pg_opclass opc
JOIN pg_am opfmethod ON opc.opcmethod = opfmethod.oid
WHERE opfmethod.amname = 'smol'
ORDER BY opcname;

-- Verify that smol indexes can be created with all supported types
DROP TABLE IF EXISTS t_validate CASCADE;
CREATE UNLOGGED TABLE t_validate (
    i2 int2,
    i4 int4,
    i8 int8,
    f4 float4,
    f8 float8,
    d date,
    t text
);

-- Create indexes to ensure operator classes are valid
CREATE INDEX idx_val_i2 ON t_validate USING smol(i2);
CREATE INDEX idx_val_i4 ON t_validate USING smol(i4);
CREATE INDEX idx_val_i8 ON t_validate USING smol(i8);
CREATE INDEX idx_val_f4 ON t_validate USING smol(f4);
CREATE INDEX idx_val_f8 ON t_validate USING smol(f8);
CREATE INDEX idx_val_d ON t_validate USING smol(d);
CREATE INDEX idx_val_t ON t_validate USING smol(t);

-- Verify all indexes were created successfully
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 't_validate'
ORDER BY indexname;

-- Cleanup
DROP TABLE t_validate CASCADE;
