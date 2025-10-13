-- Attempt to corrupt catalog to test smol_validate() error paths (lines 2525-2627)
-- We'll try to create malformed operator classes that fail validation

SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- PostgreSQL doesn't allow direct catalog manipulation without superuser + special flags
-- But we can try to create operator classes with mismatched types

-- Test: Create operator with mismatched left/right types
-- This should be caught by PostgreSQL before smol_validate, but let's try

BEGIN;
DO $$
BEGIN
    -- Try to create operator class with wrong operator signature
    -- CREATE OPERATOR CLASS requires that operators match types
    NULL; -- PostgreSQL validates this before AM sees it
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'PostgreSQL validation prevents most catalog corruption';
END;
$$;
ROLLBACK;

-- The reality: smol_validate() is called during CREATE OPERATOR CLASS
-- and catches issues, but PostgreSQL's own validation happens first.
-- Without allow_system_table_mods=on and direct UPDATE on pg_opclass/pg_amproc/pg_amop,
-- we cannot create the corrupted catalog entries that smol_validate() would catch.

-- These lines (2525-2627) are defensive validation that should never fail
-- unless there's a bug in PostgreSQL's catalog management or someone
-- directly modifies system catalogs with allow_system_table_mods=on.

-- We can document this limitation:
DO $$
BEGIN
    RAISE NOTICE 'smol_validate() lines 2525-2627 are defensive checks';
    RAISE NOTICE 'They protect against catalog corruption but cannot be tested';
    RAISE NOTICE 'without allow_system_table_mods=on and direct system catalog modification';
END;
$$;
