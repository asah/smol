-- Test loop_guard mechanism (lines 2933-2940)
-- Uses smol.force_loop_guard_test GUC to force n_this=0

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Create table with enough data for multiple leaf pages
DROP TABLE IF EXISTS t_loop_guard CASCADE;
CREATE UNLOGGED TABLE t_loop_guard (k int4, v int4);

-- Insert 1000 rows
INSERT INTO t_loop_guard SELECT i, i*10 FROM generate_series(1, 1000) i;

-- Force loop guard to trigger after 2 successful iterations
-- This will cause n_this=0, which makes i stop advancing, triggering loop_guard > 3
SET smol.force_loop_guard_test = 2;

-- This CREATE INDEX should fail with "leaf build progress stalled"
BEGIN;
DO $$
BEGIN
    EXECUTE 'CREATE INDEX t_loop_guard_smol ON t_loop_guard USING smol(k)';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Expected error: %', SQLERRM;
END;
$$;
ROLLBACK;

-- Reset GUC
SET smol.force_loop_guard_test = 0;

-- Verify normal build works
CREATE INDEX t_loop_guard_smol ON t_loop_guard USING smol(k);
SELECT count(*) FROM t_loop_guard WHERE k > 500;

-- Cleanup
DROP TABLE t_loop_guard CASCADE;
