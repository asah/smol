SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test synthetic copy functions by explicitly calling them from SQL
-- This ensures _PG_init and smol_run_synthetic_tests get covered by gcov

-- Call the synthetic test function to trigger coverage of:
-- - _PG_init() code (via explicit call)
-- - smol_run_synthetic_tests()
-- - smol_copy2, smol_copy16, smol_copy_small with various sizes
-- - smol_options() test
SELECT smol_test_run_synthetic();
