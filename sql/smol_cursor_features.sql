-- SMOL cursor limitations test
-- Documents what cursor operations work and which ones don't
-- SMOL only supports forward-only cursors; SCROLL cursors are broken

SET smol.key_rle_version = 'v2';

SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Test dataset: 20 rows for cursor operations
DROP TABLE IF EXISTS t_cursor CASCADE;
CREATE UNLOGGED TABLE t_cursor (k int4, v int4);
INSERT INTO t_cursor SELECT i, i*10 FROM generate_series(1, 20) i;
CREATE INDEX t_cursor_smol ON t_cursor USING smol(k) INCLUDE (v);
ANALYZE t_cursor;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;

-- ============================================================================
-- Test 1: Forward-only cursor (WORKS)
-- ============================================================================
BEGIN;
DECLARE c1 CURSOR FOR SELECT k, v FROM t_cursor WHERE k > 5 ORDER BY k;

FETCH FROM c1;
FETCH NEXT FROM c1;
FETCH FORWARD 3 FROM c1;

CLOSE c1;
COMMIT;

-- ============================================================================
-- Test 2: NO SCROLL cursor (WORKS - same as default)
-- ============================================================================
BEGIN;
DECLARE c2 NO SCROLL CURSOR FOR SELECT k FROM t_cursor ORDER BY k;

FETCH FROM c2;
FETCH FROM c2;
FETCH FORWARD 3 FROM c2;
MOVE FORWARD 2 IN c2;
FETCH FROM c2;

CLOSE c2;
COMMIT;

-- ============================================================================
-- Test 3: FETCH ALL (WORKS)
-- ============================================================================
BEGIN;
DECLARE c3 CURSOR FOR SELECT k FROM t_cursor WHERE k <= 5 ORDER BY k;

FETCH FORWARD 2 FROM c3;
FETCH ALL FROM c3;

CLOSE c3;
COMMIT;

-- ============================================================================
-- Test 4: WITH HOLD cursor (WORKS for forward scans)
-- ============================================================================
BEGIN;
DECLARE c4 CURSOR WITH HOLD FOR SELECT k FROM t_cursor WHERE k <= 5 ORDER BY k;

FETCH FROM c4;
FETCH FROM c4;

COMMIT;

-- Cursor still usable after commit (forward only)
FETCH FROM c4;
FETCH FROM c4;
FETCH FROM c4;

CLOSE c4;

-- ============================================================================
-- Test 5: FOR UPDATE cursor (WORKS but table updates fail)
-- ============================================================================
BEGIN;
DECLARE c5 CURSOR FOR SELECT k, v FROM t_cursor WHERE k IN (1, 2, 3) FOR UPDATE;

FETCH FROM c5;
FETCH FROM c5;
FETCH FROM c5;

CLOSE c5;
COMMIT;

-- ============================================================================
-- Test 6: FOR SHARE cursor (WORKS)
-- ============================================================================
BEGIN;
DECLARE c6 CURSOR FOR SELECT k, v FROM t_cursor WHERE k IN (4, 5, 6) FOR SHARE;

FETCH FROM c6;
FETCH FROM c6;
FETCH FROM c6;

CLOSE c6;
COMMIT;

-- ============================================================================
-- Test 7: Multiple concurrent forward cursors (WORKS)
-- ============================================================================
BEGIN;
DECLARE c7a CURSOR FOR SELECT k FROM t_cursor WHERE k <= 10 ORDER BY k;
DECLARE c7b CURSOR FOR SELECT k FROM t_cursor WHERE k > 10 ORDER BY k;

FETCH FROM c7a;
FETCH FROM c7b;
FETCH FROM c7a;
FETCH FROM c7b;

CLOSE c7a;
CLOSE c7b;
COMMIT;

-- ============================================================================
-- Test 8: WHERE CURRENT OF with DELETE (BROKEN - leaves stale index entries)
-- DELETE succeeds on heap but index still contains deleted rows
-- ============================================================================
BEGIN;
DECLARE c8 CURSOR FOR SELECT k, v FROM t_cursor WHERE k BETWEEN 16 AND 18;

FETCH FROM c8;
-- This DELETE will succeed but leave stale entries in the SMOL index
DELETE FROM t_cursor WHERE CURRENT OF c8 RETURNING k, v;

FETCH FROM c8;
CLOSE c8;
ROLLBACK;  -- Rollback to restore data

-- ============================================================================
-- Test 9: WHERE CURRENT OF with UPDATE (BROKEN - fails with error)
-- UPDATE fails because SMOL index is read-only
-- ============================================================================
BEGIN;
DECLARE c9 CURSOR FOR SELECT k, v FROM t_cursor WHERE k BETWEEN 11 AND 13;

FETCH FROM c9;
-- This UPDATE will FAIL with "smol is read-only: aminsert is not supported"
UPDATE t_cursor SET v = v + 1000 WHERE CURRENT OF c9 RETURNING k, v;

FETCH FROM c9;
CLOSE c9;
ROLLBACK;

-- ============================================================================
-- Test 10: SCROLL cursor with FETCH LAST (BROKEN - returns 0 rows)
-- ============================================================================
BEGIN;
DECLARE c10 SCROLL CURSOR FOR SELECT k, v FROM t_cursor WHERE k <= 10 ORDER BY k;

FETCH FIRST FROM c10;  -- Works
FETCH NEXT FROM c10;   -- Works
FETCH LAST FROM c10;   -- BROKEN: returns 0 rows instead of row 10

CLOSE c10;
COMMIT;

-- ============================================================================
-- Test 11: SCROLL cursor with FETCH PRIOR (PARTIALLY WORKS within same page)
-- ============================================================================
BEGIN;
DECLARE c11 SCROLL CURSOR FOR SELECT k, v FROM t_cursor WHERE k <= 10 ORDER BY k;

FETCH FIRST FROM c11;  -- Works
FETCH NEXT FROM c11;   -- Works
FETCH NEXT FROM c11;   -- Works
FETCH PRIOR FROM c11;  -- Works within same page, returns k=4 (should be k=2)

CLOSE c11;
COMMIT;

-- ============================================================================
-- Test 12: SCROLL cursor with FETCH BACKWARD (BROKEN)
-- ============================================================================
BEGIN;
DECLARE c12 SCROLL CURSOR FOR SELECT k FROM t_cursor ORDER BY k;

FETCH FORWARD 5 FROM c12;  -- Works
FETCH BACKWARD 2 FROM c12; -- BROKEN: returns wrong results

CLOSE c12;
COMMIT;

-- ============================================================================
-- Test 13: FETCH ABSOLUTE with negative offset (BROKEN)
-- ============================================================================
BEGIN;
DECLARE c13 SCROLL CURSOR FOR SELECT k, v FROM t_cursor WHERE k > 5 ORDER BY k;

FETCH ABSOLUTE 1 FROM c13;  -- Works (forward)
FETCH ABSOLUTE -1 FROM c13; -- BROKEN: returns 0 rows
FETCH ABSOLUTE -3 FROM c13; -- BROKEN: returns 0 rows

CLOSE c13;
COMMIT;

-- ============================================================================
-- Test 14: FETCH RELATIVE with negative offset (BROKEN)
-- ============================================================================
BEGIN;
DECLARE c14 SCROLL CURSOR FOR SELECT k, v FROM t_cursor WHERE k BETWEEN 8 AND 15 ORDER BY k;

FETCH ABSOLUTE 5 FROM c14;  -- Works
FETCH RELATIVE -2 FROM c14; -- BROKEN: returns wrong results

CLOSE c14;
COMMIT;

-- ============================================================================
-- Test 15: MOVE BACKWARD (BROKEN)
-- ============================================================================
BEGIN;
DECLARE c15 SCROLL CURSOR FOR SELECT k FROM t_cursor ORDER BY k;

MOVE FORWARD 10 IN c15;      -- Works
FETCH 1 FROM c15;            -- Works
MOVE BACKWARD 5 IN c15;      -- BROKEN
FETCH 1 FROM c15;            -- Shows incorrect position

CLOSE c15;
COMMIT;

-- ============================================================================
-- Test 16: Backward scan with ORDER BY DESC (WORKS for forward, fails for LAST)
-- ============================================================================
BEGIN;
DECLARE c16 SCROLL CURSOR FOR SELECT k FROM t_cursor ORDER BY k DESC;

FETCH FIRST FROM c16;  -- Works: returns k=20
FETCH NEXT FROM c16;   -- Works: returns k=19
FETCH LAST FROM c16;   -- BROKEN: returns 0 rows (should return k=1)

CLOSE c16;
COMMIT;

-- ============================================================================
-- Test 17: SCROLL cursor rewind (UNEXPECTED: rescan works!)
-- ============================================================================
BEGIN;
DECLARE c17 SCROLL CURSOR FOR SELECT k FROM t_cursor WHERE k <= 5 ORDER BY k;

-- Scan forward (works)
FETCH ALL FROM c17;

-- Try to rewind (appears to work)
MOVE BACKWARD ALL IN c17;

-- Re-scan (surprisingly works - cursor restarts)
FETCH ALL FROM c17;

CLOSE c17;
COMMIT;

-- ============================================================================
-- Test 18: INSERT after creating SMOL index (BROKEN - fails with error)
-- ============================================================================
-- This demonstrates that SMOL makes the entire table read-only
INSERT INTO t_cursor VALUES (100, 1000);

-- ============================================================================
-- Test 19: Verify empty result set cursor (works correctly)
-- ============================================================================
BEGIN;
DECLARE c19 CURSOR FOR SELECT k FROM t_cursor WHERE k > 999 ORDER BY k;

FETCH FROM c19;  -- Correctly returns 0 rows

CLOSE c19;
COMMIT;

-- ============================================================================
-- Test 20: INSENSITIVE cursor (accepts declaration, forward-only)
-- ============================================================================
BEGIN;
DECLARE c20 INSENSITIVE CURSOR FOR SELECT k FROM t_cursor WHERE k <= 5 ORDER BY k;

FETCH FROM c20;
FETCH ALL FROM c20;

CLOSE c20;
COMMIT;

-- Cleanup
DROP TABLE t_cursor CASCADE;
