-- Backward scan tests - tests BackwardScanDirection paths
-- Uses cursors to force backward index scans
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = on;
SET enable_indexscan = off;
SET max_parallel_workers_per_gather = 0;

-- Test 1: Backward scan initialization with ORDER BY DESC (no bound)
DROP TABLE IF EXISTS t_back1 CASCADE;
CREATE UNLOGGED TABLE t_back1(k int4);
INSERT INTO t_back1 SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX t_back1_smol ON t_back1 USING smol(k);

-- ORDER BY DESC initializes scan with BackwardScanDirection
-- This triggers smol_rightmost_leaf() at line 1305
SELECT k FROM t_back1 ORDER BY k DESC LIMIT 10;

DROP INDEX t_back1_smol;
DROP TABLE t_back1;

-- Test 1b: Backward scan with FETCH BACKWARD from cursor
DROP TABLE IF EXISTS t_back1b CASCADE;
CREATE UNLOGGED TABLE t_back1b(k int4);
INSERT INTO t_back1b SELECT i FROM generate_series(1, 1000) i;
CREATE INDEX t_back1b_smol ON t_back1b USING smol(k);

-- Use cursor with backward fetch to trigger BackwardScanDirection
BEGIN;
DECLARE c1 CURSOR FOR SELECT k FROM t_back1b ORDER BY k;
FETCH LAST FROM c1;
FETCH BACKWARD 5 FROM c1;
FETCH BACKWARD 10 FROM c1;
COMMIT;

DROP INDEX t_back1b_smol;
DROP TABLE t_back1b;

-- Test 2: Backward scan with bound (WHERE k >= N and ORDER BY DESC)
DROP TABLE IF EXISTS t_back2 CASCADE;
CREATE UNLOGGED TABLE t_back2(k int4, v int4);
INSERT INTO t_back2 SELECT i, i*10 FROM generate_series(1, 5000) i;
CREATE INDEX t_back2_smol ON t_back2 USING smol(k) INCLUDE (v);

-- ORDER BY DESC with WHERE bound triggers rightmost_leaf with bound check (line 1302)
SELECT k, v FROM t_back2 WHERE k >= 1000 ORDER BY k DESC LIMIT 5;

-- Backward scan with cursor and lower bound
BEGIN;
DECLARE c2 CURSOR FOR SELECT k, v FROM t_back2 WHERE k >= 1000 ORDER BY k;
FETCH LAST FROM c2;
FETCH BACKWARD 3 FROM c2;
COMMIT;

-- Verify correctness by checking results
BEGIN;
DECLARE c3 CURSOR FOR SELECT k FROM t_back2 WHERE k >= 4990 ORDER BY k;
FETCH LAST FROM c3;
FETCH BACKWARD 1 FROM c3;
FETCH BACKWARD 1 FROM c3;
COMMIT;

DROP INDEX t_back2_smol;
DROP TABLE t_back2;

-- Test 3: Backward scan with int2 key (tests specific type paths)
DROP TABLE IF EXISTS t_back_int2 CASCADE;
CREATE UNLOGGED TABLE t_back_int2(k int2);
INSERT INTO t_back_int2 SELECT (i % 30000)::int2 FROM generate_series(1, 10000) i;

CREATE INDEX t_back_int2_smol ON t_back_int2 USING smol(k);

BEGIN;
DECLARE c4 CURSOR FOR SELECT k FROM t_back_int2 WHERE k >= 100::int2 ORDER BY k;
FETCH LAST FROM c4;
FETCH BACKWARD 5 FROM c4;
COMMIT;

DROP INDEX t_back_int2_smol;
DROP TABLE t_back_int2;

-- Test 4: Backward scan with two-column index
DROP TABLE IF EXISTS t_back_twocol CASCADE;
CREATE UNLOGGED TABLE t_back_twocol(k1 int4, k2 int4);
INSERT INTO t_back_twocol SELECT (i % 100)::int4, i::int4 FROM generate_series(1, 1000) i;

CREATE INDEX t_back_twocol_smol ON t_back_twocol USING smol(k1, k2);

BEGIN;
DECLARE c5 CURSOR FOR SELECT k1, k2 FROM t_back_twocol WHERE k1 >= 50 ORDER BY k1, k2;
FETCH LAST FROM c5;
FETCH BACKWARD 10 FROM c5;
COMMIT;

DROP INDEX t_back_twocol_smol;
DROP TABLE t_back_twocol;

-- Test 5: Endscan cleanup - test scan termination with buffer held
DROP TABLE IF EXISTS t_endscan CASCADE;
CREATE UNLOGGED TABLE t_endscan(k int4);
INSERT INTO t_endscan SELECT i FROM generate_series(1, 10000) i;

CREATE INDEX t_endscan_smol ON t_endscan USING smol(k);

-- Start scan but abort transaction to trigger endscan cleanup
BEGIN;
DECLARE c6 CURSOR FOR SELECT k FROM t_endscan WHERE k >= 5000;
FETCH 10 FROM c6;
-- Abort will trigger endscan with buffer still held
ABORT;

DROP INDEX t_endscan_smol;
DROP TABLE t_endscan;
