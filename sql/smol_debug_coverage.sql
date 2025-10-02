-- Test debug logging and additional coverage paths
-- Covers: debug logging, text upper bounds, backward scans with upper-bound-only

CREATE EXTENSION IF NOT EXISTS smol;

-- ============================================================================
-- PART 1: Debug Logging Coverage (lines 1809-1820 and others)
-- ============================================================================

-- Test with text column to trigger varlena logging
DROP TABLE IF EXISTS t_debug_text CASCADE;
CREATE UNLOGGED TABLE t_debug_text (s text COLLATE "C", v int4);
INSERT INTO t_debug_text SELECT 'key_' || lpad(i::text, 6, '0'), i FROM generate_series(1, 5000) i;
CREATE INDEX idx_debug_text ON t_debug_text USING smol(s);

-- Enable debug logging
SET smol.debug_log = on;
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Forward scan with text key (triggers debug logging for varlena)
SELECT count(*) FROM t_debug_text WHERE s >= 'key_001000';
SELECT s FROM t_debug_text WHERE s = 'key_002500';

-- Backward scan with text key
SELECT count(*) FROM t_debug_text WHERE s <= 'key_004000' ORDER BY s DESC;

-- Test with INCLUDE columns (triggers include debug logging)
DROP TABLE IF EXISTS t_debug_include CASCADE;
CREATE UNLOGGED TABLE t_debug_include (a int4, b int4, c int4);
INSERT INTO t_debug_include SELECT i, i*2, i*3 FROM generate_series(1, 1000) i;
CREATE INDEX idx_debug_include ON t_debug_include USING smol(a) INCLUDE (b, c);

-- Queries with INCLUDE columns (debug logging)
SELECT sum(b), sum(c) FROM t_debug_include WHERE a >= 500;
SELECT a, b, c FROM t_debug_include WHERE a = 750;

-- Test with text INCLUDE columns for varlena debug logging
DROP TABLE IF EXISTS t_debug_text_inc CASCADE;
CREATE UNLOGGED TABLE t_debug_text_inc (k int4, s text COLLATE "C");
INSERT INTO t_debug_text_inc SELECT i, 'val_' || i::text FROM generate_series(1, 500) i;
CREATE INDEX idx_debug_text_inc ON t_debug_text_inc USING smol(k) INCLUDE (s);

-- Query that returns text INCLUDE column (triggers include varlena logging)
SELECT k, s FROM t_debug_text_inc WHERE k BETWEEN 100 AND 110;

-- Disable debug logging for subsequent tests
SET smol.debug_log = off;

-- ============================================================================
-- PART 2: Text Upper Bounds with Generic Comparator (line 597)
-- ============================================================================

-- Text column with upper bound (backward scan)
DROP TABLE IF EXISTS t_text_upper CASCADE;
CREATE UNLOGGED TABLE t_text_upper (s text COLLATE "C");
INSERT INTO t_text_upper SELECT 'item_' || lpad(i::text, 5, '0') FROM generate_series(1, 10000) i;
CREATE INDEX idx_text_upper ON t_text_upper USING smol(s);

-- Upper bound only (backward scan) - uses generic comparator
SELECT count(*) FROM t_text_upper WHERE s <= 'item_05000';
SELECT count(*) FROM t_text_upper WHERE s < 'item_05000';

-- Forward scan with upper bound
SELECT count(*) FROM t_text_upper WHERE s <= 'item_08000';
SELECT count(*) FROM t_text_upper WHERE s < 'item_08000';

-- BETWEEN with text (exercises both bounds)
SELECT count(*) FROM t_text_upper WHERE s BETWEEN 'item_03000' AND 'item_07000';

-- Backward scan with both bounds
SELECT count(*) FROM (SELECT s FROM t_text_upper WHERE s BETWEEN 'item_02000' AND 'item_04000' ORDER BY s DESC) sub;

-- ============================================================================
-- PART 3: Backward Scan Starting at Upper Bound (lines 1356-1363)
-- ============================================================================

-- Single-column backward scan with upper bound only (no lower bound)
DROP TABLE IF EXISTS t_back_upper_only CASCADE;
CREATE UNLOGGED TABLE t_back_upper_only (a int4);
INSERT INTO t_back_upper_only SELECT i FROM generate_series(1, 10000) i;
CREATE INDEX idx_back_upper_only ON t_back_upper_only USING smol(a);

-- Direct backward scans with upper bound only (no BETWEEN, no lower bound)
-- Using LIMIT to encourage backward scan
SELECT a FROM t_back_upper_only WHERE a <= 5000 ORDER BY a DESC LIMIT 5;
SELECT a FROM t_back_upper_only WHERE a < 5000 ORDER BY a DESC LIMIT 5;

-- Upper bound at extreme
SELECT a FROM t_back_upper_only WHERE a <= 9999 ORDER BY a DESC LIMIT 10;

-- Very restrictive upper bound
SELECT a FROM t_back_upper_only WHERE a <= 100 ORDER BY a DESC LIMIT 10;

-- ============================================================================
-- PART 4: INT8 Backward Scan Paths (lines 1801-1803)
-- ============================================================================

-- INT8 key with backward scan (to hit line 1801: smol_copy8)
DROP TABLE IF EXISTS t_int8_back CASCADE;
CREATE UNLOGGED TABLE t_int8_back (a int8);
INSERT INTO t_int8_back SELECT i::int8 FROM generate_series(1, 5000) i;
CREATE INDEX idx_int8_back ON t_int8_back USING smol(a);

-- Enable debug logging for INT8 backward scans
SET smol.debug_log = on;

-- Direct backward scans on INT8 (triggers smol_copy8 in backward path)
SELECT a FROM t_int8_back WHERE a >= 2000 ORDER BY a DESC LIMIT 10;
SELECT a FROM t_int8_back WHERE a <= 4000 ORDER BY a DESC LIMIT 10;
SELECT a FROM t_int8_back WHERE a BETWEEN 1000 AND 3000 ORDER BY a DESC LIMIT 10;

-- INT8 with upper bound only (backward)
SELECT a FROM t_int8_back WHERE a < 3000 ORDER BY a DESC LIMIT 10;

SET smol.debug_log = off;

-- ============================================================================
-- PART 5: UUID Backward Scan (line 1802)
-- ============================================================================

-- UUID key with backward scan
DROP TABLE IF EXISTS t_uuid_back CASCADE;
CREATE UNLOGGED TABLE t_uuid_back (u uuid);
INSERT INTO t_uuid_back SELECT gen_random_uuid() FROM generate_series(1, 1000);
CREATE INDEX idx_uuid_back ON t_uuid_back USING smol(u);

-- Backward scan on UUID (16-byte key)
SELECT count(*) FROM (SELECT u FROM t_uuid_back ORDER BY u DESC LIMIT 100) sub;

-- ============================================================================
-- PART 6: Text Backward Scan with Varlena (line 1795)
-- ============================================================================

-- Text key backward scan (triggers smol_emit_single_tuple - line 1795)
DROP TABLE IF EXISTS t_text_back CASCADE;
CREATE UNLOGGED TABLE t_text_back (s text COLLATE "C");
INSERT INTO t_text_back SELECT 'text_' || i::text FROM generate_series(1, 2000) i;
CREATE INDEX idx_text_back ON t_text_back USING smol(s);

-- Enable debug logging to hit logging in backward text scans
SET smol.debug_log = on;

-- Direct backward scans on text (varlena handling - line 1795)
SELECT s FROM t_text_back WHERE s >= 'text_1000' ORDER BY s DESC LIMIT 10;
SELECT s FROM t_text_back WHERE s <= 'text_500' ORDER BY s DESC LIMIT 10;
SELECT s FROM t_text_back WHERE s BETWEEN 'text_0500' AND 'text_1500' ORDER BY s DESC LIMIT 10;

-- Upper bound only backward scan on text
SELECT s FROM t_text_back WHERE s < 'text_1000' ORDER BY s DESC LIMIT 10;

SET smol.debug_log = off;

-- ============================================================================
-- PART 7: Edge Cases for Additional Coverage
-- ============================================================================

-- INT2 with backward scan and upper bound
DROP TABLE IF EXISTS t_int2_back CASCADE;
CREATE UNLOGGED TABLE t_int2_back (a int2);
INSERT INTO t_int2_back SELECT (i % 20000)::int2 FROM generate_series(1, 5000) i;
CREATE INDEX idx_int2_back ON t_int2_back USING smol(a);

SELECT count(*) FROM (SELECT a FROM t_int2_back WHERE a <= 10000 ORDER BY a DESC) sub;

-- Two-column with upper bound and backward scan
DROP TABLE IF EXISTS t_twocol_upper CASCADE;
CREATE UNLOGGED TABLE t_twocol_upper (a int4, b int4);
INSERT INTO t_twocol_upper SELECT i % 100, i FROM generate_series(1, 5000) i;
CREATE INDEX idx_twocol_upper ON t_twocol_upper USING smol(a, b);

-- Two-column backward scan with upper bound
SELECT count(*) FROM (SELECT a, b FROM t_twocol_upper WHERE a <= 50 ORDER BY a DESC, b DESC) sub;

-- ============================================================================
-- Cleanup
-- ============================================================================

DROP TABLE t_debug_text CASCADE;
DROP TABLE t_debug_include CASCADE;
DROP TABLE t_debug_text_inc CASCADE;
DROP TABLE t_text_upper CASCADE;
DROP TABLE t_back_upper_only CASCADE;
DROP TABLE t_int8_back CASCADE;
DROP TABLE t_uuid_back CASCADE;
DROP TABLE t_text_back CASCADE;
DROP TABLE t_int2_back CASCADE;
DROP TABLE t_twocol_upper CASCADE;
