-- smol_16byte_coverage.sql
-- Priority 1 coverage tests for previously excluded lazy paths
-- Target: Remove GCOV_EXCL from 16-byte key handling and other easy wins

-- ============================================================================
-- Test 1: UUID index (16-byte key type)
-- Target: smol_build.c:1747-1749 (case 16: in key extraction)
-- ============================================================================
DROP TABLE IF EXISTS t_uuid CASCADE;
CREATE UNLOGGED TABLE t_uuid(id uuid, data int);

-- Insert various UUID patterns
INSERT INTO t_uuid VALUES
    ('00000000-0000-0000-0000-000000000000', 1),
    ('12345678-1234-5678-1234-567812345678', 2),
    ('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 3),
    ('ffffffff-ffff-ffff-ffff-ffffffffffff', 4),
    (gen_random_uuid(), 5),
    (gen_random_uuid(), 6),
    (gen_random_uuid(), 7);

-- Insert duplicates to test RLE with 16-byte keys
INSERT INTO t_uuid
    SELECT '12345678-1234-5678-1234-567812345678'::uuid, i
    FROM generate_series(10, 100) i;

-- Create index on UUID column (16-byte fixed-length type)
CREATE INDEX t_uuid_smol ON t_uuid USING smol(id);

-- Verify index works
SELECT COUNT(*) FROM t_uuid WHERE id = '12345678-1234-5678-1234-567812345678'::uuid;
SELECT COUNT(*) FROM t_uuid WHERE id > '00000000-0000-0000-0000-000000000000'::uuid;
SELECT COUNT(*) FROM t_uuid WHERE id < 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid;

-- ============================================================================
-- Test 2: UUID in INCLUDE clause (16-byte INCLUDE column)
-- Target: smol_scan.c:223 (16-byte INCLUDE copy path)
-- ============================================================================
DROP TABLE IF EXISTS t_uuid_inc CASCADE;
CREATE UNLOGGED TABLE t_uuid_inc(k int4, uuid_col uuid, data text);

-- Insert data with UUID in INCLUDE
INSERT INTO t_uuid_inc VALUES
    (1, '00000000-0000-0000-0000-000000000000', 'row1'),
    (2, '12345678-1234-5678-1234-567812345678', 'row2'),
    (3, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 'row3'),
    (4, 'ffffffff-ffff-ffff-ffff-ffffffffffff', 'row4');

-- Add duplicates to trigger RLE with INCLUDE
INSERT INTO t_uuid_inc
    SELECT i, '12345678-1234-5678-1234-567812345678'::uuid, 'dup' || i
    FROM generate_series(5, 50) i;

-- Create index with UUID in INCLUDE clause (tests 16-byte INCLUDE copy)
CREATE INDEX t_uuid_inc_smol ON t_uuid_inc USING smol(k) INCLUDE (uuid_col);

-- Force index-only scan to ensure INCLUDE data is read
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- This should do index-only scan and copy 16-byte UUID from INCLUDE
SELECT k, uuid_col FROM t_uuid_inc WHERE k > 0 AND k < 10 ORDER BY k;
SELECT k, uuid_col FROM t_uuid_inc WHERE k = 2;
SELECT COUNT(*) FROM t_uuid_inc WHERE k > 0;

-- Verify with RLE (duplicates should trigger RLE + INCLUDE caching)
SELECT k, uuid_col FROM t_uuid_inc WHERE k BETWEEN 5 AND 15 ORDER BY k;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- ============================================================================
-- Test 3: V2 RLE format read path
-- Target: smol_scan.c:129 (skip continues_byte in V2 format)
-- ============================================================================
DROP TABLE IF EXISTS t_v2_rle CASCADE;
CREATE UNLOGGED TABLE t_v2_rle(k int4, data int);

-- Force V2 RLE format for this index
SET smol.key_rle_version = 'v2';

-- Insert data with heavy duplicates to trigger RLE
INSERT INTO t_v2_rle
    SELECT (i % 10), i FROM generate_series(1, 1000) i;

-- Build index with V2 format
CREATE INDEX t_v2_rle_smol ON t_v2_rle USING smol(k);

-- Scan the index to trigger V2 RLE read path (continues_byte skip)
SELECT COUNT(*) FROM t_v2_rle WHERE k = 5;
SELECT COUNT(*) FROM t_v2_rle WHERE k > 3 AND k < 8;
SELECT k, COUNT(*) FROM t_v2_rle GROUP BY k ORDER BY k;

-- Test backward scan with V2 RLE
SELECT smol_test_backward_scan('t_v2_rle_smol'::regclass);

-- Reset to default
RESET smol.key_rle_version;

-- ============================================================================
-- Test 4: TEXT32 overflow error path
-- Target: smol_build.c:2699 (error for oversized TEXT32 keys)
-- ============================================================================
DROP TABLE IF EXISTS t_text_overflow CASCADE;
CREATE UNLOGGED TABLE t_text_overflow(k text COLLATE "C");

-- Insert text data that exceeds 32 bytes
INSERT INTO t_text_overflow VALUES
    ('short'),
    ('exactly_32_bytes_12345678901234'),  -- 32 bytes
    (repeat('x', 33)),                     -- 33 bytes - should fail
    (repeat('y', 50));                     -- 50 bytes - should fail

-- Attempt to create index - should fail with clear error message
\set ON_ERROR_STOP 0
CREATE INDEX t_text_overflow_smol ON t_text_overflow USING smol(k);
\set ON_ERROR_STOP 1

-- Verify short text works
DELETE FROM t_text_overflow WHERE length(k) > 32;
CREATE INDEX t_text_overflow_smol ON t_text_overflow USING smol(k);
SELECT COUNT(*) FROM t_text_overflow WHERE k > '';

-- Test with exactly 32 bytes (should work)
DROP TABLE IF EXISTS t_text_32 CASCADE;
CREATE UNLOGGED TABLE t_text_32(k text COLLATE "C");
INSERT INTO t_text_32 VALUES
    (repeat('a', 32)),
    (repeat('b', 32)),
    (repeat('c', 30));
CREATE INDEX t_text_32_smol ON t_text_32 USING smol(k);
SELECT COUNT(*) FROM t_text_32 WHERE k >= repeat('a', 32);

-- ============================================================================
-- Test 5: MACADDR8 index (another 16-byte type)
-- Additional coverage for case 16 validation
-- ============================================================================
DROP TABLE IF EXISTS t_macaddr8 CASCADE;
CREATE UNLOGGED TABLE t_macaddr8(mac macaddr8, data int);

INSERT INTO t_macaddr8 VALUES
    ('00:00:00:00:00:00:00:00', 1),
    ('08:00:2b:01:02:03:04:05', 2),
    ('ff:ff:ff:ff:ff:ff:ff:ff', 3);

-- Insert duplicates
INSERT INTO t_macaddr8
    SELECT '08:00:2b:01:02:03:04:05'::macaddr8, i
    FROM generate_series(10, 50) i;

CREATE INDEX t_macaddr8_smol ON t_macaddr8 USING smol(mac);

SELECT COUNT(*) FROM t_macaddr8 WHERE mac = '08:00:2b:01:02:03:04:05';
SELECT COUNT(*) FROM t_macaddr8 WHERE mac > '00:00:00:00:00:00:00:00';

-- Test MACADDR8 in INCLUDE
DROP TABLE IF EXISTS t_macaddr8_inc CASCADE;
CREATE UNLOGGED TABLE t_macaddr8_inc(k int4, mac macaddr8);

INSERT INTO t_macaddr8_inc VALUES
    (1, '00:00:00:00:00:00:00:00'),
    (2, '08:00:2b:01:02:03:04:05'),
    (3, 'ff:ff:ff:ff:ff:ff:ff:ff');

CREATE INDEX t_macaddr8_inc_smol ON t_macaddr8_inc USING smol(k) INCLUDE (mac);

SET enable_seqscan = off;
SELECT k, mac FROM t_macaddr8_inc WHERE k > 0 ORDER BY k;
RESET enable_seqscan;

-- ============================================================================
-- Test 6: Mixed TEXT and UUID in INCLUDE (trigger smol_emit_single_tuple path)
-- Target: smol_scan.c:223 (16-byte INCLUDE copy in varwidth path)
-- ============================================================================
DROP TABLE IF EXISTS t_mixed_inc CASCADE;
CREATE UNLOGGED TABLE t_mixed_inc(k int4, txt text COLLATE "C", u uuid);

INSERT INTO t_mixed_inc VALUES
    (1, 'hello', '00000000-0000-0000-0000-000000000001'),
    (2, 'world', '00000000-0000-0000-0000-000000000002'),
    (3, 'test', '00000000-0000-0000-0000-000000000003'),
    (4, 'data', '00000000-0000-0000-0000-000000000004');

-- Create index with mixed TEXT and UUID in INCLUDE
-- This forces has_varwidth=true, which triggers smol_emit_single_tuple()
CREATE INDEX t_mixed_inc_idx ON t_mixed_inc USING smol(k) INCLUDE (txt, u);

-- Force index-only scan to ensure INCLUDE data is read
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- This should trigger smol_emit_single_tuple() which processes both:
-- - txt (text, goes through inc_is_text path)
-- - u (UUID, should go through 16-byte non-text path at line 223)
SELECT k, txt, u FROM t_mixed_inc WHERE k > 0 ORDER BY k;
SELECT COUNT(*) FROM t_mixed_inc WHERE k > 0;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- Done!
