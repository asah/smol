-- Test TEXT32 indexes spanning multiple leaf pages
-- Targets uncovered lines 4099-4103 in smol_build_text_stream_from_tuplesort
-- (rightlink update for multi-page TEXT indexes)

CREATE EXTENSION IF NOT EXISTS smol;

-- Create table with enough TEXT data to span multiple leaf pages
-- With key_len=16 and ~8KB pages, we fit ~500 keys per page
-- Use 10000 rows to ensure many pages and trigger rightlink updates
DROP TABLE IF EXISTS t_text_multipage CASCADE;
CREATE UNLOGGED TABLE t_text_multipage (k text COLLATE "C");

-- Insert 10000 text keys with moderate length (10 bytes)
-- This will use cap=16, fitting ~500 keys per page = ~20 leaf pages
INSERT INTO t_text_multipage
SELECT 'key' || lpad(i::text, 6, '0')
FROM generate_series(1, 10000) i;

-- Create TEXT index WITHOUT INCLUDE to use smol_build_text_stream_from_tuplesort path
CREATE INDEX idx_text_multipage ON t_text_multipage USING smol(k);

-- Verify index works
SELECT COUNT(*) FROM t_text_multipage WHERE k >= 'key005000';

-- Cleanup
DROP TABLE t_text_multipage CASCADE;
