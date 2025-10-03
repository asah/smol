-- Test text varlena copy paths without RLE (lines 4511-4515)
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

DROP TABLE IF EXISTS t_text_norle CASCADE;
CREATE TABLE t_text_norle (k text COLLATE "C");

-- Insert distinct text values (no duplicates = no RLE)
INSERT INTO t_text_norle SELECT 'key_' || i::text FROM generate_series(1, 100) i;

-- Create index
CREATE INDEX t_text_norle_idx ON t_text_norle USING smol(k);

-- Scan the index (should trigger text varlena copy without RLE)
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT * FROM t_text_norle WHERE k >= 'key_50' LIMIT 5;

-- Cleanup
DROP TABLE t_text_norle CASCADE;
