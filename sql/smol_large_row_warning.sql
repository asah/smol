-- Test to trigger large row size warning (>250 bytes)
-- Uses multiple large INCLUDE columns to exceed the threshold

SET smol.key_rle_version = 'v2';
SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS smol;

-- Enable WARNING level messages
SET client_min_messages = warning;

DROP TABLE IF EXISTS t_large_row CASCADE;
CREATE UNLOGGED TABLE t_large_row (
    k int4,
    -- Create enough 16-byte UUID columns to exceed 250 bytes
    -- 250 bytes / 16 bytes = ~15.6, so use 16 INCLUDE columns
    -- Plus key (4 bytes) + IndexTuple header (~8 bytes) = ~268 bytes total
    v1 uuid,
    v2 uuid,
    v3 uuid,
    v4 uuid,
    v5 uuid,
    v6 uuid,
    v7 uuid,
    v8 uuid,
    v9 uuid,
    v10 uuid,
    v11 uuid,
    v12 uuid,
    v13 uuid,
    v14 uuid,
    v15 uuid,
    v16 uuid
);

INSERT INTO t_large_row
SELECT i,
    gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
    gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
    gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
    gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), gen_random_uuid()
FROM generate_series(1, 10) i;

-- This should trigger the WARNING about large row size
CREATE INDEX t_large_row_idx ON t_large_row USING smol(k)
    INCLUDE (v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16);

-- Verify the index works
SELECT count(*) FROM t_large_row WHERE k > 5;

-- Cleanup
DROP TABLE t_large_row CASCADE;
