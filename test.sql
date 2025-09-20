-- Test script for smol index access method

-- Create test table
CREATE TABLE test_smol (
    id INTEGER,
    name TEXT,
    value NUMERIC
);

-- Insert some test data
INSERT INTO test_smol VALUES 
    (1, 'first', 10.5),
    (2, 'second', 20.0),
    (3, 'third', 30.25);

-- Try to create smol index
CREATE INDEX test_smol_idx ON test_smol USING smol (id, name);

-- Test index-only scan
EXPLAIN (ANALYZE, BUFFERS) 
SELECT id, name FROM test_smol WHERE id > 0;

-- Test that regular scans work
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM test_smol WHERE id = 2;

-- Clean up
DROP INDEX IF EXISTS test_smol_idx;
DROP TABLE test_smol;
