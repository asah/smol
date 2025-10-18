# smol — Simple Memory-Only Lightweight index

SMOL is a read‑only, space‑efficient PostgreSQL index access method optimized for reporting queries on fixed-width columns.

**Status**: Research prototype - not suitable for production use. Self-hosted PostgreSQL deployments only (not AWS RDS/Aurora).

**Philosophy**: The ideas in SMOL may be best brought to mainstream use as enhancements to existing PostgreSQL features rather than as a standalone index type.

## Performance

SMOL indexes are **significantly smaller** than BTREE (often 50-80% reduction) while delivering competitive or superior query performance for the right workloads.

### When to Use SMOL

✅ **Use SMOL when:**
- Memory-constrained environments (cloud, containers)
- Read-heavy workloads (data warehouses, reporting, analytics)
- Data with duplicate keys (RLE compression excels here)
- Fixed-width columns (int2/4/8, date, uuid, timestamp)
- Index size matters (backups, restores, cache efficiency)

⚠️ **Use BTREE when:**
- Write-heavy workloads (SMOL is strictly read-only)
- Variable-length keys without C collation
- NULL values needed in index keys
- Very low selectivity point queries (< 1%)
- Every millisecond of latency matters


## Comparison with PostgreSQL Index Types

**SMOL vs BTREE**: SMOL trades write capability for significantly better space efficiency. Think of SMOL as a space-optimized, read-only variant of BTREE for analytical workloads.

**SMOL vs BRIN/HASH**: BRIN and HASH cannot satisfy range queries. SMOL supports full range scans and ordering like BTREE.


## How SMOL Works

SMOL uses a B-tree structure like nbtree, but optimized for read-only access:

1. **Read-only assumption**: No visibility checks, no MVCC overhead
2. **Columnar storage**: Attributes stored in column-major format within pages
3. **Metadata hoisting**: Per-tuple metadata stored once per page, not per tuple
4. **Adaptive compression**: Run-length encoding (RLE) for duplicate-heavy data, plain format for unique data
5. **Tuple caching**: Pre-built tuple structures reduce per-row overhead
6. **C collation**: Text treated as fixed-width binary data for efficient comparison

### Design Decision: Plain Format vs Zero-Copy

During development, we explored a "zero-copy" format that pre-materialized IndexTuple structures on disk to avoid memcpy during scans. **This was abandoned** because:

1. PostgreSQL's index-only scan protocol requires tuple construction anyway
2. The per-tuple overhead doubled index size, hurting I/O and cache efficiency
3. Benchmarks showed no performance benefit (slightly slower due to extra I/O)

**Current approach**: SMOL uses plain format (zero overhead) for unique data and RLE compression for duplicate-heavy data, chosen adaptively per page.

## Quick Start

### Prerequisites

Table must be read-only (or use an UNLOGGED copy). String columns must use C collation.

```sql
CREATE EXTENSION smol;

-- Create index
CREATE INDEX idx_name ON table_name USING smol (key_column)
  INCLUDE (other, columns);

-- CRITICAL: Run ANALYZE for optimal query plans
ANALYZE table_name;

-- Use the index
SELECT key_column, other, columns
FROM table_name
WHERE key_column >= some_value;
```

### Build & Test

**Docker** (recommended):
```bash
make dbuild          # Build container image
make dstart          # Start container + PostgreSQL
make dpsql           # Connect to psql
make installcheck    # Run regression tests
```

**Local** (inside container or native):
```bash
make build           # Clean build + install
make start           # Start PostgreSQL
make installcheck    # Run tests
make coverage        # Generate coverage report
make stop            # Stop PostgreSQL
```

## Capabilities & Limitations

### Supported
- Index-only scans (required)
- Forward and backward scans
- Parallel scans
- Range queries (<, <=, =, >=, >)
- Multi-column indexes (fixed-width columns only)
- INCLUDE columns (fixed-width types)
- Data types: int2/4/8, date, timestamp, timestamptz, uuid, float4/8, bool, oid, etc.

### Not Supported
- Write operations (strictly read-only)
- NULL values in index keys
- Bitmap scans
- Variable-length keys without C collation
- Index-only scans with heap lookups (SMOL requires IOS)

### Prototype Limitations
- No WAL logging (not crash-safe)
- No FSM (free space map)

## Use Cases

**SMOL excels at:**
- Time-series data with duplicate timestamps
- Dimension tables (many lookups on same keys)
- Event logs with categorical data
- Analytics dashboards (read-heavy, memory-constrained)
- Reporting databases (periodic rebuilds acceptable)

**Example workload**: 1M row time-series table with 50 distinct metric_ids, queried by metric_id for aggregation. SMOL's RLE compression dramatically reduces index size while maintaining fast scans.

## Documentation

- `AGENT_NOTES.md` - Detailed implementation notes for developers
- `COVERAGE_ENFORCEMENT.md` - Testing and coverage policy
- `bench/` - Benchmark suite and methodology
