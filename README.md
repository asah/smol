# smol — Simple Memory-Only Lightweight index

[![CI](https://github.com/asah/smol/actions/workflows/ci.yml/badge.svg)](https://github.com/asah/smol/actions/workflows/ci.yml)

SMOL is a read‑only, space‑efficient PostgreSQL index access method optimized for reporting queries on fixed-width columns. Think of SMOL as a space-optimized, read-only variant of BTREE for analytical workloads.

**Disclaimer**: While generally safe (read only!) and has an extensive test suite with 100% code coverage, please consider SMOL as an unsupported research prototype with highly variable performance characteristics, limited observability and other issues that limit its applicability for production use. For the foreseeable future, only self-hosted PostgreSQL deployments only (not AWS RDS/Aurora). The author is very experienced and open to commercialization on very generous terms, please contact to discuss.

## Performance

Depending on compression, SMOL indexes can be 20-99+% smaller than BTREE, while providing competitive performance. 

### When to Use SMOL

**Use SMOL when:**
- Memory-constrained environments (cloud, containers)
- Read-heavy workloads (data warehouses, reporting, analytics)
- Data with duplicate keys (compression excels)
- Fixed-width columns (int2/4/8, date, uuid, timestamp)
- Index size matters (backups, restores, cache efficiency)

**Use BTREE when:**
- Write-heavy workloads (SMOL is strictly read-only)
- Variable-length keys, strings without C collation, keys with NULL values
- Very low selectivity point queries (< 1%) - BTREE is faster for single record lookups. Also consider HASH indexes.

BRIN is another way to compress indexes but cannot support index-only scans. BRIN may be useful in cases when SMOL compression is not impressive, e.g. lots of unique keys, wide columns, etc. BRIN is production ready, while SMOL is still in development.


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

**Docker host**:
```bash
make dbuild          # Build container image
make dstart          # Start container + PostgreSQL
make dexec           # login to container
make dpsql           # one step login to container and run psql
```

**Local** (inside container or native):
```bash
make production      # (re)build optimized/non-debug version from scratch
make coverage        # Generate coverage report
make bench-quick     # quick benchmark test
make bench-full      # comprehensive benchmark test

make build           # Clean build + install
make start           # Start PostgreSQL
make stop            # Stop PostgreSQL
make installcheck    # Run tests
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

## How SMOL Works

SMOL uses a B-tree structure like nbtree, but optimized for read-only access:

1. **Read-only assumption**: No visibility checks, no MVCC overhead
2. **Columnar storage**: Attributes stored in column-major format within pages
3. **Metadata hoisting**: Per-tuple metadata stored once per page, not per tuple
4. **Adaptive compression**: Run-length encoding (RLE) for duplicate-heavy data, plain format for unique data
5. **Tuple caching**: Pre-built tuple structures reduce per-row overhead
6. **C collation**: Text treated as fixed-width binary data for efficient comparison
7. **Lookup and Scan Optimizations**: Several advanced techniques for optimizing query performance, including page prefetch, zone maps and bloom filters

### Design Decision: Plain Format vs Zero-Copy

During development, we explored a "zero-copy" format that pre-materialized IndexTuple structures on disk to avoid memcpy during scans. **This was abandoned** because:

1. PostgreSQL's index-only scan protocol requires tuple construction anyway
2. The per-tuple overhead doubled index size for narrow rows, hurting I/O and cache efficiency
3. Benchmarks showed no performance benefit (slightly slower due to extra I/O)

**Current approach**: SMOL uses plain format for unique data and RLE compression for duplicate-heavy data, chosen adaptively per page.

### Index Structure

```mermaid
graph TB
    subgraph "SMOL Index Structure"
        Meta["Metapage (blk 0)<br/>- Magic/version<br/>- Key metadata<br/>- Root block"]

        Root["Root Internal Node<br/>(if tree height > 1)"]

        L1["Leaf Page 1<br/>Plain Format"]
        L2["Leaf Page 2<br/>RLE Format"]
        L3["Leaf Page 3<br/>Plain Format"]

        Meta --> Root
        Root --> L1
        Root --> L2
        Root --> L3

        L1 --> L2
        L2 --> L3
    end

    subgraph "Leaf Page Formats"
        Plain["Plain Format<br/>Key1|Key2|...|KeyN<br/>Inc1|Inc2|...|IncN<br/>Minimal overhead"]

        RLE["RLE Format<br/>RunLen1|Key1|Inc1<br/>RunLen2|Key2|Inc2<br/>Compressed duplicates"]
    end

    L1 -.-> Plain
    L2 -.-> RLE
    L3 -.-> Plain

    style Meta fill:#e1f5ff
    style Root fill:#fff4e1
    style L1 fill:#e8f5e9
    style L2 fill:#ffe8e8
    style L3 fill:#e8f5e9
```

### Scan Process

```mermaid
flowchart TD
    Start([beginscan]) --> Init[Initialize scan state<br/>Parse scan keys]
    Init --> Rescan[rescan]

    Rescan --> CheckPar{Parallel<br/>scan?}
    CheckPar -->|Yes| ClaimBatch[Atomic claim batch<br/>of leaf pages]
    CheckPar -->|No| FindFirst[Find first leaf via<br/>binary search]

    ClaimBatch --> Position
    FindFirst --> Position[Position to start<br/>of scan range]

    Position --> Gettuple[gettuple]

    Gettuple --> CheckDir{Direction<br/>change?}
    CheckDir -->|Yes| ResetRun[Reset run state]
    CheckDir -->|No| CheckRun{In active<br/>run?}

    ResetRun --> CheckRun

    CheckRun -->|Yes| EmitFromRun[Emit from cached run<br/>O1 operation]
    CheckRun -->|No| NewRun[Detect new run<br/>Cache if beneficial]

    EmitFromRun --> BuildTuple[Build IndexTuple<br/>using cached data]
    NewRun --> ReadPage{Need next<br/>page?}

    ReadPage -->|Yes| Prefetch[Adaptive prefetch<br/>next N pages]
    ReadPage -->|No| EmitKey[Emit key + INCLUDE]

    Prefetch --> EmitKey
    EmitKey --> BuildTuple

    BuildTuple --> Filter{Runtime<br/>filters?}
    Filter -->|Pass| Return[Return tuple]
    Filter -->|Fail| Gettuple

    Return --> Gettuple

    Gettuple --> End{EOF or<br/>bound?}
    End -->|No| Gettuple
    End -->|Yes| EndScan([endscan])

    style Start fill:#e1f5ff
    style Gettuple fill:#fff4e1
    style EmitFromRun fill:#e8f5e9
    style Return fill:#e8f5e9
    style EndScan fill:#e1f5ff
```

### Compression Decision

```mermaid
flowchart TD
    Start([Collect page data]) --> Count[Count unique runs]

    Count --> Calc[Calculate ratio:<br/>unique_runs / total_items]

    Calc --> Check{Ratio <<br/>threshold?}

    Check -->|Yes, many dups| RLE[Use RLE format<br/>Store run lengths]
    Check -->|No, mostly unique| Plain[Use plain format<br/>No overhead]

    RLE --> Measure1[Measure actual<br/>compressed size]
    Plain --> Measure2[Measure plain size]

    Measure1 --> Compare{RLE size <<br/>plain size?}
    Measure2 --> Write2[Write plain page]

    Compare -->|Yes| Write1[Write RLE page]
    Compare -->|No| Fallback[Fallback to plain<br/>despite high dups]
    Fallback --> Write2
    Write1 --> Done([Page complete])
    Write2 --> Done

    style RLE fill:#ffe8e8
    style Plain fill:#e8f5e9
    style Write1 fill:#ffe8e8
    style Write2 fill:#e8f5e9
```







