# SMOL vs BTREE Memory Format Comparison

## Overview

This document compares the complete memory layout of SMOL index tuples versus PostgreSQL's standard BTREE index tuples, demonstrating why SMOL achieves significant space savings.

## SMOL Index Tuple Format

### SmolTuple Structure
```c
typedef struct SmolTuple
{
    uint16      size;           /* Total size of this tuple (2 bytes) */
    uint16      natts;          /* Number of attributes (2 bytes) */
    /* Variable-length attribute data follows with proper alignment */
    char        data[FLEXIBLE_ARRAY_MEMBER];
} SmolTuple;
```

### Memory Layout Example (int4 + text)
```
Offset  Size  Field           Example Value
------  ----  -----           -------------
0       2     size            28 (total tuple size)
2       2     natts           2 (two attributes)
4       4     attr1 (int4)    123 (aligned to 4-byte boundary)
8       20    attr2 (text)    "hello world" (aligned + length header)
------
Total: 28 bytes
```

### Key Features
- **No TID storage**: Saves 6 bytes per tuple (no heap pointers)
- **No heap access needed**: For index-only scans
- **Minimal header**: Only 4 bytes (size + natts) 
- **Direct value storage**: Attributes stored with native alignment
- **Instant visibility**: Records visible immediately (no MVCC checking)

## BTREE Index Tuple Format

### IndexTuple Structure (PostgreSQL Internal)
```c
typedef struct IndexTupleData
{
    ItemPointerData t_tid;      /* TID of heap tuple (6 bytes) */
    unsigned short  t_info;     /* Various flags (2 bytes) */
    /* Variable-length attribute data follows */
    char           t_data[FLEXIBLE_ARRAY_MEMBER];
} IndexTupleData;
```

### Memory Layout Example (int4 + text)
```
Offset  Size  Field               Example Value
------  ----  -----               -------------
0       4     t_tid.ip_blkid      Block number in heap
4       2     t_tid.ip_posid      Position within block
6       2     t_info              Flags (natts, etc.)
8       4     attr1 (int4)        123
12      20    attr2 (text)        "hello world"
------
Total: 36 bytes
```

### Additional BTREE Process Overhead
- **Page headers**: ~24 bytes per 8KB page
- **Item pointers**: 4 bytes per tuple (pointing to tuple within page)
- **Special space**: BTREE-specific page metadata
- **Heap access required**: Must fetch heap tuples for MVCC visibility checking
- **TID lookup cost**: Additional I/O to resolve heap tuple locations

## Memory Comparison

### Per-Tuple Comparison
| Component              | SMOL    | BTREE   | Savings |
|------------------------|---------|---------|---------|
| TID (heap pointer)     | 0 bytes | 6 bytes | 6 bytes |
| Tuple header           | 4 bytes | 8 bytes | 4 bytes |
| Attribute data         | N bytes | N bytes | 0 bytes |
| **Total per tuple**    | N+4     | N+14    | **10 bytes** |

### Space Efficiency Formula
```
SMOL size = 4 + sum(aligned_attribute_sizes)
BTREE size = 14 + sum(aligned_attribute_sizes) + page_overhead

Space savings per tuple = 10 bytes + (page_overhead / tuples_per_page)
```

## Real-World Example: Weather Station Data

### Schema
```sql
CREATE TABLE measurements (
    station_id INT2,      -- 2 bytes (weather station ID)
    temperature INT2,     -- 2 bytes (temperature * 10 for precision)
    timestamp INT4        -- 4 bytes (Unix timestamp)
);
```

### Memory Layout Comparison

#### SMOL Tuple (12 bytes total)
```
Offset  Size  Field
------  ----  -----
0       2     size = 12
2       2     natts = 3  
4       2     station_id (int2)
6       2     temperature (int2) 
8       4     timestamp (int4)
------
Total: 12 bytes per record
```

#### BTREE Tuple (22 bytes total)
```
Offset  Size  Field
------  ----  -----
0       6     t_tid (heap pointer)
6       2     t_info (flags)
8       2     station_id (int2)
10      2     temperature (int2)
12      4     timestamp (int4) 
16      4     alignment padding
------
Total: 22 bytes per record
```

### Billion Record Analysis

For 1 billion weather records:

| Index Type | Bytes/Record | Total Size | Storage Saved |
|------------|--------------|------------|---------------|
| BTREE      | 22 bytes     | 20.5 GB    | -             |
| SMOL       | 12 bytes     | 11.2 GB    | **9.3 GB**    |

**SMOL saves 45% space (9.3 GB) for billion-record dataset!**

## Performance Implications

### Index-Only Scans
- **SMOL**: Direct value return, no heap access needed at all
- **BTREE**: Must access heap tuples to check MVCC visibility (xmin/xmax/etc)

### Query Performance  
- **SMOL**: Smaller index = better cache utilization
- **SMOL**: Zero heap I/O for index-only queries
- **SMOL**: Reduced storage footprint = fewer disk reads
- **BTREE**: Always requires heap access for visibility checking

### MVCC Behavior
- **SMOL**: Instant visibility, no transaction isolation
- **BTREE**: Full MVCC support via heap tuple visibility info (xmin/xmax/cmin/cmax)

### Limitations
- **SMOL**: No UPDATE/DELETE support (drop index required)
- **SMOL**: No transaction isolation (not ACID compliant for visibility)
- **SMOL**: No unique constraints enforcement

## Use Cases

### Ideal for SMOL
- Data warehousing / OLAP workloads
- Time-series data with append-only patterns
- Log aggregation systems
- Read-heavy reporting databases
- IoT sensor data collection

### Better with BTREE
- OLTP systems requiring ACID compliance
- Frequent UPDATE/DELETE operations
- Complex transaction isolation requirements
- Systems requiring unique constraints

## Conclusion

SMOL achieves significant space savings (45%+ for typical workloads) by eliminating TID storage and MVCC overhead, making it ideal for read-heavy, append-only workloads where space efficiency and index-only scan performance are critical.
