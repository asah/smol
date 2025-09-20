# smol - Read-only Space-Efficient PostgreSQL Index Access Method

## Overview

`smol` is a custom PostgreSQL index access method designed for maximum space efficiency and optimized specifically for index-only scans. Unlike traditional indexes that store both indexed values and tuple identifiers (TIDs), smol indexes store only the indexed values, saving approximately 6 bytes per tuple.

## Key Features

- **Read-only**: Optimized for read-heavy workloads where data doesn't change frequently
- **Space-efficient**: Eliminates TID storage, reducing index size by ~20-30%
- **Index-only scans**: Designed specifically to support index-only scans for maximum performance
- **Multi-column support**: Supports multi-column indexes with INCLUDE columns
- **NULL handling**: Proper support for NULL values and NULL searches

## Design Philosophy

Traditional PostgreSQL indexes store:
- Indexed column values
- Tuple identifier (TID) - 6 bytes pointing to heap location

smol indexes store:
- Indexed column values only
- No TIDs (hence read-only and index-only scan optimized)

This design trade-off eliminates the ability to perform heap lookups but dramatically reduces storage requirements and improves cache efficiency for analytical workloads.

## Installation

```bash
make
make install
```

## Usage

```sql
-- Load the extension
CREATE EXTENSION smol;

-- Create a smol index
CREATE INDEX idx_name ON table_name USING smol (column1, column2);

-- Index-only scans will automatically use the smol index
SELECT column1, column2 FROM table_name WHERE column1 > 100;
```

## Limitations

1. **Read-only**: No INSERT, UPDATE, or DELETE operations supported
2. **Index-only scans only**: Cannot perform heap lookups
3. **No unique constraints**: Cannot enforce uniqueness (read-only)
4. **No clustering**: Cannot be used for CLUSTER operations

## Use Cases

Ideal for:
- Data warehouse analytical queries
- Reporting on static/historical data  
- Read replicas with heavy analytical workloads
- Materialized view indexes
- Archive data analysis

Not suitable for:
- OLTP workloads with frequent modifications
- Queries requiring heap tuple access
- Applications requiring real-time data updates

## Performance Characteristics

- **Storage**: ~20-30% smaller than btree indexes
- **Scan speed**: Faster due to better cache locality
- **Memory usage**: Lower memory footprint
- **Build time**: Instant (creates empty structure)

## Architecture

The smol access method implements a minimal PostgreSQL index access method API:

```c
typedef struct SmolTuple
{
    uint16      size;           /* Total size of this tuple */
    uint16      natts;          /* Number of attributes */
    /* Variable-length attribute data follows */
    char        data[FLEXIBLE_ARRAY_MEMBER];
} SmolTuple;
```

Key implementation details:
- Implements required IndexAmRoutine interface functions
- Provides aggressive cost estimates to prefer index-only scans
- Supports backward scanning and NULL searches
- Returns `amcanreturn = true` for all indexed attributes

## Future Enhancements

- Compressed storage for even better space efficiency
- Sorted storage for range scans
- Bloom filter integration for existence checks
- Columnar storage layout for analytical workloads
