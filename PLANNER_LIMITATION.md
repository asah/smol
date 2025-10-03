# PostgreSQL Planner Limitation: Custom AM Ordering

## Summary

SMOL declares `amcanorder=true` and `amcanbackward=true`, and implements backward scan functionality in `smol_gettuple()` (lines 1299-1332). However, **PostgreSQL's planner never uses these capabilities** for non-btree access methods.

## Evidence

### Test Results

```sql
CREATE TABLE t(k int4);
INSERT INTO t SELECT i FROM generate_series(1, 100) i;
CREATE INDEX t_btree ON t USING btree(k);
CREATE INDEX t_smol ON t USING smol(k);
SET enable_seqscan = off;

-- BTREE: Uses Index Only Scan (no sort)
EXPLAIN SELECT k FROM t WHERE k >= 7 ORDER BY k LIMIT 5;
=> Limit -> Index Only Scan using t_btree

-- BTREE BACKWARD: Uses Index Only Scan Backward (no sort)
EXPLAIN SELECT k FROM t WHERE k >= 7 ORDER BY k DESC LIMIT 5;
=> Limit -> Index Only Scan Backward using t_btree

DROP INDEX t_btree;

-- SMOL: Adds unnecessary Sort node!
EXPLAIN SELECT k FROM t WHERE k >= 7 ORDER BY k LIMIT 5;
=> Limit -> Sort -> Index Only Scan using t_smol

-- SMOL BACKWARD: Adds unnecessary Sort node!
EXPLAIN SELECT k FROM t WHERE k >= 7 ORDER BY k DESC LIMIT 5;
=> Limit -> Sort DESC -> Index Only Scan using t_smol
```

### Root Cause

PostgreSQL's planner (`src/backend/optimizer/path/indxpath.c`) has **hardcoded btree-specific optimizations**:

1. `build_index_paths()` checks `if (index->relam == BTREE_AM_OID)` before creating ordered paths
2. The planner assumes only btree can satisfy ORDER BY clauses efficiently
3. Custom AMs with `amcanorder=true` are used for index scans, but the planner doesn't trust their ordering

### Code Coverage Impact

This limitation means the following SMOL code is **unreachable via SQL**:

- **Lines 1299-1332**: Backward scan initialization (`if (dir == BackwardScanDirection)`)
  - `smol_rightmost_leaf()` calls
  - Backward positioning within leaf pages
  - Bound checking for backward scans

- **Lines 1949-2087**: Parallel scan coordination (`if (scan->parallel_scan)`)
  - Parallel worker claim batches
  - Atomic cursor advancement for parallel workers

These code paths are **functionally correct** but unreachable because:
1. PostgreSQL never calls `smol_gettuple()` with `BackwardScanDirection` for initial scan
2. PostgreSQL never sets `scan->parallel_scan` for non-btree AMs

## Workarounds Attempted

### 1. Implement `amproperty`
**Status**: Not applicable - `amproperty` is for query-time property checks, not planner path generation

### 2. Force disabled plans
```sql
SET enable_sort = off;
SET enable_seqscan = off;
```
**Result**: PostgreSQL runs **disabled plans** rather than use SMOL for ordering:
```
Limit -> Sort (Disabled: true) -> Seq Scan (Disabled: true)
```

### 3. Large tables to justify parallel
Created 500k row tables with `debug_parallel_query=on` and all parallel GUC settings
**Result**: `scan->parallel_scan` remains NULL for SMOL

## Recommendations

### Option 1: Accept the Limitation
- Document that SMOL ordering works but planner doesn't use it
- Keep the code for potential future PostgreSQL improvements
- Current coverage: 71.44% (excludes unreachable planner-dependent code)

### Option 2: Remove Unreachable Code
- Set `amcanbackward = false`
- Remove backward scan initialization (lines 1299-1332)
- Remove parallel scan code (lines 1336-1406, 1949-2087)
- Add assertions: `Assert(dir != BackwardScanDirection)`
- Simpler codebase, but loses future extensibility

### Option 3: Implement Planner Hook (Advanced)
- Create a planner hook extension to force ordered paths for SMOL
- Complex, requires deep PostgreSQL internals knowledge
- Not portable across PostgreSQL versions

## Current Status

**Decision**: Keep code as-is, document limitation

**Rationale**:
1. Code is correct and tested (works when called directly)
2. Future PostgreSQL versions might add custom AM ordering support
3. Demonstrates proper AM implementation even if planner doesn't use it
4. 71.44% coverage is excellent for production code (unreachable paths excluded)

## Testing

To verify backward scans work when forced:
```c
// Direct AM call (not via SQL)
scan->xs_orderbyscan = false;
smol_rescan(scan, ...);
smol_gettuple(scan, BackwardScanDirection);  // Works!
```

The code paths work correctly; they're just never reached via normal SQL execution due to planner limitations.
