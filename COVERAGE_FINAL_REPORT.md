# SMOL Code Coverage - Final Report

## Summary

**Starting Point**: 95.31% (2153/2259 lines), 106 uncovered  
**Final Status**: 95.49% (2159/2261 lines), 102 uncovered  
**Improvement**: +6 lines covered (+0.18 percentage points)

## Tests Created & Added to Test Suite

### 1. ✅ smol_cost_nokey.sql (1 line)
**Covered**: Line 2701  
**Target**: Cost estimation fallback when query has WHERE clause on non-leading key  
**Method**: Created two-column index `SMOL(a, b)`, queried with `WHERE b > 500` (no constraint on leading key 'a')  
**Result**: Successfully triggered `qual_selec = 0.5` cost estimation path

### 2. ✅ smol_options_coverage.sql (2 lines)
**Covered**: Lines 2497, 2500  
**Target**: smol_options() function (AM options handler)  
**Method**: Created index `WITH (fillfactor=90)` to trigger options processing  
**Result**: Function called 5 times, both lines fully covered

### 3. ✅ smol_text32_toolong.sql (1 line partial)
**Target**: Line 4619 (text32 size validation error)  
**Actual**: Line 1092 covered instead (earlier validation)  
**Method**: Inserted 33-byte text value (exceeds 32-byte limit for text32)  
**Result**: Error triggered at build phase, not insert phase; gcov has issues tracking ERROR exits

### 4. ✅ smol_empty_table.sql (2 lines)
**Covered**: Lines 2840-2841 (empty index early return in smol_build_tree_from_sorted)  
**Target**: Line 3101 (empty check in smol_build_tree1_inc_from_sorted)  
**Method**: Created indexes on empty tables (int4, text, two-column, with INCLUDE)  
**Result**: Line 3101 determined UNREACHABLE - caller checks `n > 0` before calling function  
**Finding**: Lines 2840-2841 covered instead (19 executions)

## Analysis of Remaining 102 Uncovered Lines

### HARD - Parallel/Concurrency (40+ lines)
- **Lines 2486-2493** (4 lines): `smol_parallelrescan()` - requires parallel scan on inner side of nested loop with rescan
- **Lines 4352-4401** (38 lines): `smol_parallel_sort_worker()` - background worker function, requires actual parallel index build
- **Line 2568**: Multi-type operator family validation - needs cross-type function in same family
- **Lines 2780, 3004**: Slow lock timing logs - timing-dependent, defensive code

### MEDIUM - Conditional Edge Cases (30+ lines)
- **Lines 2239, 2259, 2267-2272, 2279-2283**: Parallel scan chunk claiming with prefetch
- **Lines 3152, 3363**: RLE run limit breaks (>32000 runs, page space limits)
- **Lines 3842-3844**: Buffer reallocation for large internal node fanout
- **Lines 3906, 3913, 4147**: Various loop break conditions

### ERROR PATHS - Defensive (15+ lines)
- **Line 3463**: Failed to add TEXT+INCLUDE leaf payload
- **Line 3546**: Unexpected byval key_len (defensive check)
- **Lines 3634, 3712**: Include-RLE multi-run errors
- **Line 4007**: Unexpected end of tuplesort stream
- **Line 4026**: Failed to add text leaf payload
- **Lines 4370, 4379, 4385**: Parallel worker DSM errors
- **Line 4619**: Text32 key exceeds bytes (different from line 1092)

### POTENTIALLY REACHABLE (10+ lines)
- **Lines 3517-3518, 4107-4113**: Rightmost descend in tree navigation
- **Line 3540**: int16 byval key path - requires 2-byte byval type that ISN'T int2
- **Line 3312**: Empty check in different build function
- **Line 3982**: Early return in two-column build

## Key Findings

1. **Fast paths bypass generic code**: int2/int4/int8 have optimized comparison paths, so generic byval comparators for these types are never used

2. **Caller-level early returns**: Many "easy" empty-table checks are unreachable because callers check for empty before calling the function

3. **Parallel worker code is hard to trigger**: Lines 4352-4401 require actual parallel index build with background workers, which needs specific configuration and dataset size

4. **Error paths don't show in gcov**: Functions that ereport(ERROR) exit abnormally, so gcov may not record the line as executed even when the error is triggered

5. **RLE is always used for text**: Lines 4511-4515 (non-RLE text copy) appear unreachable because SMOL always uses RLE for text keys

## Recommendations for Further Coverage

### Achievable (5-10 lines possible)
1. **Test rightmost descend**: Lines 3517-3518, 4107-4113 might be reachable with specific tree shapes
2. **Investigate line 3540 alternatives**: May need custom 2-byte byval type
3. **Error path testing with exception handling**: Wrap error-triggering code in PL/pgSQL exception blocks

### Difficult (requires significant effort)
1. **Parallel index build**: Enable parallel workers and create large enough dataset
2. **RLE edge cases**: Create scenarios with >32000 runs or specific page-filling patterns
3. **Multi-type validation**: Debug why line 2568 isn't hit even with multi-type families

### Likely Unreachable
1. **Line 3101**: Caller prevents call with n==0
2. **Line 3540**: No standard 2-byte byval type besides int2 (which has fast path)
3. **Line 2568**: May require different operator family structure

## Files Modified

- `Makefile`: Added 4 new tests to REGRESS list
- `sql/smol_cost_nokey.sql`: Cost estimation test (NEW)
- `sql/smol_options_coverage.sql`: Options handler test (NEW)
- `sql/smol_text32_toolong.sql`: Text size validation test (NEW)
- `sql/smol_empty_table.sql`: Empty table handling test (NEW)
- `expected/*.out`: Expected outputs for all new tests

## Conclusion

Achieved **95.49% coverage**, up from 95.31%. The remaining 102 uncovered lines are primarily:
- Parallel/concurrent execution paths (difficult to trigger)
- Defensive error handling (hard to test with gcov)
- Edge cases in RLE compression (specific data patterns needed)
- Unreachable code due to caller-level optimizations

Further progress toward 96%+ would require:
- Parallel index build testing infrastructure
- Custom type creation for specific code paths
- More sophisticated test data generation
- Alternative coverage tools that handle ERROR paths better

**Tests are stable and integrated into the regression suite.**
