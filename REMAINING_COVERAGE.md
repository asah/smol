# Remaining Coverage for 100%

Current: **95.09%** (2149/2260 lines)
Remaining: **111 uncovered lines**

## Breakdown by Category

### 1. **Parallel Scan** (15 lines)
- `smol_parallelrescan()` - rescan function for parallel scans
- `smol_parallel_sort_worker()` - parallel worker function
- Most parallel scan initialization/coordination code

**Why uncovered:** Requires parallel query execution which is hard to trigger reliably in tests.

**To cover:** Create test with `SET max_parallel_workers_per_gather = 2; SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0;` and large table scan.

### 2. **Build Edge Cases** (38 lines)
- Early return paths in build functions
- Switch statement branches for rare data types (int2/byval keys)
- RLE multi-run edge cases
- Text key building rare paths

**Why uncovered:** Require specific data patterns or types not used in current tests.

**To cover:**
- Add int2 (smallint) index test
- Test borderline cases where RLE metadata doesn't fit
- Test very long text keys that trigger hex logging

### 3. **Backward Scan Edge Cases** (12 lines)
- Prefetch buffer logic for backward scans
- PG_INT64_MIN bound handling
- Chunk claiming logic for parallel backward scans

**Why uncovered:** Specific backward scan scenarios with parallel execution or edge case bounds.

**To cover:** Test backward scans with int8 type using MIN bounds.

### 4. **Error Handling** (8 lines)
- Loop guard stall detection error
- Failed leaf payload errors
- Unexpected key_len error
- Tuplesort stream end error

**Why uncovered:** These are defensive errors that shouldn't happen in normal operation.

**To cover:** Would require intentionally creating corrupt/invalid states (may not be worth testing).

### 5. **Loop Guard / Stall Detection** (4 lines)
- Test-only code for loop guard triggered by `smol_force_loop_guard_test` GUC
- Forces a stall condition during build

**Why uncovered:** The GUC exists but test doesn't trigger it yet.

**To cover:** Add test that sets `smol_force_loop_guard_test = 1` during build.

### 6. **Debug Logging** (3 lines)
- Slow lock wait logging
- Build text key hex logging

**Why uncovered:** Only triggered during lock contention or specific debug scenarios.

**To cover:** Hard to trigger reliably; low priority.

### 7. **Options/Validation** (1 line)
- `smol_options()` return NULL path (already tested via Assert)

**Why uncovered:** Function always returns NULL, already covered by synthetic test.

**To cover:** Already effectively covered.

### 8. **Other** (28 lines)
- Miscellaneous break statements, continues, worker logging
- Cost estimation edge case
- Validation continue statement

**Why uncovered:** Mix of defensive code and rare branches.

## Easiest Path to Higher Coverage

**High-value, achievable targets:**

1. ✅ **Loop guard test** (4 lines) - Add GUC-based test in existing test file
2. ✅ **Int2/smallint test** (5-10 lines) - Add new test with int2 index
3. ✅ **Int8 backward scan** (12 lines) - Test backward scan with int8 MIN bounds
4. ✅ **Parallel scan test** (15 lines) - Force parallel scan with tuned GUCs

These 4 items could add ~40 lines, bringing coverage to **~96.8%**.

**Diminishing returns beyond this:**
- Error paths are defensive and hard to trigger
- Debug logging requires artificial lock contention
- Some build edge cases require very specific data patterns

## Recommendation

Target **96-97%** coverage as a realistic goal. Getting to 100% would require:
- Synthetic error injection
- Lock contention simulation
- Very complex test scenarios

The remaining 3-4% is mostly defensive code that's valuable to have but impractical to test comprehensively.
