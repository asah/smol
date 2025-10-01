# SMOL Performance Optimization: Run-Detection Overhead

## ⚠️ STATUS: IMPLEMENTED ✅

**This document describes a performance issue that has been RESOLVED.**

The run-detection optimization described herein was implemented in smol.c:1604-1605, 1701-1705, 1811-1815.

**Benchmark results (2025-10-01):**
- Before optimization: 178ms on 1M unique int4 range scan
- After optimization: ~15ms (vs BTREE ~13.5ms)
- **Improvement: 11.8x faster, now competitive with BTREE**

This document is retained for historical reference and architectural understanding.

---

## Original Problem Summary

SMOL was 13x slower than BTREE on unique-key range scans despite reading 79% fewer buffers (743 vs 3582). The bottleneck was CPU overhead from per-row run-detection logic that executed even when runs didn't exist.

---

## Current Architecture

### What is Run-Detection?

**Purpose:** For duplicate keys, SMOL caches the key value and INCLUDE columns across equal-key runs to avoid redundant memcpy operations. This is a performance win when data has duplicates.

**Example:**
```
Keys: [42, 42, 42, 42, 43, 43, 44, ...]
      └─── run 1 ────┘ └run 2┘
```

When scanning, instead of copying "42" four times, SMOL:
1. Detects the run spans rows 1-4
2. Copies "42" once
3. Reuses it for rows 2-4

### Current Implementation (Per-Row Overhead)

**Location:** smol.c:1755-1829 (forward scan), similar for backward scan

**Flow:**
```c
while (so->cur_off <= n)  // Loop over each row in page
{
    char *keyp = smol_leaf_keyptr(page, so->cur_off, so->key_len);

    // ... bounds checking ...

    if (scan->xs_want_itup)
    {
        if (!so->two_col)  // Single-column path
        {
            if (so->run_active && so->cur_off <= so->run_end_off)
            {
                /* Already in a run: reuse cached key */
            }
            else
            {
                /* NEW RUN: Detect boundaries */
                const char *k0 = keyp;

                // 1. Copy key to cache (4-8 bytes)
                memcpy(so->run_key, k0, so->run_key_len);  // ← OVERHEAD

                // 2. Check if page is RLE-encoded
                if (!smol_leaf_run_bounds_rle(page, ...))  // ← OVERHEAD
                {
                    // 3. For plain pages: scan forward to find run end
                    while (end < n)  // ← OVERHEAD ON UNIQUE DATA
                    {
                        const char *kp = smol_leaf_keyptr(page, end + 1, ...);
                        if (!smol_key_eq_len(k0, kp, ...))
                            break;
                        end++;  // This loop is pointless on unique data!
                    }
                }
                so->run_start_off = start;
                so->run_end_off = end;
                so->run_active = true;
            }
        }

        // 4. Finally: copy the key to output tuple
        uint32 row = (uint32) (so->cur_off - 1);
        smol_copy4(so->itup_data, keyp);  // The actual work

        // 5. Return one tuple
        scan->xs_itup = so->itup;
        so->cur_off++;
        return true;
    }
}
```

### Performance Impact on Unique Data

**For each of 500,000 rows scanned:**
1. ✓ `smol_leaf_keyptr()` - necessary
2. ✗ `memcpy(so->run_key, k0, 4)` - unnecessary (key changes every row)
3. ✗ `smol_leaf_run_bounds_rle()` - unnecessary (page is plain, not RLE)
4. ✗ `while (end < n) { smol_leaf_keyptr(); smol_key_eq_len(); }` - unnecessary (finds run length = 1)
5. ✓ `smol_copy4(so->itup_data, keyp)` - necessary

**Result:** ~60% of work is unnecessary overhead.

---

## Why This Happens

### Three Page Types

SMOL supports three on-disk formats:

1. **Plain (tag = n):** `[u16 n][key1][key2]...[keyN][inc1_block][inc2_block]...`
   - Used when: Unique keys or duplicates don't compress well
   - Run detection: POINTLESS (every run has length 1)

2. **Key-RLE (tag = 0x8001):** `[tag][nitems][nruns][key||u16 count]*`
   - Used when: Many duplicate keys and RLE saves space
   - Run detection: USEFUL (reads run boundaries from RLE structure)

3. **Include-RLE (tag = 0x8003):** `[tag][nitems][nruns][key||u16 count||inc1||inc2]*`
   - Used when: Keys AND includes are constant within runs
   - Run detection: USEFUL (includes stored per-run, not per-row)
   - Status: **Reader exists, writer NOT implemented**

### Current Code Doesn't Distinguish

The scan loop treats ALL pages the same:
- **RLE pages:** Run detection is beneficial (reads structure)
- **Plain pages with INCLUDEs:** Run detection is beneficial (caches constant includes)
- **Plain pages WITHOUT includes:** Run detection is PURE OVERHEAD

The code has `smol_leaf_is_rle(page)` to detect RLE, but doesn't use it to skip overhead.

---

## Proposed Optimization

### Goal

Skip run-detection on plain (non-RLE) pages with no INCLUDE columns.

### Strategy: Check Once Per Page

**Key insight:** Page type doesn't change within a page. Check once when entering the page, not on every row.

### Pseudocode

```c
while (BlockNumberIsValid(so->cur_blk))  // Outer loop: pages
{
    Buffer buf = ReadBuffer(idx, so->cur_blk);
    Page page = BufferGetPage(buf);
    uint16 n = smol_leaf_nitems(page);

    /* CHECK ONCE PER PAGE */
    bool page_needs_run_detection = (so->ninclude > 0 || smol_leaf_is_rle(page));

    while (so->cur_off <= n)  // Inner loop: rows within page
    {
        char *keyp = smol_leaf_keyptr(page, so->cur_off, so->key_len);

        if (scan->xs_want_itup)
        {
            /* CONDITIONAL RUN DETECTION */
            if (page_needs_run_detection && !so->two_col)
            {
                if (so->run_active && so->cur_off <= so->run_end_off)
                {
                    /* Reuse cached key/includes */
                }
                else
                {
                    /* Detect new run boundaries */
                    memcpy(so->run_key, keyp, so->run_key_len);

                    if (!smol_leaf_run_bounds_rle(page, ...))
                    {
                        /* Scan forward for plain-page runs */
                        while (end < n && smol_key_eq_len(...))
                            end++;
                    }
                    so->run_active = true;
                }
            }

            /* ALWAYS: Copy key to output */
            smol_copy4(so->itup_data, keyp);
            scan->xs_itup = so->itup;
            so->cur_off++;
            return true;
        }
    }

    /* Advance to next page */
    so->cur_blk = next_page;
}
```

### Benefits

**Plain pages without INCLUDEs (common case for unique data):**
- ✓ Skip `memcpy(run_key)` - 500k saved
- ✓ Skip `smol_leaf_run_bounds_rle()` - 500k saved
- ✓ Skip forward-scan loop - 500k saved

**Expected speedup:** 3-5x faster on unique data (from 178ms → ~40-60ms, competitive with BTREE's 13ms)

**RLE pages and pages with INCLUDEs:**
- No change - run detection still executes as before

---

## Implementation Challenges

### Code Structure Issues

The current scan loop has nested `if (scan->xs_want_itup)` blocks that make simple conditional insertion difficult:

```c
// Current structure (simplified)
if (scan->xs_want_itup)           // Line 1776
{
    if (!so->two_col)              // Line 1782
    {
        // Run detection here...   // Lines 1784-1818
    }

    // Tuple building here...       // Lines 1820+
}
```

**Challenge:** Changing `if (!so->two_col)` to `if (!so->two_col && condition)` creates brace-matching issues because:
1. The run-detection block and tuple-building are inside the same outer `if`
2. Adding condition to inner `if` requires restructuring the outer block
3. The function is 600+ lines with complex nesting

### Failed Attempt

```c
// Attempted:
if (scan->xs_want_itup && !so->two_col && (condition))  // Added condition
{
    // Run detection
}

// Problem: Tuple building is in SAME if block
// Need to split it into TWO separate blocks:

if (scan->xs_want_itup && !so->two_col && (condition))
{
    // Run detection
}  // ← Close here

if (scan->xs_want_itup)  // ← Reopen for tuple building
{
    // Tuple building
}
```

This creates an extra closing brace that unbalances the outer `while` loop structure.

---

## Recommended Implementation Approach

### Option 1: Add Flag to Scan Opaque (Simplest)

```c
typedef struct SmolScanOpaqueData
{
    ...
    bool page_skip_run_detection;  // Set once per page
    ...
} SmolScanOpaqueData;
```

**In page-entry logic (once per page):**
```c
Buffer buf = ReadBuffer(idx, so->cur_blk);
Page page = BufferGetPage(buf);

/* Set flag for this entire page */
so->page_skip_run_detection = (so->ninclude == 0 && !smol_leaf_is_rle(page));

while (so->cur_off <= n)  // Row loop
{
    ...
    if (scan->xs_want_itup)
    {
        if (!so->two_col && !so->page_skip_run_detection)  // ← Simple check
        {
            // Run detection...
        }
        // Tuple building...
    }
}
```

**Pros:**
- Minimal code change (one flag, one check)
- No brace restructuring needed
- Flag is cheap (checked per row, but just a bool)

**Cons:**
- Still checks flag per row (but much cheaper than current logic)

### Option 2: Separate Fast Path (Best Performance)

Create two separate row-loop implementations:

```c
if (so->ninclude == 0 && !smol_leaf_is_rle(page))
{
    /* Fast path: no run detection */
    while (so->cur_off <= n)
    {
        char *keyp = smol_leaf_keyptr(page, so->cur_off, so->key_len);
        smol_copy4(so->itup_data, keyp);
        scan->xs_itup = so->itup;
        so->cur_off++;
        return true;
    }
}
else
{
    /* Slow path: with run detection */
    while (so->cur_off <= n)
    {
        char *keyp = smol_leaf_keyptr(page, so->cur_off, so->key_len);

        if (!so->two_col)
        {
            /* Full run detection logic */
            ...
        }

        smol_copy4(so->itup_data, keyp);
        scan->xs_itup = so->itup;
        so->cur_off++;
        return true;
    }
}
```

**Pros:**
- Zero per-row overhead on fast path
- Clean separation of concerns
- Enables future SIMD vectorization of fast path

**Cons:**
- Code duplication (but can extract common parts to inline functions)
- Larger code size

---

## Expected Performance After Fix

### Benchmark Results (Projected)

**Q1: 1M unique int4, 50% selectivity range scan**
- Current: SMOL 178ms vs BTREE 13ms (13x slower)
- After fix: SMOL ~35-45ms vs BTREE 13ms (2-3x slower)
- Remaining gap: BTREE's superior prefetching and cache locality

**Thrash test: 5M rows, 10% selectivity, low shared_buffers**
- Current: SMOL 24ms vs BTREE 18ms (25% slower)
- After fix: SMOL ~12-15ms vs BTREE 18ms (20-30% FASTER!)
- Why faster: Smaller index (58MB vs 150MB) fits better in cache

### Break-Even Analysis

After optimization:
- **Unique data:** SMOL competitive (2-3x slower acceptable given space savings)
- **Duplicate data:** SMOL already competitive (run-detection helps)
- **Low memory:** SMOL wins (smaller index = better cache hit rate)

---

## Related Optimizations

### 1. Prefetching

BTREE benefits from PostgreSQL's built-in index prefetching. SMOL could add:
```c
if (BlockNumberIsValid(next_blk))
    PrefetchBuffer(idx, MAIN_FORKNUM, next_blk);
```

Currently SMOL only prefetches in parallel mode.

### 2. SIMD Tuple Materialization

Once fast path is isolated, could vectorize:
```c
/* Copy 8 keys at once using AVX2 */
_mm256_storeu_si256(dst, _mm256_loadu_si256(src));
```

### 3. Cost Model Tuning

`smol_costestimate()` (smol.c:2337) may overestimate scan cost. After fixing run-detection overhead, re-tune planner costs to match actual performance.

---

## Action Items

1. **Immediate:** Implement Option 1 (flag-based skip)
   - Add `page_skip_run_detection` to SmolScanOpaqueData
   - Set flag on page entry
   - Check flag before run detection
   - Test with bench-quick and bench-thrash

2. **Short-term:** Implement Option 2 (fast/slow paths)
   - Create `smol_scan_plain_page()` for fast path
   - Create `smol_scan_rle_or_include_page()` for slow path
   - Benchmark improvement

3. **Medium-term:** Add prefetching
   - Prefetch next leaf during scan
   - Measure impact on sequential scans

4. **Long-term:** Consider SIMD
   - Requires fast path isolation
   - Biggest win on very wide scans (millions of rows)

---

## References

- Run detection: smol.c:1784-1818 (forward), 1678-1704 (backward)
- RLE detection: smol.c:3083 `smol_leaf_is_rle()`
- Page formats: smol.c:3009-3176 (reader helpers)
- Scan entry: smol.c:1268 `smol_gettuple()`
