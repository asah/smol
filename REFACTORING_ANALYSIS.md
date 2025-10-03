# Analysis: Restructuring Unreachable Error Paths

## Categories of Unreachable Error Checks

### 1. Should Convert to Assertions (Programmer Errors)

#### Line 709: Invalid Include Count
```c
if (ninclude < 0)
    ereport(ERROR, (errmsg("invalid include count")));
```
**Analysis**: 
- `ninclude` comes from `index->rd_index->indnkeyatts` and `index->rd_att->natts`
- Both are uint16, so result can never be negative
- This is checking for internal corruption, not user error

**Recommendation**: Convert to `Assert(ninclude >= 0);`

#### Line 973: Unexpected Byval Typlen
```c
default: ereport(ERROR, (errmsg("unexpected byval typlen=%u", (unsigned) key_len)));
```
**Analysis**:
- PostgreSQL type system guarantees byval types are 1, 2, 4, or 8 bytes
- Only reachable if catalog is corrupted or type system is broken
- This is a defensive check for impossible condition

**Recommendation**: Convert to `Assert(false); /* unreachable */` with comment
- Alternatively: Remove the default case entirely and let compiler warn about unhandled cases

#### Line 1031: PageAddItem Failure (Two-Column)
```c
if (PageAddItem(...) == InvalidOffsetNumber)
    ereport(ERROR,(errmsg("smol: failed to add two-col row payload")));
```
**Analysis**:
- Protected by line 1026: `if (n_this == 0) ereport(ERROR, "row too large")`
- If we calculated `n_this > 0`, PageAddItem should succeed
- Failure would indicate page corruption or calculation bug

**Recommendation**: Convert to `Assert(offsetNumber != InvalidOffsetNumber);`
- Or restructure to combine checks: validate available space once

### 2. Should Be Testable with Restructuring

#### Lines 690, 702, 947: Type Validation Errors
```c
// Line 690
if (typlen <= 0 && atttypid != TEXTOID)
    ereport(ERROR, (errmsg("smol supports fixed-length key types or text(<=32B) only (attno=1)")));

// Line 702  
if (typlen <= 0)
    ereport(ERROR, (errmsg("smol supports fixed-length key types only (attno=2)")));
```

**Current Issue**: PostgreSQL planner validates operator classes before calling our build function

**Restructuring Options**:

**Option A: Move validation to `amvalidate` callback**
```c
// In smol_handler, add:
amroutine->amvalidate = smol_amvalidate;

// New function:
bool smol_amvalidate(Oid opclassoid)
{
    // Validate type compatibility HERE instead of in build
    // This gets called during CREATE OPERATOR CLASS
    // Makes the check testable via SQL
}
```
This would make the check reachable and properly testable!

**Option B: Add explicit type support function**
```c
// Check during ambuild via type catalog lookup before operator class check
// Would allow testing with custom operator classes
```

**Recommendation**: **Option A** - Implement `amvalidate` callback
- Makes error paths reachable
- Better error messages at CREATE OPERATOR CLASS time
- Aligns with PostgreSQL best practices
- **This is actually a code quality improvement!**

#### Lines 2567, 2626, 2633, 2637, 2663: Build-Time Page Overflow Errors

These are theoretically reachable with pathological data:
```c
Line 2567: "cannot fit any tuple on a leaf"  
Line 2626: "failed to add leaf payload (RLE)"
Line 2633: "leaf payload exceeds page size"
Line 2637: "failed to add leaf payload"
Line 2663: "leaf build progress stalled"
```

**Recommendation**: These should remain as `ereport(ERROR)` - they're legitimate error conditions
- To test: Would need to craft data that fills pages in specific ways
- Could add targeted unit tests with controlled page sizes
- Worth keeping as runtime errors for data validation

### 3. Should Remain Unchanged (Legitimate Runtime Errors)

#### Line 2251: Operator Class Lookup Failure
```c
elog(ERROR, "cache lookup failed for operator class %u", opclassoid);
```
**Reason**: Catalog corruption is possible in the field
**Keep as-is**: Runtime error check

### 4. Debug/Info Logging (Not Errors)

Lines 2168, 2183, 2273-2349: Debug and INFO logging
- Controlled by compile-time flags or log level
- Not error paths, just uncovered logging
- **No action needed**

## Recommended Refactoring Plan

### High Priority (Quality Improvements)

1. **Implement `amvalidate` callback** (Lines 690, 702, 947)
   - Moves type validation to proper PostgreSQL lifecycle phase
   - Makes validation errors testable
   - Better user experience (errors at CREATE OPERATOR CLASS, not CREATE INDEX)
   - Estimated effort: 2-4 hours

2. **Convert to Assertions** (Lines 709, 973, 1031)
   - Replaces defensive checks for impossible conditions
   - Makes code intent clearer
   - Reduces false coverage gaps
   - Estimated effort: 30 minutes

### Medium Priority (Coverage Improvement)

3. **Add Unit Tests for Page Overflow** (Lines 2567, 2626, 2633, 2637, 2663)
   - Create test cases with pathologically large rows
   - Requires test framework for controlled page sizes
   - Estimated effort: 4-6 hours

### Code Examples

#### Before: Unreachable Error
```c
if (ninclude < 0)
    ereport(ERROR, (errmsg("invalid include count")));
```

#### After: Clear Assertion
```c
Assert(ninclude >= 0);  /* natts from system catalog can't be negative */
```

#### Before: Type Validation in Build
```c
// In smolbuild(), unreachable due to planner checks
if (typlen <= 0 && atttypid != TEXTOID)
    ereport(ERROR, (errmsg("smol supports fixed-length key types or text(<=32B) only")));
```

#### After: Type Validation in amvalidate
```c
// New callback in smol_handler
static bool
smol_amvalidate(Oid opclassoid)
{
    HeapTuple classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    // ... lookup type ...
    
    if (typlen <= 0 && typid != TEXTOID)
    {
        ereport(ERROR,
                (errmsg("operator class \"%s\" uses unsupported type",
                        NameStr(classform->opcname)),
                 errdetail("SMOL supports fixed-length types or text(<=32B) only")));
    }
    // Now testable with: CREATE OPERATOR CLASS ... (raises error immediately)
    return true;
}
```

## Summary

| Line(s) | Current | Recommendation | Testability Gain | Quality Gain |
|---------|---------|----------------|------------------|--------------|
| 709 | `ereport(ERROR)` | `Assert()` | N/A (impossible) | ✓ Clear intent |
| 973 | `ereport(ERROR)` | `Assert(false)` | N/A (impossible) | ✓ Clear intent |
| 1031 | `ereport(ERROR)` | `Assert()` | N/A (protected) | ✓ Clear intent |
| 690,702,947 | `ereport(ERROR)` | Move to `amvalidate` | ✓✓✓ Testable via SQL | ✓✓✓ Better UX |
| 2567+ | `ereport(ERROR)` | Keep + add tests | ✓ Testable with effort | ✓ Validates data |

**Net Result**: 
- ~5-10 lines converted to assertions (clearer code, removes false coverage gaps)
- ~3 error paths moved to `amvalidate` (makes testable + improves UX)
- Coverage would improve by ~0.5-1% from assertions alone
- **More importantly**: Code becomes more idiomatic and maintainable

**Recommendation**: Start with assertions (quick win), then implement `amvalidate` (quality improvement that also helps coverage).
