# Coverage Enforcement

## Overview

The `make coverage` target now **enforces strict coverage requirements**:

1. **100% code coverage** - All measured lines must be covered by tests
2. **Zero "Excluded covered" lines** - No covered code should be marked with GCOV_EXCL

## What are "Excluded covered" lines?

**"Excluded covered" lines are code lines that:**
- Are marked with `GCOV_EXCL_LINE` (or in `GCOV_EXCL_START/STOP` blocks)
- But are ACTUALLY covered by existing tests

**This is problematic because:**
- The code IS being tested
- But it's marked as excluded from coverage metrics
- This is wasteful - the exclusion marker should be removed!
- It misleads about what code is truly untestable

## How to check for "Excluded covered" lines

```bash
./scripts/calc_cov.sh -e
```

This will list all lines that are:
- Marked with GCOV_EXCL markers
- But covered by tests

## How to fix "Excluded covered" lines

1. **Run the diagnostic:**
   ```bash
   ./scripts/calc_cov.sh -e
   ```

2. **Review the listed lines** - Each line will show:
   - Line number
   - Whether it's in a GCOV_EXCL_START/STOP block or marked with GCOV_EXCL_LINE
   - The actual code

3. **Remove the GCOV_EXCL markers:**
   - For `GCOV_EXCL_LINE`: Remove the `/* GCOV_EXCL_LINE */` comment
   - For blocks: Remove the `/* GCOV_EXCL_START */` and `/* GCOV_EXCL_STOP */` markers

4. **Re-run coverage:**
   ```bash
   make coverage
   ```

## When should code be excluded?

**Only exclude code that is:**
- **Truly unreachable error paths** - Defensive checks that should never execute
- **Platform-specific code** - Code that only runs on certain platforms (e.g., byval vs byref variants)
- **Deprecated/dead code** - Old code paths that are kept for compatibility but no longer used
- **Intentionally disabled features** - Features disabled by default that require special setup

**Do NOT exclude code that:**
- Can be tested with the existing test infrastructure
- Just needs debug flags enabled (use debug-enabled tests instead)
- Is rare but reachable (write a test for it!)

## Make coverage behavior

### Success (exit code 0):
```
[coverage] ✓ Coverage is 100.00% (meets target)

[coverage] ✓ Coverage analysis complete!
```

### Failure: <100% coverage (exit code 1):
```
[coverage] ✗ ERROR: Coverage is 99.50%, target is 100.00%
```

### Failure: Excluded covered lines found (exit code 1):
```
[coverage] ✗ ERROR: Found 5 "Excluded covered" lines
[coverage]
[coverage] What are "Excluded covered" lines?
[coverage] ═══════════════════════════════════════════
[coverage] These are lines marked with GCOV_EXCL_LINE (or in GCOV_EXCL_START/STOP blocks)
[coverage] that are ACTUALLY covered by tests. This means:
[coverage]   • The code IS being tested
[coverage]   • But it is marked as excluded from coverage
[coverage]   • This is a waste - the exclusion marker should be removed!
[coverage]
[coverage] How to fix:
[coverage] ──────────
[coverage] 1. Run: ./scripts/calc_cov.sh -e
[coverage] 2. Review the listed lines with GCOV_EXCL markers
[coverage] 3. Remove the GCOV_EXCL_LINE, GCOV_EXCL_START, or GCOV_EXCL_STOP markers
[coverage] 4. Re-run: make coverage
[coverage]
[coverage] Why this matters:
[coverage] ────────────────
[coverage] Only truly untestable code should be excluded (e.g., unreachable error paths,
[coverage] platform-specific code). If tests already cover it, the exclusion is misleading.
```

## Current Status

As of 2025-10-12:
- **Coverage: 100.00%** (2812/2812 lines covered)
- **Excluded covered: 0** ✓
- **Excluded uncovered: 451** (legitimate exclusions)
- **All 77 regression tests passing** ✓

## Coverage Targets in CI/CD

Add this to CI/CD pipelines:

```bash
make coverage
```

This single command will:
1. Build with coverage instrumentation
2. Run all tests
3. Generate coverage report
4. **Fail if coverage < 100%**
5. **Fail if any covered code is excluded**

Exit code 0 = success, non-zero = failure.
