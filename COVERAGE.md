# SMOL Coverage Guide

## COVERAGE POLICY: 100% REQUIRED

**We demand 100.0% code coverage including all edge cases.**

The whole point of automated testing is to test everything, including edge cases.
Coverage is calculated EXCLUDING only:
1. Code between `/* GCOV_EXCL_START */` and `/* GCOV_EXCL_STOP */` markers
2. Lines marked with `/* GCOV_EXCL_LINE */` with clear justification
3. Deprecated functions marked with `pg_attribute_unused()`

**Current Status: 94.06% coverage - NEEDS IMPROVEMENT TO 100%**
- Covered: 2503 / 2661 lines
- **Remaining uncovered: 158 lines** (must be tested or marked with GCOV_EXCL)

## Allowed GCOV_EXCL Usage

GCOV_EXCL is ONLY allowed for:
1. **Deprecated functions** kept for reference (mark entire function)
2. **Assertions that can provably never fire** (e.g., `Assert(x > 0)` after `if (x <= 0) return`)
3. **Debug logging** inside `if (smol_debug_log)` blocks (not executed in tests)

NOT allowed:
- ❌ Edge cases (MUST be tested)
- ❌ Error paths (MUST be tested)
- ❌ Backward scans (MUST be tested)
- ❌ Runtime key filtering (MUST be tested)
- ❌ "Hard to test" code (write the test anyway)

## Philosophy

- **Test everything** - 100% coverage is the goal
- Edge cases MUST be tested (that's the whole point of automated testing)
- Error paths MUST be tested
- Only exclude code that is genuinely untestable:
  - Deprecated functions (should be deleted eventually)
  - Assertions that provably can never fire
  - Debug-only code not executed in tests
- Prefer deleting dead code over excluding it
- Write tests for "hard to test" code instead of excluding it

## Quick Start

Build and run coverage analysis:
```bash
make clean
make COVERAGE=1
sudo make install COVERAGE=1
make installcheck
gcov smol.c
```

Calculate **effective coverage** (excluding GCOV_EXCL blocks):
```bash
awk '
/GCOV_EXCL_START/ { excl=1; next }
/GCOV_EXCL_STOP/ { excl=0; next }
excl { next }
/^ *-:/ { next }
/^ *#####:.*GCOV_EXCL/ { next }
/^ *#####:/ { uncov++ }
/^ *[0-9]+:/ { cov++ }
END {
    total = cov + uncov
    if (total > 0) {
        pct = (cov * 100.0) / total
        printf "Effective Coverage: %.2f%% (%d covered / %d total)\n", pct, cov, total
    }
}' smol.c.gcov
```

Or use the automated Makefile target:
```bash
make coverage        # Complete workflow with exclusion-aware reporting
make coverage-report # Text report excluding GCOV_EXCL blocks
make coverage-html   # HTML report (requires lcov)
```

Artifacts:
- `smol.c.gcov` — line-by-line coverage
- `coverage_uncovered.txt` — uncovered line numbers (excluding GCOV_EXCL blocks)
- `smol.gcno`, `smol.gcda` — compiler/runtime data files

## How We Achieved 94%+ Coverage

1. Created comprehensive regression tests in `sql/` covering all critical paths:
   - RLE compression with 65K item boundary (smol_rle_65k_boundary.sql)
   - Parallel scans with various types (smol_parallel_*.sql)
   - Edge cases and error paths (smol_edge_*.sql, smol_error_paths.sql)
   - Coverage gap tests (smol_coverage_gaps.sql)

2. Marked intentionally uncovered code with GCOV_EXCL:
   - Deprecated functions (smol_build_tree_from_sorted - 346 lines)
   - Debug logging code (not executed in production)
   - Hard-to-test planner paths (specific backward scan combinations)
   - Edge cases covered by defensive programming

3. Focused on production code paths:
   - All RLE compression paths covered
   - All scan formats covered (RLE, zero-copy)
   - All parallel scan modes covered
   - All boundary conditions tested

## Diagnosing Gaps

- After `make coverage`, inspect:
  - `smol.c.gcov` for annotated source
  - `coverage_uncovered.txt` for a concise list of uncovered line numbers
- Classify each uncovered line:
  1) Reachable → add/extend a test in `sql/` or a helper in `smol--1.0.sql`.
  2) Planner/runtime unreachable → surround with `GCOV_EXCL` and add a brief comment why.
  3) Truly dead code → delete it.
- Optional helper: `analyze_coverage.sh` can assist with categorization.

## What Code is Currently Excluded

### 1. Deprecated Functions (346 lines) - ACCEPTABLE
- `smol_build_tree_from_sorted()` (lines 3645-3992)
- Legacy single-column build replaced by RLE two-pass
- Marked with `GCOV_EXCL_START`/`STOP`
- **Action: Should be deleted eventually**

### 2. Debug Logging Code (~50 lines) - ACCEPTABLE
- Code inside `if (smol_debug_log)` blocks (not executed in test suite)
- Code inside `if (so->prof_enabled)` blocks (not executed in test suite)

### 3. Edge Cases (~100 lines) - **NOT ACCEPTABLE, NEEDS TESTS**
Currently marked with `GCOV_EXCL_LINE` but MUST be tested:
- ❌ Empty page detection (lines 940, 944) → Need test
- ❌ Zero-copy upper bound check (line 952) → Need test
- ❌ Equality bound stop scan (lines 977-978) → Need test
- ❌ Empty two-column index (line 1486) → Need test
- ❌ Runtime key filtering (lines 2042, 2047) → Need test
- ❌ NULL key rejection (lines 2052-2054) → Need test
- ❌ Backward scan paths (lines 2693-2694, 2714-2715, 2719, 2722) → Need tests

**TODO: Remove GCOV_EXCL_LINE from these and write proper tests**

## Maintaining 100% Coverage

PR checklist:
- **All code paths must be covered by tests** (edge cases, error paths, everything)
- Only mark code with GCOV_EXCL if:
  - It's a deprecated function (should be deleted later)
  - It's an assertion that provably can never fire
  - It's debug logging not executed in tests
- Run `make coverage` and verify **100.0% effective coverage**
- **Always report effective coverage** (excluding GCOV_EXCL)
- If coverage drops below 100%, either:
  1. Write tests to cover the new code
  2. Justify GCOV_EXCL with clear explanation (and expect pushback)

## Common Techniques

- White-box SQL hooks: expose minimal C helpers via `smol--1.0.sql` to drive paths the executor rarely reaches.
- Defensive checks: keep them executable and cover via targeted tests; only exclude if truly impossible.
- Planner-dependent behavior: if PostgreSQL cannot generate the path today, exclude with a rationale and re-evaluate each major PG release.

## Troubleshooting

- Coverage summary shows missing lines but `smol.c.gcov` not present:
  - Ensure `make coverage-test` ran to completion and the server was stopped to flush `.gcda`.
- HTML report empty:
  - Install `lcov` in the container (`make coverage-html` will attempt to apt-get install).
- Counts differ between runs:
  - Clean artifacts first: `make coverage-clean`.

