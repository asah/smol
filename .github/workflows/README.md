# GitHub Actions CI Workflow

## Overview

Simple, fast CI using pre-built PostgreSQL packages from PGDG repository.

## Design Principles

1. **Use pre-built packages** - Install via apt-get from PGDG (no compilation!)
2. **Single job workflow** - Runs both production and coverage tests efficiently
3. **PostgreSQL 18** - Uses latest stable/beta from PGDG (extension requires PG 18 features)
4. **Fast execution** - typically completes in 2-3 minutes

## Job: Tests & Coverage

Single job that runs both production and coverage tests:

1. **Install PostgreSQL 18**
   - Adds PGDG repository for latest PostgreSQL packages
   - Tries `postgresql-18` first (beta/RC), falls back to 17 if unavailable
   - Installs development headers and lcov for coverage

2. **Build and test (production)**
   - Builds extension with `make`
   - Runs 3 production tests: smol_core, smol_scan, smol_rle

3. **Build and test (coverage)**
   - Rebuilds with coverage instrumentation (`COVERAGE=1`)
   - Runs all 6 tests (3 base + 3 coverage-only)
   - Verifies 100% coverage using `scripts/calc_cov.sh`
   - Uploads HTML coverage report as artifact

## Viewing Results

- **Test results**: Check the Actions tab in GitHub
- **Coverage reports**: Download from job artifacts
- **Build logs**: Click on failed jobs to see details
- **Test failures**: CI automatically displays `regression.diffs` on failure (via `SHOW_DIFFS=1`)

## Local Testing

Before pushing, verify locally:

```bash
# Production tests
make production

# Coverage tests
make coverage

# Should show "Coverage: 100.00%"
```

## Debugging and Fixes Applied

### Issue 1: EXPLAIN Cost Variability ✅ FIXED

**Problem**: Query plan costs varied between environments causing test failures
**Solution**: Use `EXPLAIN (COSTS OFF)` following PostgreSQL core standards
**Files Changed**: `sql/smol_coverage1.sql`, `sql/smol_coverage2.sql`, expected outputs
**Status**: Tests now pass ✓

### Issue 2: PostgreSQL Restart After Coverage Build ✅ FIXED

**Problem**: Coverage build install didn't reload shared library with test GUCs
**Solution**: Added `pg_ctlcluster restart` with proper `pg_isready` wait loop
**Why Needed**: Test GUCs only available in coverage-built library
**Files Changed**: `.github/workflows/ci.yml`
**Status**: Proper restart with 30s timeout + readiness check

### Issue 3: smol_advanced Test Timing 🔧 IN PROGRESS

**Problem**: Test expects 5000 rows (with fake upper bound) but gets 10001 in CI
**Root Cause**: Test GUC `smol.test_force_page_bounds_check` not activating in CI
**Status**: Works locally ✓, investigating CI-specific timing/loading issue
**Latest Fix**: Enhanced restart wait logic with diagnostic output

**Test Details**:
- Query: `SELECT count(*) FROM t_upper_bound_stop WHERE k > 0`
- Data: 5000 rows (1-5000) + 5001 rows (100000-105000) = 10001 total
- Expected: 5000 (fake upper bound at 10000 stops scan at page boundary)
- Actual (CI): 10001 (fake upper bound not applied)
- Actual (Local): 5000 ✓ (works correctly)

## Why This Approach?

**Previous attempts failed because:**
- Tried to compile PostgreSQL from source (6+ minutes)
- Used Ubuntu default PG 16 (SMOL requires PG 18 features like CompareType)
- Ran two separate jobs duplicating setup

**This works because:**
- PGDG repository provides pre-built PostgreSQL 18 packages (fast!)
- Single job runs both production and coverage tests (efficient)
- Dynamically detects installed PostgreSQL version
- Leverages ubuntu-latest's built-in gcc and package managers
- Uses `pg_createcluster` and `pg_ctlcluster` for cluster management
- Simple PG_CONFIG environment variable override in Makefile
- No Docker needed for CI (Docker still used for dev workflow)
- Auto-displays test diffs on failure via `SHOW_DIFFS=1`
