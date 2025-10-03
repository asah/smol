# Coverage Goal: 100.0%

**IMPORTANT**: The goal is 100.0% code coverage, not "good enough" percentages.

## Why 100% Matters

1. **CI Enforcement**: 100% coverage allows CI to require 100% codecov on all PRs
2. **Quality**: Every line is tested - no hidden bugs in untested code
3. **Maintenance**: Forces removal of dead code or proper testing of all paths
4. **No Excuses**: Either the code is reachable and tested, or it should be removed

## Strategy

1. **Test everything reachable** - Use creative testing, whitebox functions, etc.
2. **Mark truly unreachable code** - Use GCOV_EXCL only when mathematically/architecturally impossible
3. **Remove dead code** - If code can't be reached and isn't needed, delete it
4. **Complete partial implementations** - Like parallel build - either finish it or remove it

## Current Status

- Coverage: 93.97% (2306/2453 lines)
- Target: 100.0%
- Remaining: ~147 lines to cover or exclude

## Next Steps

1. Implement parallel build (integrate existing infrastructure)
2. Cover all remaining error paths
3. Cover all edge cases
4. Remove any truly dead code
