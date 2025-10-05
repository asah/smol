# SMOL Coverage Guide (Target: 100%)

This repository maintains 100% line coverage for all testable code paths in `smol.c`.
Coverage is enforced by process, tooling, and disciplined use of gcov exclusion markers
for code that is provably unreachable under PostgreSQL’s current planner/runtime.

## Philosophy

- Test everything that is reachable in normal builds and runs.
- If a branch is architecturally unreachable (e.g., planner will not produce a
  backward-order path for non-btree AMs), guard it with clear `GCOV_EXCL` markers
  or feature flags and document the rationale inline.
- Prefer deleting dead or outdated code over excluding it.
- Keep defensive checks executable; cover them via dedicated white-box helpers
  in `smol--1.0.sql` when practical.

## Quick Start

- Full cycle: `make coverage` (cleans, builds with instrumentation, runs tests, reports)
- Clean: `make coverage-clean`
- Instrumented build only: `make coverage-build`
- Run tests and flush `.gcda`: `make coverage-test`
- Text report: `make coverage-report` (writes `smol.c.gcov` and prints a summary)
- HTML report: `make coverage-html` (requires `lcov`) → `coverage_html/index.html`

Artifacts:
- `smol.c.gcov` — line-by-line coverage
- `coverage_uncovered.txt` — uncovered line numbers (excluding GCOV_EXCL blocks)
- `smol.gcno`, `smol.gcda` — compiler/runtime data files

## How We Reached 100%

1. Added targeted SQL-callable test hooks in `smol--1.0.sql` to hit tricky paths
   (e.g., `smol_test_backward_scan`, validation/error helpers, white-box finders).
2. Exercised error paths and edge cases through focused regression tests in `sql/`.
3. Marked genuinely unreachable planner-dependent or optional paths with gcov exclusions:
   - Backward-order initialization the planner never invokes for non-btree AMs.
   - Deep prefetch branches and parallel-build helpers when not enabled.
   Use `GCOV_EXCL_LINE` for a single line, or `GCOV_EXCL_START/STOP` for blocks.
4. Removed obsolete or dead code discovered by coverage analysis instead of excluding it.

## Diagnosing Gaps

- After `make coverage`, inspect:
  - `smol.c.gcov` for annotated source
  - `coverage_uncovered.txt` for a concise list of uncovered line numbers
- Classify each uncovered line:
  1) Reachable → add/extend a test in `sql/` or a helper in `smol--1.0.sql`.
  2) Planner/runtime unreachable → surround with `GCOV_EXCL` and add a brief comment why.
  3) Truly dead code → delete it.
- Optional helper: `analyze_coverage.sh` can assist with categorization.

## Maintaining 100%

- PR checklist:
  - All new branches covered by tests or justified with `GCOV_EXCL` and a one-line reason.
  - Run `make coverage` locally; verify the summary prints zero uncovered lines.
  - Keep exclusions minimal; prefer tests or deletion when feasible.
- When adding features that are environment-specific (e.g., parallel build):
  - Guard with a feature flag or a `GCOV_EXCL_START/STOP` block and document the enabling conditions.

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

