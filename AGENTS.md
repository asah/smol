# Repository Guidelines

## Project Structure & Module Organization
- Core source: `smol.c` (primary module built by PGXS).
- Extension SQL: `smol--1.0.sql` (handler + operator classes).
- Build tooling: `Makefile` (PGXS), `Dockerfile` (Ubuntu 24.04 + PostgreSQL 16 dev).
- Examples/tests: `*.sql` at repo root (e.g., `test.sql`, `debug_test.sql`, `final_brc_test.sql`, benchmarks). Docs in `*.md`.

## Build, Test, and Development Commands
- Local (requires PostgreSQL server headers):
  - `make` — build the extension.
  - `make install` — install into the `pg_config` target.
  - `make clean` — clean artifacts.
- With Docker (clean toolchain):
  - `docker build -t smol-dev .`
  - `docker run --rm -it -v "$PWD":/workspace smol-dev bash`
  - Inside container: `make && make install`
- Quick sanity check (running Postgres):
  - `psql -c "CREATE EXTENSION smol;"`
  - `psql -f test.sql`

## Coding Style & Naming Conventions
- Language: C (PostgreSQL server style). Use 4-space indents, K&R braces, and `snake_case` for functions/variables. Types/structs in CamelCaps (e.g., `SmolTuple`).
- Follow PG idioms: `palloc/pfree`, `ereport/elog`, Buffer/Page APIs. Keep helpers `static` unless required by `IndexAmRoutine`.
- Files: keep module in `smol.c`; extension script named `smol--X.Y.sql` on version bumps.

## Testing Guidelines
- Add small, deterministic SQL scripts at repo root. Name tests `<feature>_test.sql`; benchmarks `<scenario>_benchmark.sql`.
- Verify index-only behavior with `EXPLAIN (ANALYZE, BUFFERS)`: expect `Index Only Scan` using `smol`.
- Run examples: `psql -f debug_test.sql` and review output; avoid long benchmarks by default.

## Commit & Pull Request Guidelines
- Commits: imperative subject (≤72 chars) + concise body explaining what/why. Separate mechanical refactors from functional changes.
- PRs: clear description, linked issues, reproduction steps, and `EXPLAIN` before/after when planner/cost/canreturn behavior changes. Update docs/examples for new ops/opclasses.

## Security & Configuration Tips
- Target PostgreSQL 16. Ensure `PG_CONFIG` points to your installation (`export PG_CONFIG=$(which pg_config)`).
- The access method is read-only; do not route write paths through it.
