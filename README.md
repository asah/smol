# smol — Read‑Only, Space‑Efficient PostgreSQL Index AM

This document captures the architecture and operational overview of the
smol index access method. Code‑level notes live in `AGENT_NOTES.md`.

Important
- One container name: smol. Always use/reuse a single container named
  `smol` for builds, tests, and benchmarks.
- Always run in Docker. Host toolchains are unsupported.

Overview
- Read‑only index AM optimized for index‑only scans on append‑only data.
- Stores only fixed‑width key values (no heap TIDs), improving density and
  cache locality.
- Supports ordered, backward, and parallel scans; no bitmap scans.

Architecture
- On‑disk
  - Metapage (blk 0): magic, version, nkeyatts, optional first‑key directory.
  - Data pages (blk ≥ 1): packed fixed‑width key payloads; one ItemId per tuple;
    no per‑tuple headers, null bitmaps, or TIDs.
- Build
  - Collect keys via `table_index_build_scan`, enforce no‑NULLs and fixed‑width.
  - Sort using opclass comparator (proc 1) honoring collation.
  - Initialize metapage and append data pages with `PageAddItem`.
  - Mark heap block 0 all‑visible and set VM bit; a synthetic TID (0,1) keeps
    the executor on index‑only paths.
- Scan
  - IOS‑only; requires `xs_want_itup`, else ERROR.
  - Forward/backward and parallel supported. For each match, materialize an
    index tuple (`xs_itup`) and return with a constant TID `(0,1)`.

Flags & Capabilities
- `amcanorder=true`, `amcanbackward=true`, `amcanparallel=true`.
- `amgetbitmap=NULL` (no bitmap scans).
- `amsearcharray=false`, `amsearchnulls=false`, `amcaninclude=false`.
- `amstrategies=5` (<, <=, =, >=, >), `amsupport=1` (comparator proc 1).
- `aminsert` ERROR (read‑only after build).

Limitations
- Read‑only; drop/recreate to change data.
- Fixed‑width key types only; no varlena (text, bytea, numeric).
- No NULL keys; no INCLUDE columns; not unique; not clusterable.
- Prototype: no WAL/FSM crash‑safety yet.

Build & Test (Docker, PostgreSQL 18 from source)
- Build image: `make dockerbuild` (builds the image used by the `smol` container).
- Use inside-container targets via a single container named `smol`:
  - Clean build + install: `docker exec -it smol make insidebuild`
  - Quick regression (build, start PG, run installcheck, stop PG):
    - `docker exec -it smol make insidecheck`
  - Start PostgreSQL (initdb if needed): `docker exec -it smol make insidestart`
  - Stop PostgreSQL: `docker exec -it smol make insidestop`
  - Benchmark (leaves PG running): `docker exec -it smol make insidebench-smol-btree-5m`

Usage
- `CREATE EXTENSION smol;`
- `CREATE INDEX idx_smol ON some_table USING smol (col1, col2);`
- Planner: favor IOS as usual; SMOL errors on non‑IOS paths; parallel scans
  are supported on large relations.

Tests & Benchmarks
- `docker exec -it smol make insidecheck` runs the regression suite and stops PG when done.
- `sql/` contains correctness/regression tests (kept short).
- `bench/` contains only benchmarks; stream output live via `docker exec -it smol make insidebench-smol-btree-5m` (use `insidestop` when finished).

Operator Classes
- int2_ops, int4_ops, int8_ops (fixed‑width only).

Roadmap
- Add WAL/FSM, tune costs, expand opclass coverage, validate DESC/multi‑col
  behavior, optional compression.
