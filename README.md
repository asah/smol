# smol — Simple Memory-Only Lightweight
# (a read‑only, space‑efficient PostgreSQL index access method)

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
 - Supports ordered and backward scans; no bitmap scans. Parallel planned
   (flag exposed; shared-state chunking not implemented yet).

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
  - Forward/backward supported (serial scans). For each match, materialize an
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
- Planner: favor IOS as usual; SMOL errors on non‑IOS paths. Parallel scans
  are supported, but see notes below on deterministic correctness testing.

Tests & Benchmarks
- `docker exec -it smol make insidecheck` runs the regression suite and stops PG when done.
- `sql/` contains correctness/regression tests (kept short).
- `bench/` contains only benchmarks; stream output live via `docker exec -it smol make insidebench-smol-btree-5m` (use `insidestop` when finished).
- Bench scripts enforce hard client timeouts (`TIMEOUT_SEC`, `KILL_AFTER`) and
  set a server-side `statement_timeout` slightly below `TIMEOUT_SEC` to avoid
  lingering backends when the client is killed by timeout.
 - Parallel IOS settings (bench): the script sets `max_parallel_workers_per_gather=5`, lowers
   `parallel_*` costs, and sets `min_parallel_*_scan_size=0` to consistently prefer up to 5‑way
   parallel Index Only Scans for both BTREE and SMOL. This makes parallel throughput benefits
   clear and comparable. For deterministic correctness checks, force single-worker.

Deterministic Correctness Check (Two‑Column)
- Some workloads (e.g., `GROUP BY a` on an index defined as SMOL(b,a)) can pick SMOL even without
  a leading‑key qual. To verify correctness deterministically:
  1. Choose `mode(a)` from a stable slice of the table, e.g., the first 100k rows by `ctid`.
  2. Compare `SELECT a, count(*) FROM t GROUP BY a` between:
     - Baseline (seqscan only), and
     - Forced SMOL IOS (drop the BTREE index, set `enable_seqscan=off`, and set
       `max_parallel_workers_per_gather=0` for single-worker), using an md5 over ordered results.
  3. Compare `SELECT count(*) FROM t WHERE a = :mode` under the same toggles.
- SMOL’s two‑column reader uses a per‑leaf cache and memcpy of both attrs per row, ensuring correctness
  while remaining allocation‑free per row. Multi‑level descent is supported for height≥2.
 - Parallel IOS settings (bench): the script sets `max_parallel_workers_per_gather=5`, lowers
   `parallel_*` costs, and sets `min_parallel_*_scan_size=0` to consistently prefer up to 5‑way
   parallel Index Only Scans for both BTREE and SMOL plans.
 - Bench scaling knobs (env): `ROWS`, `COLTYPE` (`int2|int4|int8`), `THRESH`, `BATCH`, `CHUNK_MIN`,
   and `TIMEOUT_SEC`. For large scales (≥50M), use `TIMEOUT_SEC>=30`. Post‑build maintenance is
   split into separate `CHECKPOINT`, `VACUUM`, and `ANALYZE` calls to respect statement timeouts.

Operator Classes
- int2_ops, int4_ops, int8_ops (fixed‑width only).

Roadmap
- Add WAL/FSM, tune costs, expand opclass coverage, validate DESC/multi‑col
  behavior, optional compression.
