# smol — Simple Memory-Only Lightweight index
# (a read‑only, space‑efficient PostgreSQL index access method)

This document captures the architecture and operational overview of the
smol index access method. Code‑level notes live in `AGENT_NOTES.md`.

Important
- One container name: smol. If using Docker, reuse a single container named
  `smol` for builds, tests, and benchmarks (see d* targets). Bare Make targets also work outside Docker.

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
- `amsearcharray=false`, `amsearchnulls=false`, `amcaninclude=true` (single‑key INCLUDE only).
- `amstrategies=5` (<, <=, =, >=, >), `amsupport=1` (comparator proc 1).
- `aminsert` ERROR (read‑only after build).

Limitations
- Read-only; drop/recreate to change data.
- Fixed-length key types only; no varlena (text, bytea, numeric).
- Multi-column keys supported when all key attributes are fixed-length; correctness-first implementation (no bitmap scans; IOS only).
- No NULL keys; not unique; not clusterable.
- INCLUDE columns supported for single-key indexes with fixed-length INCLUDE attrs (not limited to integers).
- Prototype: no WAL/FSM crash-safety yet.

Build & Test (PostgreSQL 18 from source)
- Docker helpers: `make dbuild` builds the image; `make dstart` creates/runs the `smol` container; `make dexec` opens a shell; `make dpsql` opens psql.
- Bare targets (inside or outside Docker):
  - Clean build + install: `make build`
  - Fast rebuild (no clean): `make rebuild`
  - Quick regression (build, start PG, run installcheck, stop PG): `make check`
  - Start PostgreSQL (initdb if needed): `make start`
  - Stop PostgreSQL: `make stop`
  - Benchmark: `make bench-smol-vs-btree`

Usage
- `CREATE EXTENSION smol;`
- `CREATE INDEX idx_smol ON some_table USING smol (col1, col2);`
- Planner: favor IOS as usual; SMOL errors on non‑IOS paths. Parallel scans
  are supported, but see notes below on deterministic correctness testing.

Tests & Benchmarks
- `make check` runs the regression suite and stops PG when done.
- `sql/` contains correctness/regression tests (kept short).
- `bench/` contains benchmarks; run `make bench-smol-vs-btree` (use `make stop` when finished).
- Bench scripts enforce hard client timeouts (`TIMEOUT_SEC`, `KILL_AFTER`) and
  set a server-side `statement_timeout` slightly below `TIMEOUT_SEC` to avoid
  lingering backends when the client is killed by timeout.
 - Parallel IOS settings (bench): the script sets `max_parallel_workers_per_gather=5`, lowers
   `parallel_*` costs, and sets `min_parallel_*_scan_size=0` to consistently prefer up to 5‑way
   parallel Index Only Scans for both BTREE and SMOL. This makes parallel throughput benefits
   clear and comparable. For deterministic correctness checks, force single-worker.
- Gap scenario (key-only COUNT): `make bench-gap-scenario` runs `bench/smol_gap_scenario.sh`.
  - Schema: UNLOGGED table `gap_bench(b int2)`; indexes: BTREE(b) and SMOL(b); query: `COUNT(*) WHERE b > -1`.
  - Purpose: emphasize SMOL’s density and IOS advantage with minimal aggregation overhead.

Deterministic Correctness Check (Multi‑Column Regression)
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
- int2_ops, int4_ops, int8_ops, plus several fixed‑length builtins (oid, float4/float8, date, timestamp, timestamptz, bool) as provided in `smol--1.0.sql`.

Roadmap
- Add WAL/FSM, tune costs, expand opclass coverage, validate DESC/multi‑col
  behavior, optional compression.

Benchmarking
- Quick default (legacy): `make bench-smol-btree-5m` compares BTREE vs SMOL on 5M rows.
- Selectivity-based: `make bench-smol-vs-btree` drives `bench/smol_vs_btree_selectivity.sh` and accepts:
  - ROWS: number of rows (default 5_000_000)
  - COLTYPE: `int2|int4|int8` (default int2)
  - COLS: number of columns (default 2). COLS=1 creates a key-only table (b). COLS>1 creates b plus INCLUDE columns a1..a{COLS-1} for BTREE and SMOL.
  - SELECTIVITY: comma list of fractions in [0.0..1.0]. For each s, the script regenerates data so that the predicate `b >= M` (M is the midpoint of the domain) matches approximately `s * ROWS` rows. Distribution: two random windows on either side of M (~25% of the domain wide) and one precise record at b=M so `s=0.0` yields exactly one match.
  - WORKERS: comma list for `max_parallel_workers_per_gather` (e.g., `0,1,2,5`).
  - TIMEOUT_SEC, KILL_AFTER, BATCH, CHUNK_MIN as before.
- Curated set: `make bench` runs COLS={1,2} and SELECTIVITY in {0.1,0.5,0.9}.
- Exhaustive matrix (heavy): `make bench-all` loops over `sel={0.0,0.1,0.25,0.5,0.75,0.9,0.95,0.99,1.0}`, `int2/4/8`, `workers=1/2/5`, `rows=500K/5M/50M`, and `cols=1/2/4/8/16`.
- Output: CSV rows with index type, build time (ms), size (MB), query time (ms), and the EXPLAIN plan line; per-selectivity/worker sweeps are emitted inline.

RLE + Duplicate-Caching Advantage (new)
- Target: `make bench-rle-advantage ROWS=... WORKERS=... INC=... COLTYPE=int2 UNIQVALS=... [DISTRIBUTION=uniform|zipf]`
- Suggested sweep: `SWEEP_UNIQVALS=10,1000,100%` (represents heavy duplicates → mid → many-distinct).
- Sweep support: `SWEEP="0.12,0.5,0.99"` runs multiple selectivities; results saved to `results/rle_adv.csv`.
- Pretty summary: `make rle-adv-pretty ROWS=... WORKERS=... INC=...` prints btree vs smol times and speedups.
- Plot (requires matplotlib): `make rle-adv-plot ROWS=... WORKERS=... INC=... OUT=results/rle_adv_plot.png`.
- Suggested “big win” repro (50M rows, 5 workers):
  - `INC=12` (many identical INCLUDEs), `SWEEP=0.12,0.5`.
  - Expect SMOL ~2–8× faster on COUNT, ~1.1–1.5× on SUM; SMOL index significantly smaller than BTREE.

Development policy
- Builds must be warning-free: `make build` and `make rebuild` should emit no compiler warnings. Remove unused code or fix signatures to satisfy this.

psql in Docker
- Always connect as the `postgres` OS user inside the container.
- From host: run `make dpsql` to open psql as `postgres` in the `smol` container.
- Inside container: run `make psql` or `su - postgres -c "/usr/local/pgsql/bin/psql"`.
- If you run `psql` as `root`, PostgreSQL will error with `FATAL: role "root" does not exist`.
