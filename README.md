# smol — Simple Memory-Only Lightweight index
# (a read‑only, space‑efficient PostgreSQL index access method)

This document captures the architecture and operational overview of the
smol index access method. Code‑level notes live in `AGENT_NOTES.md`.

Important
- One container name: smol. If using Docker, reuse a single container named
  `smol` for builds and tests (see d* targets). Bare Make targets also work outside Docker.

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

Usage
- `CREATE EXTENSION smol;`
- `CREATE INDEX idx_smol ON some_table USING smol (col1, col2);`
- Planner: favor IOS as usual; SMOL errors on non‑IOS paths. Parallel scans
  are supported, but see notes below on deterministic correctness testing.

Tests
- `make check` runs regression (`sql/`) and stops PG when done.

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
 

Operator Classes
- int2_ops, int4_ops, int8_ops, plus several fixed‑length builtins (oid, float4/float8, date, timestamp, timestamptz, bool) as provided in `smol--1.0.sql`.

Roadmap
- Add WAL/FSM, tune costs, expand opclass coverage, validate DESC/multi‑col
  behavior, optional compression.

Real‑world mappings
- RL replay buffers / telemetry with hot keys + small features: model with heavy duplicates on the key, many INCLUDE columns, and parallel IOS — e.g., `ROWS=50M WORKERS=5 INC=12 UNIQVALS=10 DISTRIBUTION=zipf` (see RLE+dupcache).
- Time‑series by day (date + fact): two‑column scans with a lower bound on date and small include fields — e.g., `COLTYPE=int4 (value), date (key)`, `COLS=2`, `SELECTIVITY` varying. Unified driver handles this via selectivity and COLS sweeps.
- Short identifiers/text keys: C collation, lengths 4/8/16/32; count/equality workloads — e.g., `ROWS=5M WORKERS=5 UNIQVALS=1000 STRLEN=16`.
- Analytics with moderate selectivity and wide include lists: `SELECTIVITY=0.1 COLS=10 ROWS=50M WORKERS=5` (key‑only or single key with INCLUDEs).

 

Development policy
- Builds must be warning-free: `make build` and `make rebuild` should emit no compiler warnings. Remove unused code or fix signatures to satisfy this.

psql in Docker
- Always connect as the `postgres` OS user inside the container.
- From host: run `make dpsql` to open psql as `postgres` in the `smol` container.
- Inside container: run `make psql` or `su - postgres -c "/usr/local/pgsql/bin/psql"`.
- If you run `psql` as `root`, PostgreSQL will error with `FATAL: role "root" does not exist`.
