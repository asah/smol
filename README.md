# smol — Simple Memory-Only Lightweight index

SMOL is a read‑only, space‑efficient PostgreSQL index access method that can provide substantial speedups for reporting queries that access a small number of small, fixed-width columns.
SMOL is currently a research prototype, not suitable for production and only deployable as an EXTENSION in self-hosted PostgreSQL deployments, e.g. not AWS RDS/Aurora.
The authors are aware that the ideas expressed in SMOL may be best brought to mainstream use as new capabilities of existing features, rather than a new index type.
To discourage inappropriate use, the authors intentionally are not packaging SMOL yet for pgxn or other "easy" installation methods - you must compile from source for now.

## Performance

SMOL indexes can be 0-99.95% smaller than B-Trees with corresponding less use of cache, memory and I/O, while providing +/- 20% performance, with edge cases providing multiplicative speedups. SMOL works best on single key columns with highly repeated values and optionally, INCLUDE column(s) that are highly repeated. For unique values, SMOL falls back to an uncompressed, "zero copy" format that's a small optimization over B-Trees in space and speed.

### When to Use SMOL

✅ **Use SMOL when:**
- Memory-constrained environments (cloud, containers)
- Read-only reporting workloads
- Fixed-width columns (integers, date, uuid)
- Smaller backups/faster restores matter

⚠️ **Use BTREE when:**
- Write-heavy workloads (SMOL is read-only)
- Variable-length keys (SMOL requires fixed-width or C-collation text)
- NULL values needed (SMOL rejects NULLs)
- Ultra low-latency requirements (every millisecond counts)

See [BENCHMARKING.md](BENCHMARKING.md) for detailed results and methodology.


## How does SMOL compare with existing PostgreSQL features and index types

It's perhaps helpful to think of SMOL as a form of materialized view. Unlike MATERIALIZED VIEW (MV), existing SQL statements do not need to be retargeted for the MV. That said, for reasons explained below, users are warned not to think they can use SMOL haphazardly, or they may experience errors.

The primary alternative to SMOL are B-trees, specifically the nbtree implementation built into PostgreSQL.

BRIN and HASH indexes cannot satisfy range queries. SMOL is probably not competitive with HASH indexes for equality-only.


## How does SMOL work?

SMOL provides a page-oriented tree structure over the data just like nbtree, but unlike nbtree:
1. SMOL assumes read-only, so avoids the need to check for visibility (also simplifies the code).
2. SMOL removes per-tuple fields storing the number of attributes and their lengths - this is stored once for the whole index
3. SMOL stores data in columnar format within a page
4. given fixed attributes, lengths and columnar layout, SMOL can then tightly pack values and use very fast compression (RLE). Notably, when scanning repeated values, the RLE-aware executor inherently knows how many values are in the repetition, just reading the dictionary.
5. SMOL optimizes small, repetitive data and caches the response tuple to avoid extra traversal and memory copies.
6. SMOL detects and enforces maximum strength during CREATE INDEX and assumes "C" collation, which allows its execution to treat strings like fixed width numbers. This is fine for identifier-type strings which are typically ASCII.

## Quick start

### preparing the table

The underlying table must be read-only. If it's not, then you need to copy the table, e.g. to an UNLOGGED table.

The string columns used in SMOL must be short and use "C" collation. If not, consider ALTER TABLE tblname ADD COLUMN colname TEXT COLLATE "C";

See `assignments.sql` for an example. 

### creating the index

CREATE INDEX idxname ON tblname USING smol (keyfield) INCLUDE (other, fields, you, want);

-- IMPORTANT: Always run ANALYZE after index creation for optimal performance
ANALYZE tblname;

SELECT keyfield, COUNT(*) FROM tblname GROUP BY 1;

SELECT keyfield, other, fields, COUNT(*) FROM tblname GROUP BY 1,2,3;

SELECT keyfield, COUNT(*) FROM tblname GROUP BY 1;


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

Tests
- Run regression tests: `make install && make start && make installcheck && make stop`
- Code coverage: make coverage

 

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
- Inside container: run `make psql` or just `psql` directly (you are running as user `postgres`).
- If you run `psql` as `root`, PostgreSQL will error with `FATAL: role "root" does not exist`.
