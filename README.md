# smol — Read‑Only, Space‑Efficient PostgreSQL Index AM

This README is the single source of truth for humans and LLMs. It covers goals, architecture, detailed technical behavior, usage, build/run (Docker and local), tests, and benchmarks.

## Overview

`smol` is a custom PostgreSQL index access method optimized for index‑only scans on append‑only data. It omits heap TIDs and stores only fixed‑width key values, yielding smaller indexes and better cache density. It supports ordered scans, backward scans, and parallel scans. Writes are not supported via the access method (read‑only after build).

Key properties
- Read‑only AM; drop/recreate to change data
- Fixed‑width key types only (e.g., int2, int4, int8, date, timestamp)
- Multi‑column keys supported; no INCLUDE columns
- No NULL keys (rejected at build)
- Supports parallel scans; no bitmap scans
- Always index‑only: executor receives tuples from the index

## Architecture

On‑disk layout
- Metapage (block 0): magic, version, `nkeyatts`, optional first/last‑key directory
- Data pages (block ≥ 1): packed, aligned fixed‑width key values only; no per‑tuple headers, no null bitmap, no TIDs; one standard line pointer per item

Build path
1. Lock heap `AccessExclusive` and scan via `table_index_build_scan(..., smol_build_callback, ...)`
2. Enforce invariants: no NULLs; fixed‑width attributes only
3. Sort entries using opclass comparator (support proc 1) honoring per‑key collation
4. Initialize metapage; write data pages with `PageAddItem`
5. For executor plumbing, mark heap block 0 PD_ALL_VISIBLE and set its VM bit so a synthetic TID on (0,1) never causes heap fetches

Scan path
- IOS‑only: requires `xs_want_itup`; errors if planner tries non‑IOS
- Forward/backward and parallel scans supported; returns tuples via `index_form_tuple`
- Provides a synthetic TID `(block 0, off 1)`; heap fetches never occur

AM flags
- `amcanorder=true`, `amcanbackward=true`, `amcanparallel=true`
- `amgetbitmap=NULL` (no bitmap scans)
- `amsearcharray=false`, `amsearchnulls=false`, `amcaninclude=false`
- `amstrategies=5` (<, <=, =, >=, >), `amsupport=1` comparator
- `aminsert` ERROR (read‑only)

Tuple & page sizing
- BTREE payload header: 6‑byte TID + 2‑byte `t_info` = 8 bytes
- SMOL payload: key data only (no TID, no per‑tuple header)
- Before page effects, payload saves ~8–14 bytes/tuple; on page with line pointers/alignment, typical observed savings at 1M rows:
  - Two int2 keys: ~19 MB vs ~21 MB (≈10–12% smaller)
  - Two int4 keys: similar ballpark; savings shrink as key width grows

Performance model
- SMOL always delivers true index‑only scans (no heap fetches)
- BTREE can also achieve true IOS when the visibility map (VM) marks heap pages all‑visible; otherwise it may issue heap fetches
- When both are IOS, SMOL’s advantage is mainly smaller index size and better cache density; otherwise SMOL can avoid BTREE’s heap I/O

## Limitations
- Read‑only: no insert/update/delete via the AM
- Fixed‑width key types only; no varlena (`text`, `bytea`, `numeric`)
- No NULL keys; no INCLUDE columns
- Not a unique index; not clusterable
- Prototype: no WAL/FSM crash‑safety yet

## Build & Install

Inside Docker (recommended, clean toolchain)
- `docker build -t smol-dev .`
- `docker run --rm -t -v "$PWD":/workspace -w /workspace smol-dev bash -lc 'make clean && make && make install'`
- Start a reusable container for testing:
  - `docker run -d --name smol-dev-ctr -v "$PWD":/workspace -w /workspace smol-dev sleep infinity`
  - `docker exec smol-dev-ctr bash -lc 'pg_ctlcluster 16 main start'`
  - `docker exec smol-dev-ctr bash -lc "su - postgres -c 'psql -v ON_ERROR_STOP=1 -c \"CREATE EXTENSION smol;\"'"`

Local (requires PostgreSQL 16 server headers)
- Ensure `pg_config` is on PATH
- `make && make install`

## Usage

Create index
```sql
CREATE EXTENSION smol;
CREATE INDEX idx_smol ON some_table USING smol (col1, col2);
```

Planner
- Favor IOS as usual; SMOL will error if planner attempts non‑IOS
- Parallel scans are supported and often chosen on large relations

## Tests & Benchmarks

Regression
- `make installcheck` (runs `sql/smol_basic.sql`): verifies IOS, ordering, and NULL rejection

Benchmarks (root)
- `bench_brc.sql` — BRC‑style (two int2), BTREE vs SMOL; ensures BTREE VM bits for IOS
- `bench_fixed.sql` — Combined fixed‑width benchmarks:
  - SMALLINT uniprocessor
  - SMALLINT parallel
  - INT4 uniprocessor
  - INT4 parallel

Run in Docker
- `docker exec smol-dev-ctr bash -lc "su - postgres -c 'psql -f /workspace/bench_brc.sql'"`
- `docker exec smol-dev-ctr bash -lc "su - postgres -c 'psql -f /workspace/bench_fixed.sql'"`

Interpreting results
- Check `pg_relation_size` for index size; expect SMOL < BTREE
- In EXPLAIN (ANALYZE), `Heap Fetches: 0` indicates true IOS; SMOL always 0
- With VM‑assisted BTREE IOS, performance gap narrows; SMOL’s win is primarily better cache density

## Operator Classes

Provided (fixed‑width only)
- int2_ops, int4_ops, int8_ops (default for int2/int4/int8)

## Implementation Notes (deep dive)

On‑disk
- Metapage stores counts and optional first/last‑key directory for the first attribute (int2/int4/int8) to seed page‑level lower bounds
- Data pages store only aligned fixed‑width key payloads; tuples are materialized on demand for executor output

Build
- Collects key datums in memory, sorts with opclass cmp(1), and writes pages
- Enforces fixed‑width and no‑NULL constraints; disallows inserts post‑build
- Marks heap block 0 all‑visible and sets VM bit, enabling synthetic TID usage

Scan
- Requires `xs_want_itup` (IOS), otherwise ERROR
- Parallel scans coordinate via a shared atomic page counter
- Lower‑bound seek uses directory or binary search by last key on pages

Planner/costs
- Lightly favors SMOL to encourage IOS and parallel plans on large indexes
- No bitmap scan support

## Roadmap
- Crash‑safety: WAL for page init/inserts; FSM; proper extension protocol
- Planner: tuned costs; broader fixed‑width opclass coverage; DESC tests
- Features: optional compression; columnar variants; Bloom filters

## Troubleshooting
- “Heap Fetches” in BTREE IOS: run `VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING)` after `CHECKPOINT`, with `vacuum_freeze_* = 0` in a quiet cluster to set VM bits
- Parallel plans: set `max_parallel_workers_per_gather` and lower `min_parallel_index_scan_size`; ensure `enable_seqscan=off` when exploring IOS
