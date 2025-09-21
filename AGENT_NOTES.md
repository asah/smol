# SMOL AM Development Notes

This file summarizes the hard‑won details needed to work on the `smol` PostgreSQL index access method in this repo.

## Goals & Constraints
- Ordered semantics; read‑only index. After CREATE INDEX, table becomes read‑only (enforced via triggers).
- Index‑only scans only. Do NOT store TIDs; reject non‑IOS access (`amgettuple` errors if `!xs_want_itup`).
- No NULL support at all (CREATE INDEX errors on NULLs).
- Fixed‑width key types only (no varlena such as `text`, `bytea`, `numeric`).
- Prototype: no WAL/FSM yet; correctness over crash‑safety for now.
- PostgreSQL 16; no INCLUDE columns; parallel scans supported; no bitmap scan.

## Architecture Overview
- On‑disk
  - Metapage at block 0: magic, version, `nkeyatts`.
  - Data pages from block 1: packed, aligned fixed‑width key attribute values only; no per‑tuple headers, no null bitmap, no TIDs.
- Build path (`ambuild`)
  - Acquire `AccessExclusiveLock` on heap.
  - Use `table_index_build_scan(..., smol_build_callback, ...)` to collect all rows.
  - Enforce no NULLs; fixed‑width datums only. Be careful to allocate array with `palloc` on first growth (avoid `repalloc` with cap==0).
  - Sort entries using opclass comparator proc 1 (`index_getprocinfo`), respecting per‑key collation.
  - Write metapage (if absent) and append data pages with `PageAddItem`.
  - Executor skip‑heap contract: mark heap block 0 all‑visible (must:
    1) lock heap block 0, `PageSetAllVisible`, `MarkBufferDirty`, 2) `visibilitymap_pin`, 3) `visibilitymap_set` passing the locked heap buffer).
- Scan path (`amgettuple`)
  - Require `xs_want_itup` (IOS only) or ERROR.
  - Iterate forward/backward over data pages; support parallel scans; for each match, build `xs_itup = index_form_tuple(...)`.
  - Synthesize a constant TID for executor plumbing: set `xs_heaptid` to `(block 0, off 1)`.

## AM Flags & Behavior
- `amcanorder=true`, `amcanbackward=true`, `amcanparallel=true`, `amgetbitmap=NULL`.
- `amsearcharray=false`, `amsearchnulls=false`, `amcaninclude=false`.
- `amstrategies=5` (<, <=, =, >=, >); `amsupport=1` (comparator proc 1).
- `aminsert` ERROR (read‑only after build).

## Docker & Testing
- Build image: `docker build -t smol-dev .`
- Start reusable container: `docker run -d --name smol-dev-ctr -v "$PWD":/workspace -w /workspace smol-dev sleep infinity`
- Compile/install inside container: `docker exec smol-dev-ctr bash -lc 'make && make install'`
- Start cluster: `docker exec smol-dev-ctr bash -lc 'pg_ctlcluster 16 main start'`
- Run tests: `docker exec smol-dev-ctr bash -lc 'su - postgres -c "cd /workspace && make installcheck"'`

Regression suite
- `sql/smol_basic.sql` and `expected/smol_basic.out` cover: IOS ordering, reject non‑IOS scans, NULL build error, and sealing writes.
- Note: `DROP TRIGGER IF EXISTS` emits NOTICEs when triggers are absent; expected output should include them or use an alternative to suppress noise.

## TODO / Next Steps
- Add crash‑safety: Generic WAL for page init/inserts; FSM + proper relation extension protocol.
- Cost model and planner properties; broader opclass coverage.
- Optional: auto‑seal helper invocation after CREATE INDEX.
- Improve tests (more types, multi‑column, DESC ordering) and stabilize outputs.

Quick mental checklist
- Collect → sort (proc 1, collations) → write metapage + data pages.
- Mark heap blk 0 PD_ALL_VISIBLE then set VM bit with `visibilitymap_set`.
- IOS only: enforce `xs_want_itup`; set a synthetic `xs_heaptid`.
- No NULLs; no TIDs; read‑only; ordered.

## Detailed Technical Notes (for future me)

What PG internals I actually read
- AM API and glue
  - `postgres/src/include/access/amapi.h`: `IndexAmRoutine` fields; how handler wires callbacks and flags.
  - `postgres/src/backend/access/index/indexam.c`: backend calls into AM: `index_beginscan`, `index_rescan`, `index_getnext_tid`, `index_fetch_heap`, `index_getnext_slot`, `index_getbitmap`.
  - `postgres/src/include/access/genam.h`: wrappers, `RelationGetIndexScan`, `index_getprocinfo`, etc.
  - `postgres/src/backend/catalog/index.c`: build path uses `FormIndexDatum`, `BuildIndexInfo`; I switched to `table_index_build_scan`.
- Executor behavior for IOS
  - `postgres/src/backend/executor/nodeIndexonlyscan.c`: always asks AM for a TID via `index_getnext_tid`. It will skip heap fetch only if VM says the TID’s heap page is all-visible; otherwise it fetches from heap. It expects `xs_want_itup=true` and either `xs_itup` or `xs_hitup`. It takes a page-level predicate lock on the heap block of the returned TID (even for IOS).
- Storage and visibility
  - `postgres/src/include/storage/bufpage.h`: page flags; `PD_ALL_VISIBLE` macros (`PageIsAllVisible`, `PageSetAllVisible`).
  - `postgres/src/include/access/visibilitymap.h` and `postgres/src/backend/access/heap/visibilitymap.c`: `visibilitymap_pin/set/get_status`; asserts require PD_ALL_VISIBLE to be set when setting VM bits (when not in recovery). VM writes may WAL-log via `log_heap_visible` when needed.
  - `postgres/src/backend/storage/freespace/indexfsm.c`: how FSM works (not used yet).
- Reference AMs skimmed
  - `nbtree`: `nbtree.c`, `nbtinsert.c`, `nbtsearch.c`, `nbtpage.c` for overall shape and handler flags.
  - `brin`, `gist`, `gin`, `spgist`: scanned to see how they get procs via `index_getprocinfo` and their build/insert paths.

Key design choices and why
- IOS only with no stored TIDs: executor still requires a TID to drive visibility and predicate locking. I synthesize a constant TID `(block 0, off 1)` and ensure VM says block 0 is all-visible, so executor never fetches the heap. Tradeoff: predicate locks always target heap block 0.
- Ordered semantics: collect all entries during build, sort using opclass comparator proc 1 per key (respect collation), and write pages in order. Scans just iterate pages in order.
- Read-only: `aminsert` ERROR; after CREATE INDEX we provide `smol_seal_table(regclass)` to install BEFORE triggers that block writes on the table.
- No NULLs: enforce at build callback; reject early.

Build path details and pitfalls
- Use `table_index_build_scan(table_rel, index_rel, index_info, ..., smol_build_callback, state, scan=NULL)`; the callback signature provides `values[]`, `isnull[]`, and a heap TID. I ignore the TID.
- Memory: growth bug fixed. Do not call `repalloc` with `cap==0`. First growth must use `palloc(sizeof(T)*newcap)` then `repalloc` subsequently.
- Include columns: `index->rd_index->indnkeyatts` is the number of key attrs; `RelationGetDescr(index)->natts` can include INCLUDE attributes. Because we don’t support INCLUDE, set `bst.natts = nkeyatts` and disable `amcaninclude`.
- Sorting: retrieve comparator with `index_getprocinfo(index, attno, 1)` (proc 1) and call via `FunctionCall2Coll(proc, collation, a, b)`, expecting int32 result semantics like btree `btint4cmp`, `bttextcmp`.
- Page writing: ensure a metapage exists at block 0 with magic/version/att counts; data starts at block 1. On each data page, `PageInit`, `PageAddItem`. No WAL or FSM yet.

Scan path details
- Reject non-IOS: if `!scan->xs_want_itup`, `ereport(ERROR)` with a clear message.
- Start from block 1 (skip metapage). Walk pages; for each used line pointer, validate tuple bounds; apply scankeys; if match, materialize `xs_itup` with `index_form_tuple` and set `xs_heaptid` to `(0,1)`; set `xs_recheck=false`.

Visibility map required sequence (to keep IOS heapless)
- You cannot call `visibilitymap_set()` with an `InvalidBuffer` heapBuf: `visibilitymap_set` asserts `PageIsAllVisible(BufferGetPage(heapBuf))` when not in recovery. The safe sequence is:
  1. `heapBuf = ReadBuffer(heap, 0); LockBuffer(heapBuf, BUFFER_LOCK_EXCLUSIVE);`
  2. `PageSetAllVisible(BufferGetPage(heapBuf)); MarkBufferDirty(heapBuf);`
  3. `visibilitymap_pin(heap, 0, &vmbuf);`
  4. `visibilitymap_set(heap, 0, heapBuf, InvalidXLogRecPtr, vmbuf, InvalidTransactionId, VISIBILITYMAP_ALL_VISIBLE);`
  5. Unlock/release both buffers.

Bugs I hit and fixes
- Segfault during `CREATE INDEX` before sorting: cause was using `repalloc` on a NULL pointer when `cap==0`. Fix: allocate with `palloc` on initial growth.
- Segfault inside `visibilitymap_set`: I initially passed `InvalidBuffer` for `heapBuf` and hadn’t set PD_ALL_VISIBLE; `visibilitymap_set` requires `PageIsAllVisible(heapBuf)`. Fix: set PD_ALL_VISIBLE under exclusive lock and pass `heapBuf` into `visibilitymap_set`.
- INCLUDE columns mismatch: using `itupdesc->natts` caused me to think I had `natts` attrs to store, but I only store key attrs. Fix: set `bst.natts = nkeyatts` and `amcaninclude=false`.
- Starting scan at block 0 (metapage) was wrong. Fix: start at block 1.
- Test diffs: `smol_seal_table` drops non-existent triggers first, causing NOTICEs; expected file must include these NOTICEs or avoid dropping when absent.

Flags and opclasses
- `amstrategies = 5` for (<, <=, =, >=, >); `amsupport = 1` comparator proc. `amcanorder=true`, `amcanbackward=true`, `amcanparallel=true`, `amsearcharray=false`, `amsearchnulls=false`, `amgetbitmap=NULL`, `amcaninclude=false`.
- Operator classes provided in `smol--1.0.sql` for fixed‑width types (e.g., `int2`, `int4`, `int8`, `date`, `timestamp`) using btree compare procs.

On-disk tuple and (de)serialization
- Each tuple is the concatenation of fixed‑width key values in order. Offsets are computed from attribute lengths; advance by `MAXALIGN(attlen)` as needed. Store by‑val types with `store_att_byval`, otherwise `memcpy` fixed‑length by‑ref types. Varlen types are not supported.

Docker/test workflow notes
- Build: `docker build -t smol-dev .` (Ubuntu 24.04 with PostgreSQL 16 dev packages).
- Reusable container: `docker run -d --name smol-dev-ctr -v "$PWD":/workspace -w /workspace smol-dev sleep infinity`.
- Compile/install: `docker exec smol-dev-ctr bash -lc 'make && make install'`.
- Start cluster: `docker exec smol-dev-ctr bash -lc 'pg_ctlcluster 16 main start'`.
- Run tests: `docker exec smol-dev-ctr bash -lc 'su - postgres -c "cd /workspace && make installcheck"'`.
- Server logs live at `/var/log/postgresql/postgresql-16-main.log` inside the container; very helpful to pinpoint segfault sites and emit `elog(LOG, ...)` breadcrumbs.

C warnings and style
- GCC/Clang in PGXS environment warn on C90 mixed declarations; stylistically, move declarations to top of block when convenient. Keep functions `static` unless part of AM API.

Caveats and future work
- No WAL/FSM yet: crash-safety is not guaranteed. Add Generic WAL to log metapage/data page initialization and inserts; integrate with FSM and relation extension protocol.
- Cost model/correlation need work; current estimates are placeholders.
- Multi-column scans and DESC ordering: verify comparator semantics match planner expectations; add tests.
- Parallel scans supported; bitmap scans not supported; flags set accordingly.

## STATE SNAPSHOT (2025-09-21)

What’s implemented (key points)
- Tight packing of fixed‑width attributes:
  - Build path writes key values back‑to‑back (no MAXALIGN per attribute; no per‑tuple headers).
  - Scan path steps by fixed attlen only; byval values are memcpy’d into Datum; byref returns pointer.
- Per‑scan CPU reductions:
  - Reuse values/isnull buffers; per‑tuple memory context for xs_itup.
  - Precompute natts, attlen[], attbyval[], offsets[] per scan.
- Parallel scan fix and scheduling:
  - Fixed correctness bug: shared page counters are initialized once; no re‑init by workers.
  - Chunked page scheduling per worker to reduce atomics; light prefetch in serial scans.
  - GUC: smol.parallel_chunk_pages (default 8) controls pages per chunk.

Bench scripts (kept)
- bench_brc.sql — BRC‑style (two int2), parameter: -v rows=NNN (default 1,000,000).
- bench_fixed.sql — Combined SMALLINT/INT4 × uni/parallel; parameters:
  - -v rows_si=NNN (default 1,000,000)
  - -v rows_i4=NNN (default 1,000,000)
  - -v par_workers_si=K (default 2)
  - -v par_workers_i4=K (default 2)

Correctness
- correctness_check.sql compares BTREE vs SMOL (serial) for SMALLINT/INT4 sums; confirmed equal after parallel fix.
- Parallel vs serial SMOL sums validated equal on 1M rows (post‑fix).

Large‑scale results (high level)
- SMALLINT 50M: BTREE ~1067 MB, SMOL ~574 MB; uni ~59.5s vs ~5.3s; parallel(2) ~28.2s vs ~5.35s.
- INT4 25M: BTREE ~536 MB, SMOL ~287 MB; uni ~15.5s vs ~1.36s; parallel(2) ~7.2s vs (SMOL parallel INT4 ~0.46–0.33s with 2–4 workers).

How to rebuild & run (Docker)
- Build/install: docker build -t smol-dev . && docker run --rm -t -v "$PWD":/workspace -w /workspace smol-dev bash -lc 'make clean && make && make install'
- Start DB: docker run -d --name smol-dev-ctr -v "$PWD":/workspace -w /workspace smol-dev sleep infinity && docker exec smol-dev-ctr bash -lc 'pg_ctlcluster 16 main start'
- Enable extension: docker exec smol-dev-ctr bash -lc "su - postgres -c 'psql -c \"CREATE EXTENSION smol;\"'"
- Benchmarks:
  - docker exec … "su - postgres -c 'psql -v rows_si=50000000 -v rows_i4=25000000 -f /workspace/bench_fixed.sql'"
  - Set workers: -v par_workers_si=4 -v par_workers_i4=3
  - Tune SMOL chunking: SET smol.parallel_chunk_pages = 128;
- Correctness:
  - docker exec … "su - postgres -c 'psql -f /workspace/correctness_check.sql -v rows_si=5000000 -v rows_i4=2500000'"

Open TODOs (next iteration targets)
- Further reduce SMOL per‑tuple CPU: extend fast paths to secondary attrs; consider reusing a preallocated xs_itup where safe.
- Adaptive parallel chunk size based on relpages; explore larger chunks on big indexes.
- Planner cost calibration for SMOL; consider track_io_timing to separate CPU vs I/O and tune.
- Optional: expose a GUC for prefetch distance.

Notes for restart
- If Codex is restarted, this section is the authoritative state snapshot. Bench scripts and GUCs allow reproducing results without large chat context.
