# SMOL AM Development Notes

This file summarizes the hard‑won details needed to work on the `smol`
PostgreSQL 18 index access method in this repo.

Run Policy (Mandatory)
- One container name only: `smol`. Always build, test, and benchmark using
  a single Docker container named `smol`; reuse it across runs.
- Always run inside Docker. Build the image with `make dockerbuild`, then
  invoke inside-container targets via `docker exec -it smol make <target>`.
  Do not attempt host builds/tests; they are unsupported and prone to
  SDK/PG mismatches.

## Goals & Constraints
- Ordered semantics; read‑only index. After CREATE INDEX, table becomes read‑only (enforced via triggers).
- Index‑only scans only. Do NOT store TIDs; reject non‑IOS access (`amgettuple` errors if `!xs_want_itup`).
- No NULL support at all (CREATE INDEX errors on NULLs).
- Fixed‑width key types only (no varlena such as `text`, `bytea`, `numeric`).
- Prototype: no WAL/FSM yet; correctness over crash‑safety for now.
- PostgreSQL 18; no INCLUDE columns; parallel scans supported; no bitmap scan.
- we are restarting this project to provide cleaner code, so these notes may contain info from the previous iteration(s). If you spot inconsistencies please propose cleanup to the user.


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
- Misc
  - to enforce read-only, AM functions which perform writes throw errors and return, with the assumption that the caller will abort the transaction.

## AM Flags & Behavior
- `amcanorder=true`, `amcanbackward=true`, `amcanparallel=true`, `amgetbitmap=NULL`.
- `amsearcharray=false`, `amsearchnulls=false`, `amcaninclude=false`.
- `amstrategies=5` (<, <=, =, >=, >); `amsupport=1` (comparator proc 1).
- `aminsert` ERROR (read‑only after build).

## Docker & Testing
- Build image: `make dockerbuild` (image tag: `smol`).
- Start/reuse container: `make dockerstart` (container name: `smol`).
- One‑shot regression: `make dockercheck` (build+install, initdb if needed, start PG18, run installcheck, stop PG so container is idle).
- Ad‑hoc: `docker exec smol bash -lc 'make clean && make && make install'`
- To init/start PG manually (first time only):
  - `docker exec smol bash -lc 'su - postgres -c "/usr/local/pgsql/bin/initdb -D /var/lib/postgresql/data"'`
  - `docker exec smol bash -lc 'su - postgres -c "/usr/local/pgsql/bin/pg_ctl -D /var/lib/postgresql/data -l /tmp/pg.log -w start"'`
- Stop/quiet: `make dockerstop` (stops PG18); remove: `make dockerrm`.
- Keep regression fast; correctness lives under `sql/`. `bench/` is for benchmarks only.
- Server logs: `/tmp/pg.log` (or set a different path) inside the container.

Quick mental checklist
- Collect → sort (proc 1, collations) → write metapage + data pages.
- Mark heap blk 0 PD_ALL_VISIBLE then set VM bit with `visibilitymap_set`.
- IOS only: enforce `xs_want_itup`; set a synthetic `xs_heaptid`.
- No NULLs; no TIDs; read-only; ordered.

## Logging
- Runtime GUC: `smol.debug_log` (SUSET). When enabled, SMOL emits concise LOG lines
  prefixed with `[smol]` covering key lifecycle events (handler init, build start/end,
  page creation, meta read/write, scan begin/rescan, leaf hops). The hot inner loops
  (per-tuple return) avoid logging to keep overhead negligible.
- Macros provide near-zero cost when disabled:
  - `SMOL_LOG(msg)` and `SMOL_LOGF(fmt, ...)` guard `elog(LOG, ...)` behind the
    `smol.debug_log` flag; define `SMOL_TRACE=0` at compile time to compile logs out
    entirely if needed for microbenchmarks.
  - Default build sets `SMOL_TRACE=1`, but the GUC defaults to off, so production
    and benchmarks incur no meaningful overhead.

## Detailed Technical Notes (for future me)

What PG internals I actually read
- see AGENT_PGIDXAM_NOTES.md
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

Bugs to watch out for:
- Segfault during `CREATE INDEX` before sorting: `repalloc` on a NULL pointer when `cap==0`. Fix: allocate with `palloc` on initial growth.
- Segfault inside `visibilitymap_set`: I initially passed `InvalidBuffer` for `heapBuf` and hadn’t set PD_ALL_VISIBLE; `visibilitymap_set` requires `PageIsAllVisible(heapBuf)`. Fix: set PD_ALL_VISIBLE under exclusive lock and pass `heapBuf` into `visibilitymap_set`.
- INCLUDE columns mismatch: using `itupdesc->natts` caused me to think I had `natts` attrs to store, but I only store key attrs. Fix: set `bst.natts = nkeyatts` and `amcaninclude=false`.
- Starting scan at block 0 (metapage) was wrong. Fix: start at block 1.

Flags and opclasses
- `amstrategies = 5` for (<, <=, =, >=, >); `amsupport = 1` comparator proc. `amcanorder=true`, `amcanbackward=true`, `amcanparallel=true`, `amsearcharray=false`, `amsearchnulls=false`, `amgetbitmap=NULL`, `amcaninclude=false`.
- Operator classes provided in `smol--1.0.sql` for fixed‑width types (e.g., `int2`, `int4`, `int8`, `date`, `timestamp`) using btree compare procs.

On-disk tuple and (de)serialization
- Each tuple is the concatenation of fixed‑width key values in order. Offsets are computed from attribute lengths; advance by `MAXALIGN(attlen)` as needed. Store by‑val types with `store_att_byval`, otherwise `memcpy` fixed‑length by‑ref types. Varlen types are not supported.

C warnings and style
- GCC/Clang in PGXS environment warn on C90 mixed declarations; stylistically, move declarations to top of block when convenient. Keep functions `static` unless part of AM API.

Practical guidance
- If Codex is restarted, this section is the authoritative state snapshot. Bench scripts and GUCs allow reproducing results without large chat context. Read AGENT_PGIDXAM_NOTES.md carefully and confirm what files you have read.
- Comments were added throughout tricky code paths (compression layout, SIMD prefilter, suite runner, and Makefile targets) to explain intent, invariants, and safety. Keep comments in sync with code as you iterate; avoid change logs in comments (git tracks history).


# Notes on read-only indexes

i want to build a new index am for pg18 that's read-only. is it ok to simply throw/return an error on calls which require writes?  if so, please provide brief notes for my LLM coding agent to use when writing this for me. it wanted to setup triggers and that seems messy - but is that better for some reason?

Short answer: yes—make the AM itself fail any write-path entry points. You don’t need triggers, and they won’t attach to indexes anyway. Triggers fire on table DML and add complexity you don’t need unless you’re trying to “pre-empt” errors for UX reasons.

Here are tight notes for your LLM coding agent:

* # What “read-only” means for a PostgreSQL index AM

  * Implement a normal handler that returns an `IndexAmRoutine`, but treat every write/maintenance callback as unsupported at runtime. The AM API **requires** you to provide these callbacks; you can implement them as stubs that `ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED, ...)`. Required callbacks include `ambuild`, `ambuildempty`, `aminsert`, `ambulkdelete`, `amvacuumcleanup`, the scan suite, etc.
  * You still need `ambuild` to populate the index during `CREATE INDEX`. Everything after creation (inserts, updates, vacuum maintenance) should error out.

* # Minimum implementation sketch

  * Handler: return an `IndexAmRoutine` with realistic capability flags (e.g., `amcanorder`, `amcaninclude`, etc.) and function pointers. See “Basic API Structure for Indexes.”
  * `ambuild`: allow normal build from heap (this is DDL; permitted).
  * `ambuildempty`: implement normally (used for unlogged indexes init fork).
  * `aminsert`: `ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED, "read-only index AM does not support inserts");` (required to exist, but always errors).
  * `ambulkdelete` and `amvacuumcleanup`: also `ereport(ERROR, ...)` so VACUUM can’t mutate the structure. (They are required entry points; okay to error if the contract of the AM is read-only.)
  * Scan functions (`ambeginscan`, `amrescan`, `amgettuple`/`amgetbitmap`, `amendscan`): implement normally so SELECTs work. Which of `amgettuple` vs `amgetbitmap` you implement determines plain vs bitmap scan support.
  * `amcanreturn`: set to `NULL` if you don’t support index-only scans; otherwise implement per column.
  * `amcostestimate`, `amoptions`, `amvalidate`: supply simple, conservative versions so the planner can cost scans and options parse/validate.
  * SQL wrapper: `CREATE ACCESS METHOD my_readonly_am TYPE INDEX HANDLER my_readonly_am_handler;` (as documented under the index AM interface).

* # Error behavior vs. triggers

  * With this design, any table `INSERT/UPDATE/DELETE` that would touch the index hits your AM’s `aminsert` and raises a **clean “feature not supported”** error—simple and explicit.
  * Triggers are table-level and unrelated to index AMs; they can block the DML earlier for nicer UX (avoids failing deep in executor), but they’re “messy” (deployment, permissions, ordering) and don’t belong inside an AM. Prefer AM-level errors unless you must keep mixed-write tables from ever reaching the AM path. (Postgres docs show these AM functions are the proper contract boundary.)

* # Caveats to document

  * Only use this AM on tables you truly treat as read-only at the SQL level, or where you’re comfortable surfacing an error on any write that would touch indexed columns.
  * Consider setting planner cost high for updates isn’t applicable (planner costs are for scans); just keep scan costs realistic.
  * Make error messages precise (mention the AM name) to aid ops.

* # Testing checklist

  * `CREATE ACCESS METHOD` + `CREATE INDEX` on a sample table succeeds (build path works).
  * `INSERT/UPDATE/DELETE` on the indexed table raises your AM’s `FEATURE_NOT_SUPPORTED` error.
  * `SELECT` that can use the index produces correct results via your scan callbacks.
  * `VACUUM` should be a no-op on the index relation, since it's read-only.
Bench and Timeouts
- All benchmark scripts must wrap every psql invocation in a hard timeout to avoid hangs.
- Standard: use `timeout -k 5s ${TIMEOUT_SEC:-30}s psql ...` for both `-c` and heredoc invocations.
- Scripts should exit on timeout and print a clear error; default TIMEOUT_SEC is 30 seconds.

Inside-Container Workflow
- Run inside-* targets from the host via Docker exec, e.g.:
  - `docker exec -it smol make insidebuild` — clean build and install
  - `docker exec -it smol make insidestart` — initdb if needed and start PostgreSQL
  - `docker exec -it smol make insidecheck` — build, run regression (installcheck), then stop PG
  - `docker exec -it smol make insidebench-smol-btree-5m` — run the fair 5M-row IOS benchmark (leaves PG running)
- Use `docker exec -it smol make insidestop` to stop PostgreSQL when finished.
- Benchmark scripts honor `TIMEOUT_SEC` and `KILL_AFTER` env vars.

Profiling smol_gettuple
- Set `smol.profile = on` (superuser GUC) to log per-scan counters at endscan:
  - calls, rows returned, leaf pages visited, bytes copied, binary-search steps.
- Use these to identify hotspots in amgettuple; typical wins:
  - Eliminate per-row heap/tuple allocations and lock churn (done via prebuilt tuple + pinned pages).
  - Reduce small memcpy overhead by favoring constant-size copies (2/4/8 bytes).

## Session Playbook (Learn-and-Do)
- Never run regression and benchmarks at the same time. Before `make insidecheck`, ensure no bench run is active; if uncertain, stop PG (`make insidestop`), then `make insidestart` and rerun.
- Always set a server-side `statement_timeout` in benches for long statements (CREATE INDEX) to be <= client `TIMEOUT_SEC - 2s`. This prevents orphan backends when the client times out.
- For fair IOS benchmarking, VACUUM the base table (not the index): `VACUUM (ANALYZE, FREEZE, DISABLE_PAGE_SKIPPING) <table>` after index creation (BTREE arm). Keep `enable_seqscan=off`, `enable_bitmapscan=off`, `enable_indexonlyscan=on`.
- Scale benches under the 30s cap: prefer `ROWS=2e6..3e6` if `TIMEOUT_SEC=30`. If the goal is scaling, raise `TIMEOUT_SEC` only for bench runs; do not change regression timeouts.
- If `DROP DATABASE` hangs in installcheck: (1) inspect `pg_stat_activity` for lingering `CREATE INDEX` sessions; (2) terminate them; if stuck, (3) `pg_ctl -m immediate stop` then start; (4) rerun installcheck.
- Build path implementation rules: use radix sort for int2/int4/int8 keys and for (k1,k2) pairs; keep two-col collections in parallel arrays to avoid struct copies.
- While writing leaves, compute and store each leaf’s high key; build the root from these cached highkeys—do not re-read leaves to fetch tail keys.
- In two-col leaf packing, bulk-memcpy `k2` when key_len2=8; otherwise use tight fixed-width copies (2/4). Reuse a single scratch buffer per page to avoid repeated palloc/free.
- Link leaf right-siblings by keeping the previous leaf pinned: set its rightlink once the next leaf is allocated, then release; avoid reopen/lock of the previous leaf.
- To demonstrate multiplicative query speedups, make the workload I/O‑bound or enable parallel IOS: set `min_parallel_index_scan_size=0` and `max_parallel_workers_per_gather` to a modest value (e.g., 4) for both arms.
- When optimizing the scan hot path, specialize per type (int2/int4/int8) to eliminate key_len branches; hoist per-page/group invariants; prefetch next leaf via rightlink near page end.
- Improve `smol_costestimate` to reflect high tuple density and realistic page counts so the planner chooses SMOL-friendly IOS/parallel plans.
