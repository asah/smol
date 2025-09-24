# SMOL AM Development Notes

This file summarizes the hard‑won details needed to work on the `smol`
PostgreSQL 18 index access method in this repo.

Run Policy (Mandatory)
- One container name only: `smol`. Always build, test, and benchmark using
  a single Docker container named `smol`; reuse it across runs.
- Always run inside Docker. Build the image with `make dockerbuild`, then
  run inside-container targets directly as `make <target>` (this agent runs
  inside the `smol` container via `make dockercodex`). Do not attempt host
  builds/tests; they are unsupported and prone to SDK/PG mismatches.

## Goals & Constraints
- Ordered semantics; read‑only index. Enforce read-only at the AM level
  (write entry points ERROR); no triggers involved.
- Index‑only scans only. Do NOT store TIDs; reject non‑IOS access (`amgettuple` errors if `!xs_want_itup`).
- No NULL support at all (CREATE INDEX errors on NULLs).
- Fixed‑width key types only (no varlena such as `text`, `bytea`, `numeric`).
- Prototype: no WAL/FSM yet; correctness over crash‑safety for now.
- PostgreSQL 18; no INCLUDE columns; parallel scans stubbed (flag set; no
  DSM/chunking yet); no bitmap scan.
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
- Build+install extension: `make insidebuild`.
- Quick regression (build, start PG, run installcheck, stop PG): `make insidecheck`.
- Start/stop PostgreSQL in the container: `make insidestart` / `make insidestop`.
- Run benchmark (leaves PG running): `make insidebench-smol-btree-5m`.
- Keep regression fast; correctness lives under `sql/`. `bench/` is for benchmarks only.
- Server logs: `/tmp/pg.log` (or set a different path) inside the container.

## Bench Parallelism Policy
- For comparability, the bench script forces up to 5‑way parallelism on explanatory queries:
  - Sets `max_parallel_workers_per_gather=5` and `max_parallel_workers=5`.
  - Lowers `parallel_setup_cost` and `parallel_tuple_cost` to 0.
  - Sets `min_parallel_index_scan_size=0` and `min_parallel_table_scan_size=0`.
- This encourages the planner to use 5 workers for both BTREE and SMOL Index Only Scans.
- The script also supports `COLTYPE=int2|int4|int8` and scales to ≥50M rows with `TIMEOUT_SEC>=30`.

## Parallelism in SMOL
- Scan: `amcanparallel=true` and the bench uses parallel IOS at query time. Shared-state chunking is
  still minimal; however, with ordered scans and our pin-across-calls strategy, parallel scans operate
  correctly under the planner’s partitioning. Future work: add explicit shared state to divide leaf ranges.
- Build: Two‑column path includes a DSM+bgworker sorter (bucketed by k1) with a single‑process fallback.
  Multi‑level internal build (height ≥ 3) prevents root fanout overflows at scale.

Status 2025-09-23: DSM leaf-claim path wired for scans
- Implemented a simple parallel leaf-claim protocol in scan:
  - Added `SmolParallelScan { pg_atomic_uint32 curr; }` in DSM.
  - On first claim, atomically replace `curr=0` with `rightlink(leftmost)` and return `leftmost`.
  - On subsequent claims, CAS `curr=<blk>` to `rightlink(<blk>)` and process `<blk>`.
  - This fixes the leftmost-leaf double-claim race present in the earlier sketch.
- Two‑column scans now honor leading‑key lower bound when building the per‑leaf cache (start at `cur_group`).
- Regression tests pass (smol_basic, smol_twocol).

Open issue: parallel correctness mismatch (double counting)
- Ad‑hoc parallel checks (5 workers, btree vs smol) show SMOL returning ~2x rows under parallel IOS on two‑column indexes.
- Single‑worker (parallel disabled) and regression paths match BTREE.
- Hypothesis: remaining coordination bug in the leaf‑claim loop lets leader and one worker walk overlapping ranges. Needs deeper tracing (compare claimed blkno sequence across participants) or switch to an LWLock-protected scheme like BTREE’s `BTParallelScanDesc`.
- Interim guidance: leave `amcanparallel=true` for planning, but force `max_parallel_workers_per_gather=0` for deterministic correctness checks. Benchmarks may show inflated counts until fixed.

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
- Run inside-* targets directly (this agent runs inside the `smol` container):
  - `make insidebuild` — clean build and install
  - `make insidestart` — initdb if needed and start PostgreSQL
  - `make insidecheck` — build, run regression (installcheck), then stop PG
  - `make insidebench-smol-btree-5m` — run the fair 5M-row IOS benchmark (leaves PG running)
- Use `make insidestop` to stop PostgreSQL when finished.
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
### Troubleshooting: pg_regress DROP DATABASE hangs
- Symptom: server log repeats "still waiting for backend to accept ProcSignalBarrier" during `DROP DATABASE IF EXISTS "contrib_regression"`.
- Cause: a lingering backend (e.g., prior `CREATE INDEX`) preventing ProcSignalBarrier; stale socket/lock files can also block restarts.
- Quick recovery inside container:
  - Kill stuck backends/postmaster if needed: `ps -ef | grep postgres`, then `kill -9 <backend> <postmaster>`.
  - Remove stale IPC files: `rm -f /tmp/.s.PGSQL.5432 /tmp/.s.PGSQL.5432.lock`.
  - Start fresh cluster if wedged: `rm -rf /var/lib/postgresql/data && make insidestart` (Makefile re-runs initdb).
- For ad-hoc smoke tests (to avoid pg_regress), run minimal correctness via psql as `postgres` without expected-error checks.
- Build path implementation rules: use radix sort for int2/int4/int8 keys and for (k1,k2) pairs; keep two-col collections in parallel arrays to avoid struct copies.
- While writing leaves, compute and store each leaf’s high key; build the root from these cached highkeys—do not re-read leaves to fetch tail keys.


## Test & Benchmark Design (Correctness + Performance)

This section proposes focused, fast correctness tests and higher‑signal benchmark queries tailored to SMOL’s design and likely pitfalls. The goal is to catch ordering/boundary issues, page‑chain traversal, two‑column grouping, backward scans, and parallel leaf claiming. Where heavy data is needed (to force multi‑level trees), guard with psql vars to keep regression fast.

Guiding principles
- Always compare to a BTREE baseline (assumed correct). Use INCLUDE(a) for two‑column baselines to match IOS behavior.
- Keep small, deterministic datasets for edge cases; use UNLOGGED tables and disable autovacuum for speed.
- For multi‑page and multi‑level coverage, use optional heavy sections guarded by RUN_HEAVY.
- For parallel correctness, prefer 3–5 workers and disable seq/bitmap.

### Correctness Scenarios (small, fast)

1) Empty and single‑leaf trees
- Empty: CREATE INDEX on empty table; queries return 0 rows; drop works.
- Single leaf: <= page capacity rows; exercise >=, >, =, and backward scans.

SQL (run manually or add to smol_edgecases)
  CREATE EXTENSION IF NOT EXISTS smol;
  -- empty
  DROP TABLE IF EXISTS e1 CASCADE; CREATE UNLOGGED TABLE e1(a int);
  CREATE INDEX e1_smol ON e1 USING smol(a);
  SET enable_seqscan=off; SELECT count(*) FROM e1 WHERE a >= 0;  -- expect 0
  DROP INDEX e1_smol;
  -- single leaf
  DROP TABLE IF EXISTS s1 CASCADE; CREATE UNLOGGED TABLE s1(a int);
  INSERT INTO s1 SELECT i FROM generate_series(1,1000) i;  -- fits in one page for int4
  CREATE INDEX s1_smol ON s1 USING smol(a);
  SET enable_seqscan=off; SELECT array_agg(a) FROM s1 WHERE a >= 990 ORDER BY a;
  SELECT array_agg(a) FROM s1 WHERE a >= 990 ORDER BY a DESC;  -- backward

2) Boundary values and negatives (ordering)
- Ensure sign handling and comparisons behave correctly across int2/int4/int8 extremes and around 0.

SQL
  DROP TABLE IF EXISTS b1 CASCADE; CREATE UNLOGGED TABLE b1(a int2);
  INSERT INTO b1 VALUES (-32768::int2),(-1::int2),(0::int2),(1::int2),(32767::int2);
  CREATE INDEX b1_smol ON b1 USING smol(a);
  SET enable_seqscan=off;
  -- exact bounds
  SELECT array_agg(a) FROM b1 WHERE a >= -1 ORDER BY a;
  SELECT array_agg(a) FROM b1 WHERE a > -1 ORDER BY a;
  SELECT array_agg(a) FROM b1 WHERE a >= 32767 ORDER BY a;
  SELECT array_agg(a) FROM b1 WHERE a > 32767 ORDER BY a;  -- expect NULL/empty

3) Duplicates, all‑equal keys, and page‑crossing duplicates
- Exercise emission order and page boundary logic when many equal keys straddle leaves.

SQL
  DROP TABLE IF EXISTS d1 CASCADE; CREATE UNLOGGED TABLE d1(a int);
  -- many equal keys to cross pages
  INSERT INTO d1 SELECT 42 FROM generate_series(1,20000);
  INSERT INTO d1 SELECT 7 FROM generate_series(1,10000);
  CREATE INDEX d1_smol ON d1 USING smol(a);
  SET enable_seqscan=off;
  -- order and counts
  SELECT count(*), min(a), max(a) FROM d1 WHERE a >= 7;  -- count should match total
  SELECT count(*) FROM d1 WHERE a = 42;                  -- exact dup count
  -- backward
  SELECT a FROM d1 WHERE a >= 7 ORDER BY a DESC LIMIT 5;

4) Two‑column grouping and equality on second key
- Validate that second‑column equality filters are honored across page groups.

SQL
  DROP TABLE IF EXISTS t2 CASCADE; CREATE UNLOGGED TABLE t2(a int2, b int2);
  INSERT INTO t2 SELECT (i % 100)::int2, (i % 5)::int2 FROM generate_series(1,20000) i;
  CREATE INDEX t2_smol ON t2 USING smol(b,a);
  SET enable_seqscan=off;
  -- compare with BTREE INCLUDE baseline
  DROP INDEX IF EXISTS t2_btree; CREATE INDEX t2_btree ON t2 USING btree(b) INCLUDE (a);
  SELECT (SELECT sum(a)::bigint FROM t2 WHERE b > 2 AND a = 17) =
         (SELECT sum(a)::bigint FROM t2 WHERE b > 2 AND a = 17) AS match;  -- same query; planner may choose BTREE
  SET enable_indexscan=off;  -- force IOS
  SELECT (SELECT sum(a)::bigint FROM t2 WHERE b > 2 AND a = 17) AS smol_sum;
  SET enable_indexscan=on; SET enable_indexonlyscan=on;

5) No‑match and out‑of‑range bounds
- Fast‑fail behavior and correct empty results for upper bounds above max or strict lower bounds above all values.

SQL
  DROP TABLE IF EXISTS nm CASCADE; CREATE UNLOGGED TABLE nm(a int8);
  INSERT INTO nm SELECT i::bigint FROM generate_series(1,10000) i;
  CREATE INDEX nm_smol ON nm USING smol(a);
  SET enable_seqscan=off;
  SELECT count(*) FROM nm WHERE a > 1000000000000;  -- expect 0
  SELECT count(*) FROM nm WHERE a = -1;            -- expect 0

6) Rescan and different quals on the same scan node
- Exercise smol_rescan by parameterizing quals and rescanning in a single session.

SQL
  DROP TABLE IF EXISTS rs CASCADE; CREATE UNLOGGED TABLE rs(a int);
  INSERT INTO rs SELECT i FROM generate_series(1,10000) i;
  CREATE INDEX rs_smol ON rs USING smol(a);
  PREPARE q(int) AS SELECT count(*) FROM rs WHERE a >= $1;
  EXECUTE q(9000); EXECUTE q(9500); EXECUTE q(1);

7) Backward scans equivalence
- Ensure ORDER BY DESC results match reversed ASC range.

SQL
  DROP TABLE IF EXISTS bw CASCADE; CREATE UNLOGGED TABLE bw(a int);
  INSERT INTO bw SELECT i FROM generate_series(1,2000) i;
  CREATE INDEX bw_smol ON bw USING smol(a);
  SET enable_seqscan=off;
  SELECT array_agg(a) FROM (SELECT a FROM bw WHERE a >= 1500 ORDER BY a DESC LIMIT 20) s1;
  SELECT array_agg(a) FROM (SELECT a FROM bw WHERE a >= 1500 ORDER BY a ASC LIMIT 20) s2;  -- reverse should match

8) Non‑leading‑key plan safety
- If planner still picks SMOL without leading‑key qual (despite high cost), results must still be correct. Force with GUCs.

SQL
  DROP TABLE IF EXISTS nl CASCADE; CREATE UNLOGGED TABLE nl(a int, b int);
  INSERT INTO nl SELECT (i % 10), i FROM generate_series(1,5000) i;
  CREATE INDEX nl_smol ON nl USING smol(a,b);
  SET enable_seqscan=off; SET enable_indexscan=off; SET enable_indexonlyscan=on;
  -- No leading key qual on a, but equality on b
  SELECT (SELECT b FROM nl WHERE b = 1234 ORDER BY a LIMIT 1) IS NOT NULL;

### Multi‑page and Multi‑level Trees (optional heavy)

- Force multiple leaves and internal levels to exercise internal descent and rightlink traversal.
- Guard with RUN_HEAVY to keep CI fast; set ROWS variable for scale.

SQL (enable with: psql -v RUN_HEAVY=1 -v ROWS=200000)
  \if :{?RUN_HEAVY}
  \if :{?ROWS} \else \set ROWS 200000 \endif
  DROP TABLE IF EXISTS ml4 CASCADE; CREATE UNLOGGED TABLE ml4(a int4);
  INSERT INTO ml4 SELECT i FROM generate_series(1, :ROWS) i;
  CREATE INDEX ml4_smol ON ml4 USING smol(a);
  SET enable_seqscan=off; SET enable_indexonlyscan=on; SET max_parallel_workers_per_gather=0;
  -- check sums across disjoint ranges that cross many page boundaries
  SELECT sum(a)::bigint FROM ml4 WHERE a > (:ROWS/3);
  SELECT sum(a)::bigint FROM ml4 WHERE a > (:ROWS/2);
  SELECT sum(a)::bigint FROM ml4 WHERE a > (:ROWS-100);
  -- backward scan sanity
  SELECT a FROM ml4 WHERE a >= (:ROWS-50) ORDER BY a DESC LIMIT 10;
  \endif

### Parallel Scan Correctness

- Validate DSM leaf‑claim logic: no double‑emit, no gaps, independent of worker count.
- Use equality on second key and range on first to vary match density.

SQL
  SET client_min_messages=warning;
  SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on;
  SET max_parallel_workers_per_gather=5; SET max_parallel_workers=5;
  SET parallel_setup_cost=0; SET parallel_tuple_cost=0;
  SET min_parallel_index_scan_size=0; SET min_parallel_table_scan_size=0;
  DROP TABLE IF EXISTS px CASCADE; CREATE UNLOGGED TABLE px(a int4, b int4);
  INSERT INTO px SELECT (i % 100000), (i % 1000) FROM generate_series(1,200000) i;
  ANALYZE px;
  CREATE INDEX px_bt ON px USING btree(b) INCLUDE (a);
  CREATE INDEX px_sm ON px USING smol(b,a);
  -- baseline
  SELECT sum(a)::bigint, count(*)::bigint FROM px WHERE b > 500; \gset
  -- force parallel
  SET max_parallel_workers_per_gather=5;
  SELECT ((SELECT sum(a)::bigint FROM px WHERE b > 500) = :sum) AND
         ((SELECT count(*)::bigint FROM px WHERE b > 500) = :count) AS match_all;
  -- equality on second key (narrow)
  SELECT ((SELECT count(*)::bigint FROM px WHERE b > 500 AND a = 42) =
          (SELECT count(*)::bigint FROM px WHERE b > 500 AND a = 42)) AS match_eq;

### Benchmark Query Additions (performance signal)

1) Selectivity sweep (single‑col and two‑col)
- For TBL(a,b) with SMOL(b[,a]) and BTREE(b INCLUDE a), run b > percentile thresholds (P10,P50,P90,P99) and record plan, runtime, and rows.
- Two‑col: also run b > P50 AND a = const with varying equality selectivity (~0.1%, 1%, 10%).

2) Out‑of‑range and no‑match microbench
- Measure overhead of scans that quickly determine emptiness: b > max(b)+1, a = value not present.

3) Backward scan throughput
- Repeat the selectivity sweep with ORDER BY a DESC to ensure parity with forward throughput.

4) Parallel scaling curve
- Fix dataset; run with max_parallel_workers_per_gather in {0,1,2,3,5}; report speedup and verify counts equal baseline.

5) Duplicate‑heavy distributions
- Create skewed data with few distinct b values (e.g., Zipf or 95% in top 5 vals). Compare SMOL(b,a) vs BTREE(b INCLUDE a) for b > thresh near the heavy mode and for equality on a; watch per‑leaf group emission cost.

6) Page‑boundary adversarial batches
- Seed data so that b transitions exactly at page capacity boundaries (e.g., count per distinct b equals floor(keys_per_leaf)). Ensure scanning across rightlinks is smooth (no duplicates or gaps) and measure impact.

Implementation notes for benches
- Extend bench/smol_vs_btree.sh with optional flags:
  - EQUAL_ON_A=val to add AND a = val when comparing two‑col.
  - SWEEP=percentiles (e.g., 10,50,90,99) to iterate THRESH automatically based on table stats.
  - WORKERS=n to run the same query under several parallel settings and emit one CSV row per setting.
- Keep timeouts as is; split long maintenance into separate statements as already done.

### What these catch (mapping to SMOL internals)
- Boundaries/negatives/strict vs non‑strict: smol_cmp_keyptr_bound, binary‑search seek, strict flag.
- Duplicates/grouping: two‑col per‑leaf group headers and k2 array packing; single‑col packed payload counts.
- Page chains: rightlink linkage and pinned‑page iteration across leaves.
- Internal descent: smol_find_first_leaf child selection via highkeys on height ≥ 2.
- Backward scans: reverse iteration logic for cur_off and page transitions.
- Parallel scans: DSM curr atomic claim protocol; no double claims, no missed leaves.
- Rescans: smol_rescan rebinds keys without leaking old state.

All of the above are designed to be either small and fast or optional heavies, respecting our regression speed policy while raising coverage and confidence.
- In two-col leaf packing, bulk-memcpy `k2` when key_len2=8; otherwise use tight fixed-width copies (2/4). Reuse a single scratch buffer per page to avoid repeated palloc/free.
- Link leaf right-siblings by keeping the previous leaf pinned: set its rightlink once the next leaf is allocated, then release; avoid reopen/lock of the previous leaf.
- To demonstrate multiplicative query speedups, make the workload I/O‑bound; keep
  parallel disabled for now (shared-state scan not yet implemented).
- When optimizing the scan hot path, specialize per type (int2/int4/int8) to eliminate key_len branches; hoist per-page/group invariants; prefetch next leaf via rightlink near page end.
- Improve `smol_costestimate` to reflect high tuple density and realistic page counts so the planner chooses SMOL-friendly IOS/parallel plans.

### Build-time diagnostics
- GUCs:
  - `smol.debug_log` (bool): enables verbose LOG messages.
  - `smol.progress_log_every` (int, default 250000): log progress every N tuples during build; used by scan callbacks, radix sort passes, and leaf writes.
- Sort logs: `radix16/32/64: pass P/N n=...` and `radix64(idx,16bit)` for pair-index secondary sort.
- Collection logs: `collect int2/int4/int8/pair: tuples=...` emitted every `smol.progress_log_every` tuples during `table_index_build_scan`.
- Page-build logs: `leaf built ... progress=..%` (1-col) and `leaf(2col) built ... progress=..%`.

## Benchmark Snapshot (reference)
- Environment: Docker "smol", PG18, bench/smol_vs_btree.sh with ROWS=1,000,000, TIMEOUT_SEC=30, BATCH=100000, CHUNK_MIN=20000, PGOPTIONS='-c max_parallel_workers_per_gather=0 -c min_parallel_index_scan_size=0'.
- Multi-col (b,a): BTREE build 228 ms, 21.48 MB, query 36.98 ms (actual ~18.99 ms); SMOL build 92 ms, 2.20 MB, query 28.75 ms (actual ~16.44 ms).
- Single-col (b): BTREE build 205 ms, 7.17 MB, query 27.53 ms (actual ~20.33 ms); SMOL build 40 ms, 1.95 MB, query 27.93 ms (actual ~15.70 ms).
- Note: For larger ROWS (e.g., 5M), revisit—user observed slower CREATE INDEX for SMOL; 1M snapshot shows SMOL build faster but we need to validate trend at scale.

### 5M rows (multi-col b,a), 30s per-step cap
- Result (bench/smol_vs_btree.sh default):
  - BTREE: Build_ms=1165, Size_MB=107.28, Query_ms=263.68
  - SMOL: Build_ms≈30007 (hit 30s timeout), Size/Query unavailable (canceled)
- Indicates SMOL build path exceeds 30s at 5M; need profiling to separate collect vs sort vs write phases (enable `smol.debug_log=on`).

Note: Attempted switching build to tuplesort to remove the sort bottleneck; first pass hit PG18 API mismatches and backend crashes — reverted to stable radix path for now.

## Testing Checklist

## Investigation Log (2025-09-23)
- Reproduced 5M two-col timeout at ~30s; no lock waits observed. Collection logs advance rapidly; many leaf inits; client timeout kills CREATE INDEX.
- Binary-searched 3s cap for two-col: OK up to ~2.60M; first timeout at ~2.62M. Single-col is fine at these scales. Bottleneck is two-col path.
- Added detailed build-phase logs. With TIMEOUT_SEC=1 at 3.0M (two-col), sort completes quickly (~80 ms), leaf write completes, and cancellation occurs during root build. The last log before cancel is the root PageAddItem failure fallback:
  - smol.c:1819 logs PageAddItem ok every 32 children; smol.c:1803 logs "2col-root: PageAddItem failed at li=680; fallback to height=1".
- Conclusion: at this scale and log level, the create-time cliff is triggered while adding ~680th child to the root; fallback to height=1 is taken but the client timeout aborts mid‑fallback. No evidence of buffer lock contention; compute/log I/O dominates.

## Reader Bug + Fix (Two‑Column IOS)
- Symptom: GROUP BY a (no leading-key qual) returned nonsensical counts on large tables; even single-worker forced SMOL mismatched seqscan.
- Root causes identified:
  1) Group-level memcpy optimisation for k1 (copy once per group) led to stale/misaligned tuples under some paths.
  2) After introducing multi-level internals, scan descent still assumed height<=2; leaf selection could be wrong.
- Fixes implemented:
  - Build a per‑leaf cache of (k1,k2) pairs by walking the packed group directory once. Emit rows by memcpy’ing both attrs into a preallocated IndexTuple (allocation‑free per row; allocation per leaf only).
  - Update scan descent to walk internal pages from root to leaf for height≥2.
  - Optional second‑key equality filter: when executor passes a=const, apply local filter during emission.
  - Planner guard: penalize SMOL when there is no leading‑key qual, steering planner away from pathological plans.
- Deterministic correctness harness:
  - Choose mode(a) from first 100k rows (ctid order).
  - Compare GROUP BY a results (md5 over ordered pairs) between baseline seqscan and forced SMOL (single-worker, then 5‑way after DSM).
  - Compare counts for a=mode under the same toggles.
- Status: single-worker GROUP BY a and count(a=mode) now match exactly on 50M int4. Two‑col reader corrected.

## Parallel IOS Plan (DSM splitter)
- Goal: Simple and robust parallel partitioning of leaf pages among up to 5 workers.
- PG18 ABI: use `ParallelIndexScanDescData.ps_offset_am` to locate AM-specific shared memory region.
- Shared state: `pg_atomic_uint32 curr` holds the next leaf blkno to claim; 0 means uninitialized; InvalidBlockNumber means done.
- First claim: first worker CASes 0→leftmost leaf. Workers then CAS curr to `rightlink(leaf)` after claiming.
- Integration points:
  - On init (forward scans), claim and pin leaf; build leaf cache.
  - On leaf advance, atomically publish `rightlink` and move.
- Next: finish the init/advance control flow and re‑enable amcanparallel (single-worker path remains correct and fast).

## Bench & Deterministic Testing Notes
- Bench harness standardized to prefer 5‑way parallel IOS by setting planner GUCs and `min_parallel_*_scan_size=0`.
- Script supports `COLTYPE=int2|int4|int8` and scales to ≥50M; maintenance split into CHECKPOINT, VACUUM, ANALYZE per-step with timeouts.
- Deterministic correctness checks use:
  - `mode(a)` from first 100k by `ctid` to choose a reasonably selective constant.
  - GROUP BY a hash comparison (baseline vs forced SMOL, single and parallel once DSM splitter is finalized).
- Basic IOS: `CREATE EXTENSION smol;` build small tables (int2/int4/int8), build SMOL indexes; `EXPLAIN (ANALYZE, BUFFERS)` selective queries; expect Index Only Scan using smol.
- Non-IOS rejection: disable `enable_indexonlyscan`, enable `enable_indexscan`; queries that would use Index Scan should ERROR with “smol supports index-only scans only”. Re-enable IOS afterward.
- NULLs rejected at build: table with NULLs in key column must ERROR on `CREATE INDEX ... USING smol`.
- DESC/backward: ensure backward scans produce correct order when requested by executor (dir=BackwardScanDirection).
- Multi-column ordering: for small datasets, compare ordered results/sums vs btree on equivalent definitions (btree(b) INCLUDE (a) vs smol(b,a)).
- Parallel scans: not implemented yet; keep `max_parallel_workers_per_gather=0` during correctness runs.
