AGENT_PGIDXAM_NOTES.md

Curated index access method reading list (~200 files) for PostgreSQL 18, with one‑line summaries. Grouped by area to accelerate AM development for the smol extension.

Core AM API and Glue
- postgres/src/include/access/amapi.h — IndexAmRoutine definition and AM capability flags.
- postgres/src/include/access/parallel.h — Parallel scan descriptor definitions for AMs.
- postgres/src/include/access/relation.h — Relation access-layer helpers used by AM code.
- postgres/src/backend/access/index/amapi.c — Calls AM handler to fetch IndexAmRoutine.
- postgres/src/backend/access/index/amvalidate.c — Common validation for AM opclasses/opfamilies.
- postgres/src/backend/access/index/genam.c — Generic index support (proc lookup, SortSupport, etc.).
- postgres/src/backend/access/index/indexam.c — Executor-facing AM wrappers: begin/rescan/getnext/bitmap.

Generic Access/Common Utilities
- postgres/src/backend/access/common/attmap.c — Attribute mapping helpers for tuple mapping.
- postgres/src/backend/access/common/bufmask.c — Buffer mask helpers for WAL consistency checks.
- postgres/src/backend/access/common/detoast.c — Detoasting utilities for varlena datums.
- postgres/src/backend/access/common/heaptuple.c — Heap tuple formation/manipulation routines.
- postgres/src/backend/access/common/indextuple.c — Index tuple formation and support.
- postgres/src/backend/access/common/printsimple.c — Simple tuple printing (debug/helpers).
- postgres/src/backend/access/common/printtup.c — Tuple printing to destinations.
- postgres/src/backend/access/common/relation.c — Open/close relation helpers.
- postgres/src/backend/access/common/reloptions.c — Parse and apply reloptions (AM options interface).
- postgres/src/backend/access/common/scankey.c — Initialize and manipulate ScanKey arrays.
- postgres/src/backend/access/common/session.c — Session-level storage helpers for access methods.
- postgres/src/backend/access/common/syncscan.c — Synchronized seqscan support (affects AM scan choices).
- postgres/src/backend/access/common/tidstore.c — TID store datastructure (bitmap/bitmap-like support).
- postgres/src/backend/access/common/toast_compression.c — TOAST compression helpers (context for detoasting).
- postgres/src/backend/access/common/toast_internals.c — TOAST internal helpers.
- postgres/src/backend/access/common/tupconvert.c — Tuple conversion utilities.
- postgres/src/backend/access/common/tupdesc.c — Tuple descriptor helpers.

Built-in AM: B-Tree (reference implementation of ordered AM)
- postgres/src/backend/access/nbtree/nbtcompare.c — B-Tree comparator functions for datatypes.
- postgres/src/backend/access/nbtree/nbtdedup.c — Leaf-level deduplication mechanics.
- postgres/src/backend/access/nbtree/nbtinsert.c — Insertion path and page splits.
- postgres/src/backend/access/nbtree/nbtpage.c — Page layout/manipulation for B-Tree pages.
- postgres/src/backend/access/nbtree/nbtpreprocesskeys.c — Preprocess scan keys for search.
- postgres/src/backend/access/nbtree/nbtree.c — AM handler and high-level routines.
- postgres/src/backend/access/nbtree/nbtsearch.c — Search and scan implementation.
- postgres/src/backend/access/nbtree/nbtsort.c — Build-time sort support.
- postgres/src/backend/access/nbtree/nbtsplitloc.c — Choose split location during page split.
- postgres/src/backend/access/nbtree/nbtutils.c — Miscellaneous helpers (metapage, key tests).
- postgres/src/backend/access/nbtree/nbtvalidate.c — Opclass/opfamily validator for btree.
- postgres/src/backend/access/nbtree/nbtxlog.c — WAL records and redo for btree.
- postgres/src/backend/access/nbtree/README — B-Tree implementation notes.

Built-in AM: GiST (generalized search trees)
- postgres/src/backend/access/gist/gist.c — AM handler and top-level routines.
- postgres/src/backend/access/gist/gistbuild.c — Build path for GiST.
- postgres/src/backend/access/gist/gistbuildbuffers.c — Build buffering infrastructure.
- postgres/src/backend/access/gist/gistget.c — Search and scan logic.
- postgres/src/backend/access/gist/gistproc.c — Generic methods calling user-defined procs.
- postgres/src/backend/access/gist/gistscan.c — Scan state management.
- postgres/src/backend/access/gist/gistsplit.c — Page split mechanics.
- postgres/src/backend/access/gist/gistutil.c — Utility functions (metapage, keys, etc.).
- postgres/src/backend/access/gist/gistvacuum.c — Vacuum support.
- postgres/src/backend/access/gist/gistvalidate.c — Opclass validator.
- postgres/src/backend/access/gist/gistxlog.c — WAL/redo for GiST.
- postgres/src/backend/access/gist/README — GiST implementation notes.

Built-in AM: GIN (inverted index)
- postgres/src/backend/access/gin/ginarrayproc.c — Array support procedures for GIN.
- postgres/src/backend/access/gin/ginbtree.c — BTree-like structure used within GIN.
- postgres/src/backend/access/gin/ginbulk.c — Bulk build helpers.
- postgres/src/backend/access/gin/gindatapage.c — Data page layout/ops.
- postgres/src/backend/access/gin/ginentrypage.c — Entry page layout/ops.
- postgres/src/backend/access/gin/ginfast.c — Fast update mechanism.
- postgres/src/backend/access/gin/ginget.c — Search and scan.
- postgres/src/backend/access/gin/gininsert.c — Insert path.
- postgres/src/backend/access/gin/ginlogic.c — Boolean logic for GIN.
- postgres/src/backend/access/gin/ginpostinglist.c — Posting list utilities.
- postgres/src/backend/access/gin/ginscan.c — Scan state and iteration.
- postgres/src/backend/access/gin/ginutil.c — AM handler; proc lookup and config.
- postgres/src/backend/access/gin/ginvacuum.c — Vacuum support.
- postgres/src/backend/access/gin/ginvalidate.c — Validate opclass/opfamily.
- postgres/src/backend/access/gin/ginxlog.c — WAL/redo for GIN.
- postgres/src/backend/access/gin/README — GIN implementation notes.

Built-in AM: SP-GiST (space-partitioned GiST)
- postgres/src/backend/access/spgist/spgdoinsert.c — Insert orchestration.
- postgres/src/backend/access/spgist/spginsert.c — Insert support / page ops.
- postgres/src/backend/access/spgist/spgkdtreeproc.c — Built-in kd-tree support procs.
- postgres/src/backend/access/spgist/spgproc.c — Core support procs and config.
- postgres/src/backend/access/spgist/spgquadtreeproc.c — Built-in quadtree support procs.
- postgres/src/backend/access/spgist/spgscan.c — Scan implementation.
- postgres/src/backend/access/spgist/spgtextproc.c — Text support procs.
- postgres/src/backend/access/spgist/spgutils.c — AM handler and utilities.
- postgres/src/backend/access/spgist/spgvacuum.c — Vacuum support.
- postgres/src/backend/access/spgist/spgvalidate.c — Opclass validator.
- postgres/src/backend/access/spgist/spgxlog.c — WAL/redo for SP-GiST.
- postgres/src/backend/access/spgist/README — SP-GiST design notes.

Built-in AM: BRIN (block range index)
- postgres/src/backend/access/brin/brin.c — AM handler and top-level routines.
- postgres/src/backend/access/brin/brin_bloom.c — Bloom opclass support for BRIN.
- postgres/src/backend/access/brin/brin_inclusion.c — Inclusion opclass support.
- postgres/src/backend/access/brin/brin_minmax.c — Minmax opclass support.
- postgres/src/backend/access/brin/brin_minmax_multi.c — Multi-minmax opclass.
- postgres/src/backend/access/brin/brin_pageops.c — Page operations.
- postgres/src/backend/access/brin/brin_revmap.c — Reverse map (heap->index) structure.
- postgres/src/backend/access/brin/brin_tuple.c — Tuple layout and ops.
- postgres/src/backend/access/brin/brin_validate.c — Opclass validator.
- postgres/src/backend/access/brin/brin_xlog.c — WAL/redo for BRIN.
- postgres/src/backend/access/brin/README — BRIN design notes.

Built-in AM: Hash
- postgres/src/backend/access/hash/hash.c — AM handler and top-level routines.
- postgres/src/backend/access/hash/hashfunc.c — Hash support functions.
- postgres/src/backend/access/hash/hashinsert.c — Insert path.
- postgres/src/backend/access/hash/hashovfl.c — Overflow page management.
- postgres/src/backend/access/hash/hashpage.c — Page operations.
- postgres/src/backend/access/hash/hashsearch.c — Search/scan implementation.
- postgres/src/backend/access/hash/hashsort.c — Build-time sort support.
- postgres/src/backend/access/hash/hashutil.c — Utilities (metapage etc.).
- postgres/src/backend/access/hash/hashvalidate.c — Opclass validator.
- postgres/src/backend/access/hash/hash_xlog.c — WAL/redo for Hash.
- postgres/src/backend/access/hash/README — Hash index notes.

Table AM, Heap, and Visibility (needed for IOS contracts)
- postgres/src/backend/access/heap/heapam.c — Heap table AM core.
- postgres/src/backend/access/heap/heapam_handler.c — Heap AM handler glue.
- postgres/src/backend/access/heap/heapam_visibility.c — Visibility rules for heap tuples.
- postgres/src/backend/access/heap/heapam_xlog.c — Heap AM WAL/redo.
- postgres/src/backend/access/heap/heaptoast.c — TOAST for heap.
- postgres/src/backend/access/heap/hio.c — Heap page insertion/extension.
- postgres/src/backend/access/heap/pruneheap.c — HOT pruning.
- postgres/src/backend/access/heap/rewriteheap.c — Rewrite support.
- postgres/src/backend/access/heap/vacuumlazy.c — Heap vacuum implementation.
- postgres/src/backend/access/heap/visibilitymap.c — Visibility map maintenance (needed for IOS).
- postgres/src/backend/access/heap/README.HOT — HOT design notes.
- postgres/src/backend/access/heap/README.tuplock — Tuple locking notes.

- postgres/src/backend/access/table/table.c — Table AM dispatcher.
- postgres/src/backend/access/table/tableam.c — Table AM implementation utilities.
- postgres/src/backend/access/table/tableamapi.c — TableAmRoutine handler retrieval.
- postgres/src/backend/access/table/toast_helper.c — TOAST helper wrappers.

Executor (Index scans and IOS)
- postgres/src/backend/executor/execAmi.c — AM-iterator integration (begin/rescan/getnext/bitmap).
- postgres/src/backend/executor/execIndexing.c — DML index maintenance helpers.
- postgres/src/backend/executor/execScan.c — Common scan node framework.
- postgres/src/backend/executor/execParallel.c — Executor parallel infrastructure.
- postgres/src/backend/executor/nodeIndexscan.c — Index Scan node implementation.
- postgres/src/backend/executor/nodeIndexonlyscan.c — Index Only Scan node.
- postgres/src/backend/executor/nodeBitmapIndexscan.c — Bitmap Index Scan node.
- postgres/src/backend/executor/README — Executor design notes.

Optimizer (paths, costs, and plans)
- postgres/src/backend/optimizer/path/allpaths.c — Generate base relation access paths.
- postgres/src/backend/optimizer/path/indxpath.c — Build IndexPath/IndexOnlyPath for indexes.
- postgres/src/backend/optimizer/path/costsize.c — Cost estimation for scans and joins.
- postgres/src/backend/optimizer/path/pathkeys.c — Pathkeys and ordering information.
- postgres/src/backend/optimizer/path/joinpath.c — Join path assembly (ordering interactions).
- postgres/src/backend/optimizer/plan/createplan.c — Turn paths into executable plans.
- postgres/src/backend/optimizer/plan/planmain.c — Planner interface and high-level flow.
- postgres/src/backend/optimizer/plan/analyzejoins.c — Join planning helpers.
- postgres/src/backend/optimizer/path/tidpath.c — TID paths (for comparison with bitmap/AM paths).
- postgres/src/backend/optimizer/plan/README — Planner design notes.

Catalog & Commands (DDL for indexes and AMs)
- postgres/src/backend/catalog/index.c — Index build orchestration and callbacks.
- postgres/src/backend/catalog/indexing.c — System catalog index management helpers.
- postgres/src/backend/commands/indexcmds.c — CREATE/DROP INDEX and options processing.
- postgres/src/backend/commands/amcmds.c — CREATE ACCESS METHOD and handler registration.
- postgres/src/backend/commands/opclasscmds.c — CREATE OPCLASS/OPFAMILY and validation.
- postgres/src/backend/commands/cluster.c — CLUSTER command using AM order.
- postgres/src/backend/commands/explain.c — EXPLAIN formatting (Index Only Scan label, etc.).

Contrib: Example and Inspection Extensions
- postgres/contrib/bloom/blcost.c — Bloom AM cost estimation hooks.
- postgres/contrib/bloom/blinsert.c — Bloom AM insert support.
- postgres/contrib/bloom/blscan.c — Bloom AM scan implementation.
- postgres/contrib/bloom/blutils.c — Bloom AM utilities.
- postgres/contrib/bloom/blvacuum.c — Bloom AM vacuum.
- postgres/contrib/bloom/blvalidate.c — Bloom AM opclass validator.
- postgres/contrib/bloom/bloom.h — Bloom index on-disk/layout definitions.
- postgres/contrib/bloom/bloom--1.0.sql — SQL definitions for Bloom AM and opclasses.
- postgres/contrib/bloom/bloom.control — Extension control file for Bloom.

- postgres/contrib/pageinspect/btreefuncs.c — Inspect B-Tree pages from SQL.
- postgres/contrib/pageinspect/brinfuncs.c — Inspect BRIN pages.
- postgres/contrib/pageinspect/gistfuncs.c — Inspect GiST pages.
- postgres/contrib/pageinspect/ginfuncs.c — Inspect GIN pages.
- postgres/contrib/pageinspect/hashfuncs.c — Inspect Hash pages.
- postgres/contrib/pageinspect/heapfuncs.c — Inspect Heap/VM/FSM pages (needed for IOS checks).
- postgres/contrib/pageinspect/rawpage.c — Generic page reader.

- postgres/contrib/amcheck/verify_nbtree.c — Check B-Tree invariants.
- postgres/contrib/amcheck/verify_gin.c — Check GIN invariants.
- postgres/contrib/amcheck/verify_common.c — Shared verification helpers.
- postgres/contrib/amcheck/verify_heapam.c — Heap checks (context for IOS correctness).
- postgres/contrib/amcheck/verify_common.h — Common verification headers.

Documentation (SGML) — AMs, extension hooks, and storage
- postgres/doc/src/sgml/indexam.sgml — Developer documentation for index access methods.
- postgres/doc/src/sgml/xindex.sgml — SQL reference for index management.
- postgres/doc/src/sgml/btree.sgml — B-Tree index documentation.
- postgres/doc/src/sgml/gin.sgml — GIN index documentation.
- postgres/doc/src/sgml/gist.sgml — GiST index documentation.
- postgres/doc/src/sgml/spgist.sgml — SP-GiST index documentation.
- postgres/doc/src/sgml/hash.sgml — Hash index documentation.
- postgres/doc/src/sgml/brin.sgml — BRIN index documentation.
- postgres/doc/src/sgml/bloom.sgml — Bloom contrib index documentation.
- postgres/doc/src/sgml/extend.sgml — Extensibility overview (AMs, FDWs, hooks).
- postgres/doc/src/sgml/generic-wal.sgml — Generic WAL facility for extensions.
- postgres/doc/src/sgml/wal-for-extensions.sgml — WAL patterns for extension developers.
- postgres/doc/src/sgml/storage.sgml — Storage subsystem overview.
- postgres/doc/src/sgml/arch-dev.sgml — Developer architecture guide.

Key Headers: Access Layer (include/access)
- postgres/src/include/access/genam.h — Generic index support functions and lookups.
- postgres/src/include/access/itup.h — Index tuple headers and macros.
- postgres/src/include/access/heapam.h — Heap AM API (visibility, scanning, DML hooks).
- postgres/src/include/access/reloptions.h — Reloptions API (AM options).
- postgres/src/include/access/relscan.h — Index/heap scan descriptor structs.
- postgres/src/include/access/skey.h — ScanKey and key comparison support.
- postgres/src/include/access/htup.h — Heap tuple definitions.
- postgres/src/include/access/htup_details.h — Heap tuple details/macros.
- postgres/src/include/access/stratnum.h — Strategy numbers for AM operator semantics.
- postgres/src/include/access/visibilitymap.h — Visibility map API.
- postgres/src/include/access/visibilitymapdefs.h — VM constants/defs.
- postgres/src/include/access/tableam.h — Table AM interface.
- postgres/src/include/access/nbtree.h — B-Tree on-disk format and API.
- postgres/src/include/access/nbtxlog.h — B-Tree WAL record definitions.
- postgres/src/include/access/hash.h — Hash index definitions.
- postgres/src/include/access/hash_xlog.h — Hash WAL records.
- postgres/src/include/access/gist.h — GiST API and structs.
- postgres/src/include/access/gist_private.h — GiST internal structures.
- postgres/src/include/access/gistxlog.h — GiST WAL records.
- postgres/src/include/access/gin.h — GIN API and structs.
- postgres/src/include/access/gin_private.h — GIN internal structures.
- postgres/src/include/access/ginxlog.h — GIN WAL records.
- postgres/src/include/access/spgist.h — SP-GiST API.
- postgres/src/include/access/spgist_private.h — SP-GiST internal structures.
- postgres/src/include/access/spgxlog.h — SP-GiST WAL records.
- postgres/src/include/access/amvalidate.h — Opclass validation helpers.
- postgres/src/include/access/tupdesc.h — Tuple descriptor definitions.
- postgres/src/include/access/tupdesc_details.h — Tuple descriptor helpers.
- postgres/src/include/access/attmap.h — Attribute mapping type.

Key Headers: Storage Layer (include/storage)
- postgres/src/include/storage/buf.h — Shared buffer handle type.
- postgres/src/include/storage/buf_internals.h — Buffer manager internals.
- postgres/src/include/storage/bufmgr.h — Buffer manager API (ReadBuffer, etc.).
- postgres/src/include/storage/bufpage.h — Page layout macros and item operations.
- postgres/src/include/storage/item.h — Item (line pointer) layout.
- postgres/src/include/storage/itemid.h — ItemId accessors.
- postgres/src/include/storage/itemptr.h — TID representation and macros.
- postgres/src/include/storage/relfilelocator.h — Physical relation file locator.
- postgres/src/include/storage/freespace.h — Free space map API.
- postgres/src/include/storage/indexfsm.h — Index FSM access.
- postgres/src/include/storage/smgr.h — Storage manager API.
- postgres/src/include/storage/md.h — md.c interface (smgr md implementation).
- postgres/src/include/storage/lmgr.h — Lock manager (relation/tuple locks).
- postgres/src/include/storage/lwlock.h — Lightweight lock primitives.
- postgres/src/include/storage/spin.h — Spinlocks.

Key Headers: Catalog, Nodes, Utils
- postgres/src/include/catalog/pg_am.h — Catalog for access methods (pg_am).
- postgres/src/include/catalog/pg_am.dat — Initial contents for pg_am.
- postgres/src/include/catalog/pg_amop.h — AM operators (pg_amop).
- postgres/src/include/catalog/pg_amop.dat — Initial contents for pg_amop.
- postgres/src/include/catalog/pg_amproc.h — AM support procs (pg_amproc).
- postgres/src/include/catalog/pg_amproc.dat — Initial contents for pg_amproc.
- postgres/src/include/catalog/pg_opclass.h — Operator classes (pg_opclass).
- postgres/src/include/catalog/pg_opclass.dat — Initial contents for pg_opclass.
- postgres/src/include/catalog/pg_opfamily.h — Operator families (pg_opfamily).
- postgres/src/include/catalog/pg_opfamily.dat — Initial contents for pg_opfamily.
- postgres/src/include/catalog/pg_index.h — Index catalog tuple definition.
- postgres/src/include/catalog/index.h — Catalog-level index helpers.
- postgres/src/include/catalog/indexing.h — System catalog indexing macros.
- postgres/src/include/catalog/namespace.h — Namespace helpers (DDL context).

- postgres/src/include/nodes/execnodes.h — Executor node state structs.
- postgres/src/include/nodes/pathnodes.h — Planner path nodes (IndexPath, etc.).
- postgres/src/include/nodes/plannodes.h — PlannedStmt and plan nodes (IndexScan, IOS).
- postgres/src/include/nodes/primnodes.h — Expression and scan key nodes.
- postgres/src/include/nodes/tidbitmap.h — Bitmap/TID data structures.

- postgres/src/include/utils/rel.h — Relation descriptor and helpers.
- postgres/src/include/utils/relcache.h — Relcache API.
- postgres/src/include/utils/lsyscache.h — System catalog cache lookups (opclass/opfamily, etc.).
- postgres/src/include/utils/selfuncs.h — Selectivity estimation API.
- postgres/src/include/utils/index_selfuncs.h — Index selectivity routines (btree, etc.).
- postgres/src/include/utils/sortsupport.h — Sort support for comparisons.
- postgres/src/include/utils/snapmgr.h — Snapshot management.
- postgres/src/include/utils/snapshot.h — Snapshot structs/macros.
- postgres/src/include/utils/fmgrtab.h — Builtin function table (for proc lookup IDs).
- postgres/src/include/utils/memutils.h — Memory contexts (palloc, per-tuple contexts).
- postgres/src/include/utils/palloc.h — Memory allocation wrappers.
- postgres/src/include/utils/elog.h — Error reporting/logging API.
 
Backend Utilities Often Consulted by AMs
- postgres/src/backend/utils/cache/lsyscache.c — Implementation of system catalog cache lookups.
- postgres/src/backend/utils/cache/relcache.c — Relation cache management (rd_amcache, opclass cache).
- postgres/src/backend/utils/cache/syscache.c — Syscache core (OID-based lookups used by AM plumbing).
- postgres/src/backend/utils/sort/sortsupport.c — SortSupport implementation (operator caching, abbrev keys).
- postgres/src/backend/utils/sort/tuplesort.c — Tuplesort implementation (used by index builds).
- postgres/src/backend/utils/adt/selfuncs.c — Generic selectivity estimation.
- postgres/src/backend/utils/adt/array_selfuncs.c — Array operator selectivity.
- postgres/src/backend/utils/adt/network_selfuncs.c — Network selectivity (for operator classes).
- postgres/src/backend/utils/adt/rangetypes_selfuncs.c — Range operator selectivity.


Additional Headers Commonly Touched by AMs
- postgres/src/include/postgres.h — Core defs and macros.
- postgres/src/include/c.h — System-wide C types and macros used by PG.
- postgres/src/include/miscadmin.h — Backend globals and GUC accessors.
- postgres/src/include/funcapi.h — Set-returning function API (useful for contrib tools).
- postgres/src/include/windowapi.h — Window API (contextual, rarely directly in AMs).

Secondary Files (planner/executor headers)
- postgres/src/include/executor/executor.h — Executor API.
- postgres/src/include/executor/execScan.h — Scan node framework API.
- postgres/src/include/executor/nodeIndexscan.h — IndexScan node prototypes.
- postgres/src/include/executor/nodeIndexonlyscan.h — IndexOnlyScan node prototypes.
- postgres/src/include/executor/nodeBitmapIndexscan.h — BitmapIndexScan prototypes.
- postgres/src/include/executor/instrument.h — Instrumentation API (buffers/timing for EXPLAIN).

Notes
- This list emphasizes AM API, planner/executor hooks, storage primitives, built-in AMs as reference, and contrib tools for inspection and validation. It intentionally includes both .c and .h where reading both adds clarity.
- For WAL and crash-safety, pair AM sources with their xlog headers (e.g., nbtxlog.h, ginxlog.h) and docs on generic WAL.

SMOL Implementation Notes (LLM‑oriented, dense)
- AM shape: read‑only, ordered, IOS‑only. No NULLs. Fixed‑width keys only. No TIDs on disk. Parallel scans planned (flag set; shared-state/chunking not implemented yet). No bitmap scans. No INCLUDE columns in v1. Planner should still build multi‑col indexes; smol will only ever return key attrs.
- IndexAmRoutine wiring (required fields):
  - amstrategies=5 (<, <=, =, >=, >); amsupport=1 (comparator proc=1); amoptsprocnum=0.
  - amcanorder=true; amcanorderbyop=false; amcanhash=false; amconsistentequality=true; amconsistentordering=true.
  - amcanbackward=true (serial); amcanparallel=true; amcanbuildparallel=false; amcaninclude=false.
  - amoptionalkey=true; amsearcharray=false; amsearchnulls=false; amstorage=false; amclusterable=false; ampredlocks=false; amusemaintenanceworkmem=false; amsummarizing=false; amparallelvacuumoptions=0; amkeytype=InvalidOid.
  - callbacks: ambuild, ambuildempty, aminsert(ERROR), ambulkdelete(NULL or stub), amvacuumcleanup(stub), amcanreturn (true for key attnos 1..nkeyatts), amcostestimate (simple placeholder), amgettreeheight (0 or derive from relpages), amoptions(NULL), amproperty(NULL), ambuildphasename(NULL), amvalidate(smolvalidate or NULL if opclass SQL is tight), amadjustmembers(NULL), ambeginscan, amrescan, amgettuple, amgetbitmap=NULL, amendscan, ammarkpos=NULL, amrestrpos=NULL, amestimateparallelscan, aminitparallelscan, amparallelrescan, amtranslatestrategy=NULL, amtranslatecmptype=NULL.

- On‑disk layout:
  - Metapage (blk 0): SmolMetaPageData { magic=SMOL_META_MAGIC, version=SMOL_META_VERSION, nkeyatts, natts==nkeyatts, dir_keylen (0 or 2/4/8 for first‑key directory), dir_count, dir_group, comp_algo } at page special or regular data with PageInit.
  - Data pages (blk >=1): items are raw packed key payloads, no per‑tuple header/null bitmap/TID. Each ItemId points to contiguous bytes of size row_key_size = sum(attlen[i]). Pack values back‑to‑back per row. No per‑attribute MAXALIGN needed when reading back via memcpy. Maintain pd_lower/pd_upper via PageAddItem.
  - Optional directory (meta v2+): per‑page or per‑group min/max of first key to enable page‑skip. Store in metapage payload; dir_keylen encodes first‑key width.
  - Optional compression (future): FOR+bitpack int4 for single‑col; comp_algo toggles; per‑page header marks frame base and bit width. Keep off by default.

- Build path (ambuild):
  - Input: heapRelation, indexRelation, IndexInfo.
  - Locking: table_index_build_scan acquires necessary visibility/scanning; no extra rel locks beyond index build path. Heap is read; index is written.
  - Collect rows via table_index_build_scan(heap, index, indexInfo,…) callback. In callback:
    - Enforce xs_want_itup is irrelevant here; instead enforce invariants: no NULLs on any key attr; error if any is NULL.
    - Enforce fixed‑width attrs: for each key attr, attlen > 0; if attlen < 0 (varlena), ereport(ERROR).
    - Copy datum payloads into an append buffer: for byval types (attbyval), store the raw Datum bytes of length attlen (2/4/8); for byref fixed‑len types, detoasting not needed, but copy attlen bytes so on‑page is self‑contained.
    - Store tuples in an array of {char* ptr or offset into big arena} or a single contiguous growable arena (palloc in TopMemoryContext or a short‑living context; avoid repalloc from size 0; pre‑grow geometrically).
  - Sort: qsort over pointers/offsets with comparator that applies opclass proc 1 per key attribute left‑to‑right; respect collation per key. Use SortSupport only if helpful; a simple fmgr comparator is adequate for N prototype. Fast‑paths: inline int2/int4/int8 comparisons to avoid fmgr for common types; honor ASC/DESC if supported later.
  - Write:
    - Write metapage if file is empty: ReadBufferExtended(P_NEW) on blk 0, PageInit, memcpy SmolMetaPageData, MarkBufferDirty, UnlockRelease.
    - Append data pages: for each row in sorted order, pack key bytes into a temp tuple buffer and PageAddItem; when no more space, allocate new P_NEW buffer, PageInit, continue. Maintain simple page‑local count if useful.
  - IOS contract hack: Ensure IndexOnlyScan never touches heap by pointing xs_heaptid to a fabricated TID on a heap page whose VM bit is set.
    - Steps to mark heap block 0 all‑visible:
      1) Buffer heapbuf = ReadBuffer(heapRel, 0); LockBuffer(heapbuf, BUFFER_LOCK_EXCLUSIVE);
      2) Page page = BufferGetPage(heapbuf); if (!PageIsAllVisible(page)) { PageSetAllVisible(page); MarkBufferDirty(heapbuf); }
      3) visibilitymap_pin(heapRel, 0, &vmbuf); visibilitymap_set(heapRel, 0, heapbuf, vmbuf, InvalidXLogRecPtr, vmbits);
      4) UnlockReleaseBuffer(vmbuf if valid); UnlockReleaseBuffer(heapbuf).
    - This ensures nodeIndexonlyscan.c checks VM and skips heap fetches for synthetic (0,1) TID.
  - Result: IndexBuildResult with heap_tuples and index_tuples counts.

- Build empty (ambuildempty):
  - Initialize metapage only in the init fork for unlogged or empty index creation. Similar to btree’s btbuildempty: allocate metapage, set magic/version/nkeyatts=0, MarkBufferDirty, UnlockRelease.

- Read‑only enforcement:
  - aminsert returns ereport(ERROR, "smol is read‑only"). No triggers needed; AM-level write entry points ERROR. Disallow ambulkdelete/amvacuumcleanup effects; return NULL/no‑op.

- Scan path (ambeginscan/rescan/amgettuple/amendscan):
  - ambeginscan(rel, nkeys, norderbys): allocate SmolScanOpaque in scan->opaque with:
    - natts, nkeyatts; per‑attr attlen[], attbyval[], attr collation OIDs.
    - precomputed row_key_size and per‑attr offsets[] into the on‑page payload.
    - xs_itupdesc = RelationGetDescr(index) (required by executor)
    - per‑scan memory context for forming tuples (small per‑tuple context optionally reset every smol.tuple_reset_period tuples).
    - parallel state hooks: if parallel, attach or init shared descriptor in aminitparallelscan.
  - amrescan: update scan keys; reset page/offset cursors; if backward, position at last tuple; if quals changed, recompute lower bound seek.
  - amgettuple(scan, dir): require scan->xs_want_itup else ereport(ERROR, "smol supports index‑only scans only"). Algorithm:
    1) If first call, perform lower‑bound seek: binary search across pages using meta directory (if available) or iterative page read: read last key on page, compare to scankeys; find first candidate page; set (blkno, itemidx).
    2) Iterate tuples on page: for each item, reconstruct xs_itup:
       - Allocate a Datum/isnull array of length nkeyatts (no INCLUDE).
       - For each attr i: base = itemPtr + offsets[i]; if byval: memcpy(attlen into local aligned word); datum = fetch; if byref fixed‑len: copy into short palloc’d chunk and pointer DatumSetPointer.
       - xs_itup = index_form_tuple(RelationGetDescr(index), values, isnull) in per‑tuple context.
       - xs_heaptid = synthetic ItemPointerSetBlockNumber(&tid, 0); ItemPointerSetOffsetNumber(&tid, 1).
       - If quals not met (for recheck or non‑key quals), skip; else return true.
    3) If page exhausted, advance to next page (forward) or prev (backward). For forward serial scans, prefetch next smol.prefetch_distance pages via ReadBufferExtended(RBM_NORMAL) with StrategyPrefetch.
    4) Stop at EOF/BOF.
  - amendscan: free opaque, pfree temp buffers, reset contexts.

- Parallel scan support:
  - amestimateparallelscan(index, nkeys, norderbys): return sizeof(ParallelSmolScanDesc) rounded to SHMEM alignment.
  - aminitparallelscan(target): initialize shared descriptor fields in DSM: next_page=first_candidate_page (compute from lower bound collectively or default 1), last_page=RelationGetNumberOfBlocks(index)-1, atomic state (LWLock or pg_atomic_uint32) and chunk size from GUC smol.parallel_chunk_pages. Each worker grabs a chunk via atomic fetch‑add; no per‑page locking needed; only buffer pins.
  - amparallelrescan(scan): reset shared next_page to first page; reset worker‑local state.
  - Backward scans: for parallel keep forward only; executor may request backward=false in parallel IOS. For serial backward, implement local decrement and avoid parallel codepath.

- Planner and IOS behavior:
  - amcanreturn(rel, attno): return true iff 1 <= attno <= nkeyatts (we can reconstruct all key columns).
  - amcostestimate: Basic model: startupCost ~ 0; totalCost ~ cpu_index_tuple_cost * ntuples + page_io_cost * pages_touched; selectivity from indxpath computed by clauses. Expose amgettreeheight=0 to indicate flat structure. Keep correlation neutral. Good enough to allow planner to consider SMOL; tune later.
  - amproperty/IndexAMProperty: Optional; not needed unless we want tailor‑made EXPLAIN properties.

- Operator classes:
  - One support proc at index_getprocinfo(index, i+1, 1) returning comparator: int32 compare(a, b) <0/0/>0. Use this for build sort and for scan bound comparisons.
  - Strategies: map <,<=,=,>=,> to CompareType via amtranslatestrategy if we wire it; else planner will use direct operators and our comparator for key ordering within build; for scan, smol_tuple_matches_keys consults scankeys directly invoking function OIDs.
  - Collation: use per‑attr collation from index rel attCollation[] if type is collation‑aware; comparator proc must respect it; pass to FmgrInfo via fmgr_info_collation.

- Safety & correctness invariants:
  - No NULLs allowed in any key column (enforced at build). If any, ereport(ERROR) with DETAIL pointing to column name.
  - Fixed‑width only: Form_pg_attribute.attlen > 0 for all key attrs; attlen < 0 rejected. No varlena.
  - IOS‑only: amgettuple must ERROR if !scan->xs_want_itup. amgetbitmap=NULL (planner will not request bitmap scans if amgetbitmap is NULL); IndexScan (non‑IOS) should be rejected similarly.
  - Synthetic TID must always reference a VM‑all‑visible page; keep using (0,1) after marking heap blk 0 as PD_ALL_VISIBLE + VM set during build.
  - Buffer/page API discipline: ReadBuffer/LockBuffer for writes; use P_NEW for extension; PageInit on fresh pages; MarkBufferDirty; UnlockReleaseBuffer.
  - Memory contexts: Avoid per‑tuple palloc churn by using a small per‑tuple context reset periodically (smol.tuple_reset_period). Precompute attlen/offsets and reuse arrays.

- Performance tactics (cheap, impactful):
  - Precompute attlen[], attbyval[], offsets[] once per scan; compute row_key_size and keep in opaque.
  - Fast‑path int2/int4/int8 compare: inline comparisons without fmgr for proc 1 when opfamily is int ops; this reduces qsort + scan costs significantly.
  - Light read‑ahead: prefetch smol.prefetch_distance pages ahead in serial forward scans.
  - Parallel chunking: allocate smol.parallel_chunk_pages to amortize atomics; prefetch within chunk.
  - Avoid repeated index_form_tuple allocations by caching a reusable buffer if TupleDesc stable; careful with varlena (we don’t support), so safe to memcpy into a stable buffer then heap_copy_tuple_as_datum if needed.

- Edge cases and error paths:
  - Empty index: ambuildempty creates metapage; ambuild with zero keys writes meta only; scans should immediately return no tuples.
  - DESC order: amcanbackward=true allows executor to request backward; in amgettuple honor dir=BackwardScanDirection; position initial cursor at last page/item.
  - Multi‑column: comparator must chain across columns; equality on all keys determines duplicates; stable sort not required but maintained by comparator’s 0 result behavior.
  - PAGE_FULL: when PageAddItem fails due to space, allocate new page; no right‑link maintenance needed; pages are independent and discovered by blkno.
  - VACUUM: ambulkdelete/amvacuumcleanup as no‑ops; index is read‑only; no dead items to prune.
  - SERIALIZABLE: ampredlocks=false may reduce SSI precision; acceptable for prototype. If needed, follow btree and set true with minimal implementation.

- Minimal cost model (amcostestimate sketch):
  - Inputs: path->indexselectivity, estimated clauses, root global costs.
  - pages = RelationGetNumberOfBlocks(index) - 1; tuples = reltuples estimate from pg_class.reltuples for index.
  - frac = clamp(path_selectivity, 0..1); pages_vis = ceil(frac * pages); tuples_vis = ceil(frac * tuples).
  - startup=0; total = pages_vis * random_page_cost * 0.1 + tuples_vis * cpu_index_tuple_cost; correlation=0; indexPages=pages.
  - Keep conservative to allow planner to pick SMOL for IOS with selective quals.

- Testing checklist (regression/bench):
  - CREATE EXTENSION smol; create tables with smallint/int4/int8; build indexes; run EXPLAIN (ANALYZE, BUFFERS) SELECT … WHERE col = const; expect Index Only Scan using smol.
  - Verify non‑IOS rejection: SELECT without columns in index target list in IndexScan should ERROR.
  - Verify NULLs rejected at build.
  - Verify DESC scans return correct order.
  - Verify multi‑col ordering matches btree for sample datasets.
  - Verify parallel scans: set max_parallel_workers, enable gather; ensure no duplicates/missed tuples; match sums vs serial.

- Key executor interactions to remember:
  - nodeIndexonlyscan.c calls index_getnext_tid (via amgettuple), then uses VM to check all‑visible; if yes, it constructs slot from xs_itup (our responsibility) without heap fetch; else it fetches heap; our synthetic TID trick prevents heap fetch.
  - RelationGetDescr(rel) assigned to scan->xs_itupdesc in ambeginscan is mandatory; btree does this unconditionally.
  - index_getprocinfo(index, keyattno, 1) provides opclass comparator; load once per scan into FmgrInfo[] with proper collation.

- Future: WAL/FSM integration (Generic WAL outline):
  - Use generic_xlog to WAL‑log page init and item insertions so crash‑safe. Wrap each new page in generic WAL buffer, register page, perform PageAddItem, finish record.
  - FSM: use indexfsm.c to get a free page; for SMOL’s append‑only build, a simple "always extend" is fine; for future updates, integrate FSM.
  - VACUUM: implement amvacuumcleanup to report reltuples/relpages stats; no pruning needed.

- Implementation map (names to implement/verify):
  - smolhandler(PG_FUNCTION_ARGS): returns palloc’d IndexAmRoutine with flags and function pointers as above.
  - ambuild: smol_build(); ambuildempty: smol_build_empty();
  - aminsert: smol_insert() → ERROR; ambulkdelete: smol_bulkdelete(NULL);
  - amvacuumcleanup: smol_vacuumcleanup(): return stats; amcanreturn: smol_canreturn();
  - amcostestimate: smol_costestimate(); amgettreeheight: smol_gettreeheight(); amoptions=NULL.
  - amvalidate (optional): smol_validate() if opclass definitions need checking beyond generic.
  - ambeginscan: smol_beginscan(); amrescan: smol_rescan(); amgettuple: smol_gettuple(); amendscan: smol_endscan(); amgetbitmap=NULL.
  - amestimateparallelscan: smol_estimateparallelscan(); aminitparallelscan: smol_initparallelscan(); amparallelrescan: smol_parallelrescan().
  - Metapage helpers: smol_meta_read(), smol_meta_init(); Page I/O: smol_extend_page(), smol_page_additem(); Compare: smol_compare_keys(); Tuple match: smol_tuple_matches_keys(); Seek: smol_seek_lower_bound().

- Pitfalls to avoid:
  - Don’t forget to set xs_itup for IOS; executor expects it when xs_want_itup is true.
  - Don’t allow amgettuple to run when xs_want_itup is false; enforce ERROR early.
  - Don’t call visibilitymap_set without locking the heap buffer and setting PD_ALL_VISIBLE first (follow documented sequence to avoid races).
  - Don’t store varlena/attlen<0; even "char(n)" (bpchar) is varlena; reject explicitly.
  - Don’t rely on misaligned loads; always memcpy out of page into aligned locals for byval widths.
  - Don’t implement amgetbitmap unless actually supported; leave NULL so planner won’t choose Bitmap Index Scan.

- Quick reload crib (flags):
  - amstrategies=5; amsupport=1; amcanorder=1; amcanbackward=1; amcanparallel=1; amcaninclude=0; amoptionalkey=1; amsearcharray=0; amsearchnulls=0; amgettuple!=NULL; amgetbitmap=NULL.
  - amcanreturn(attno 1..nkeyatts)=true; others false.
  - Synthetic TID=(0,1); VM set on heap blk 0 during build.
  - Disk: meta @blk0, data @blk>=1, each item size = sum(attlen[]), PageAddItem loop.
  - Parallel: global next_page atomic, chunk = smol.parallel_chunk_pages.
