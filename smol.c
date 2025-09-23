/*
 * smol.c (restart, minimal skeleton)
 *
 * Fresh, simplified scaffold for the SMOL index AM. The full previous
 * implementation has been moved to smol-old.c for reference only.
 * This file provides a compilable handler and stubs to enable iterative
 * development in small steps.
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "access/visibilitymapdefs.h"
#include "access/nbtree.h"
#include "catalog/index.h"
#include "access/xlogdefs.h"
#include "fmgr.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "port/atomics.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include <string.h>
#include "utils/memutils.h"
#include "utils/rel.h"
#include "nodes/pathnodes.h"
#include "utils/lsyscache.h"
#include "access/tupmacs.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"
#include "portability/instr_time.h"
/* Parallel build support */
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"

PG_MODULE_MAGIC;

/* ---- Lightweight logging ------------------------------------------------ */
#ifndef SMOL_TRACE
#define SMOL_TRACE 1
#endif

static bool smol_debug_log = false; /* toggled by GUC smol.debug_log */
static bool smol_profile_log = false; /* toggled by GUC smol.profile */
static int  smol_progress_log_every = 250000; /* GUC: log progress every N tuples */
static int  smol_wait_log_ms = 500; /* GUC: log waits longer than this (ms) */
extern int maintenance_work_mem;

#if SMOL_TRACE
#define SMOL_LOG(msg) \
    do { if (smol_debug_log) elog(LOG, "[smol] %s:%d: %s", __func__, __LINE__, (msg)); } while (0)
#define SMOL_LOGF(fmt, ...) \
    do { if (smol_debug_log) elog(LOG, "[smol] %s:%d: " fmt, __func__, __LINE__, __VA_ARGS__); } while (0)
#else
#define SMOL_LOG(msg)        ((void) 0)
#define SMOL_LOGF(fmt, ...)  ((void) 0)
#endif

void _PG_init(void);

void
_PG_init(void)
{
    DefineCustomBoolVariable("smol.debug_log",
                             "Enable verbose SMOL logging",
                             "When on, SMOL emits detailed LOG messages for tracing.",
                             &smol_debug_log,
                             false,
                             PGC_SUSET,
                             0,
                             NULL, NULL, NULL);

    DefineCustomBoolVariable("smol.profile",
                             "Log per-scan microprofile counters",
                             "When on, SMOL logs counters for amgettuple hot path (pages, rows, copies).",
                             &smol_profile_log,
                             false,
                             PGC_SUSET,
                             0,
                             NULL, NULL, NULL);

    DefineCustomIntVariable("smol.progress_log_every",
                            "Emit progress logs every N tuples during build",
                            "When smol.debug_log is on, log progress during scan/sort/build at this interval.",
                            &smol_progress_log_every,
                            250000, /* default */
                            1000, /* min */
                            100000000, /* max */
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.wait_log_ms",
                            "Log any single wait > N milliseconds",
                            "Applies to buffer locks and bgworker waits in build path.",
                            &smol_wait_log_ms,
                            500,
                            0,
                            60000,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);
}

/* forward decls */
PGDLLEXPORT Datum smol_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(smol_handler);

/* --- Minimal prototypes for IndexAmRoutine --- */
static IndexBuildResult *smol_build(Relation heap, Relation index, struct IndexInfo *indexInfo);
static void smol_buildempty(Relation index);
static bool smol_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                        Relation heapRel, IndexUniqueCheck checkUnique, bool indexUnchanged,
                        struct IndexInfo *indexInfo);
static IndexScanDesc smol_beginscan(Relation index, int nkeys, int norderbys);
static void smol_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
static bool smol_gettuple(IndexScanDesc scan, ScanDirection dir);
static void smol_endscan(IndexScanDesc scan);
static bool smol_canreturn(Relation index, int attno);
static Size smol_estimateparallelscan(Relation index, int nkeys, int norderbys);
static void smol_initparallelscan(void *target);
static void smol_parallelrescan(IndexScanDesc scan);
static void smol_costestimate(struct PlannerInfo *root, struct IndexPath *path, double loop_count,
                              Cost *indexStartupCost, Cost *indexTotalCost,
                              Selectivity *indexSelectivity, double *indexCorrelation,
                              double *indexPages);
static struct IndexBulkDeleteResult *smol_vacuumcleanup(struct IndexVacuumInfo *info,
                                                        struct IndexBulkDeleteResult *stats);

/* --- On-disk structs (prototype, 1- or 2-column fixed-width ints) --- */
#define SMOL_META_MAGIC   0x534D4F4CUL /* 'SMOL' */
#define SMOL_META_VERSION 1

typedef struct SmolMeta
{
    uint32      magic;
    uint16      version;
    uint16      nkeyatts;   /* 1 or 2 (prototype) */
    uint16      key_len1;   /* bytes for k1 */
    uint16      key_len2;   /* bytes for k2 (0 if single-col) */
    BlockNumber root_blkno; /* root can be a leaf if height==1 */
    uint16      height;     /* 1=leaf root, 2=root+leaves */
} SmolMeta;

typedef struct SmolPageOpaqueData
{
    uint16      flags;     /* 1=leaf, 2=internal */
    BlockNumber rightlink; /* next leaf (for leaf pages), or InvalidBlockNumber */
} SmolPageOpaqueData;

#define SMOL_F_LEAF     0x0001
#define SMOL_F_INTERNAL 0x0002

typedef struct SmolInternalItem
{
    int32       highkey;   /* highest key in child */
    BlockNumber child;
} SmolInternalItem;

/* Leaf reference used during build */
typedef struct SmolLeafRef
{
    BlockNumber blk;
} SmolLeafRef;

static inline SmolMeta *
smol_meta_ptr(Page page)
{
    return (SmolMeta *) PageGetContents(page);
}

static inline SmolPageOpaqueData *
smol_page_opaque(Page page)
{
    return (SmolPageOpaqueData *) PageGetSpecialPointer(page);
}

/* --- Scan opaque --- */
/*
 * SmolScanOpaqueData
 * -------------------
 * Per-scan state used by amgettuple().
 *
 * Key ideas for performance:
 * - We never lock index buffers during scan: the index is read-only after
 *   build, so no page content can change. We do keep pages pinned to ensure
 *   the memory remains valid while reading.
 * - We hold a pin on the current leaf page across calls and only release it
 *   when moving to the next/prev leaf. This avoids per-row ReadBuffer calls.
 * - We prebuild a single IndexTuple (no NULLs, no varlena) and simply copy
 *   fixed-width integers into its data area per row. That removes per-row
 *   palloc/formatting overhead from index_form_tuple().
 */
typedef struct SmolScanOpaqueData
{
    bool        initialized;    /* positioned to first tuple/group? */
    BlockNumber cur_blk;        /* current leaf blkno */
    OffsetNumber cur_off;       /* 1-based item index for single-col */

    /* pinned buffer for current leaf (no locking needed: index is read-only) */
    Buffer      cur_buf;
    bool        have_pin;

    /* optional lower bound on leading key (>=, >, =) */
    bool        have_bound;
    bool        bound_strict;   /* true when '>' bound (not >=) */
    int64       bound;          /* normalized to int64 for comparisons */
    /* optional equality filter on second key (attno=2) */
    bool        have_k2_eq;
    int64       k2_eq;

    /* type/width info (leading key always present; second key optional) */
    Oid         atttypid;       /* INT2OID/INT4OID/INT8OID */
    Oid         atttypid2;      /* second column type if 2-col, else InvalidOid */
    uint16      key_len;        /* k1 bytes (2/4/8) */
    uint16      key_len2;       /* k2 bytes (0 if single-col) */
    bool        two_col;        /* true when scanning (k1,k2) index */

    /* two-col iteration state */
    uint16      cur_group;      /* 0-based group index within page */
    uint32      pos_in_group;   /* 0-based position within current group */

    /* prebuilt index tuple reused for IOS */
    IndexTuple  itup;           /* allocated once in beginscan */
    char       *itup_data;      /* pointer to data area inside itup */
    uint16      itup_off2;      /* second-attr offset from data (two-col), else 0 */
    char        align1;         /* typalign for attr1 */
    char        align2;         /* typalign for attr2 (if any) */

    /* lightweight profiling (enabled by smol.profile) */
    bool        prof_enabled;
    uint64      prof_calls;     /* amgettuple calls */
    uint64      prof_rows;      /* rows returned */
    uint64      prof_pages;     /* leaf pages visited */
    uint64      prof_bytes;     /* bytes copied into tuples */
    uint64      prof_bsteps;    /* binary-search steps */

    /* two-col per-leaf cache to simplify correct emission */
    int64      *leaf_k1;
    int64      *leaf_k2;
    uint32      leaf_n;
    uint32      leaf_i;
} SmolScanOpaqueData;
typedef SmolScanOpaqueData *SmolScanOpaque;

/* Parallel scan shared state (DSM):
 * curr == 0            => uninitialized; first worker sets to leftmost leaf blkno
 * curr == InvalidBlockNumber => all leaves claimed
 * otherwise curr holds the next leaf blkno to claim; workers atomically swap to its rightlink. */
typedef struct SmolParallelScan
{
    pg_atomic_uint32 curr;
} SmolParallelScan;

/* Utilities */
static void smol_meta_read(Relation idx, SmolMeta *out);
static void smol_mark_heap0_allvisible(Relation heapRel);
static Buffer smol_extend(Relation idx);
static void smol_init_page(Buffer buf, bool leaf, BlockNumber rightlink);
static void smol_build_tree_from_sorted(Relation idx, const void *keys, Size nkeys, uint16 key_len);
static void smol_build_tree2_from_sorted(Relation idx, const int64 *k1v, const int64 *k2v,
                             Size nkeys, uint16 key_len1, uint16 key_len2);
static void smol_build_internal_levels(Relation idx,
                                       BlockNumber *child_blks, const int64 *child_high,
                                       Size nchildren, uint16 key_len,
                                       BlockNumber *out_root, uint16 *out_levels);
static BlockNumber smol_find_first_leaf(Relation idx, int64 lower_bound, Oid atttypid, uint16 key_len);
static BlockNumber smol_rightmost_leaf(Relation idx);
static BlockNumber smol_prev_leaf(Relation idx, BlockNumber cur);
static Datum smol_read_key_as_datum(Page page, OffsetNumber off, Oid atttypid, uint16 key_len);
static int smol_cmp_keyptr_bound(const char *keyp, uint16 key_len, Oid atttypid, int64 bound);
static bytea *smol_options(Datum reloptions, bool validate);
static bool smol_validate(Oid opclassoid);
static uint16 smol_leaf_nitems(Page page);
static char *smol_leaf_keyptr(Page page, uint16 idx, uint16 key_len);
/* Two-column helpers */
static uint16 smol2_leaf_ngroups(Page page);
static char *smol2_group_k1_ptr(Page page, uint16 g, uint16 key_len1);
static void smol2_group_offcnt(Page page, uint16 g, uint16 key_len1, uint32 *off, uint16 *cnt);
static char *smol2_k2_base(Page page, uint16 key_len1, uint16 ngroups);
static char *smol2_k2_ptr(Page page, uint32 idx, uint16 key_len2, uint16 key_len1, uint16 ngroups);
static char *smol2_last_k1_ptr(Page page, uint16 key_len1);
static int smol_cmp_k1_group(Page page, uint16 g, uint16 key_len1, Oid atttypid, int64 bound);
static void smol2_build_leaf_cache(SmolScanOpaque so, Page page);

/* Page summary (diagnostic) */
static void smol_log_page_summary(Relation idx);

/* Sorting helpers for build path */
static void smol_sort_int16(int16 *keys, Size n);
static void smol_sort_int32(int32 *keys, Size n);
static void smol_sort_int64(int64 *keys, Size n);
static void smol_sort_pairs64(int64 *k1, int64 *k2, Size n);
static void smol_sort_pairs_rows64(int64 *k1, int64 *k2, Size n);
static void smol_sort_pairs16_16_packed(int64 *k1v, int64 *k2v, Size n);
PGDLLEXPORT void smol_parallel_sort_worker(Datum arg);

/* DSM layout for parallel two-column sort */
typedef struct SmolParallelHdr
{
    uint32      magic;
    uint32      nbuckets;      /* number of k1 high-bit buckets */
    uint64      total_n;       /* total rows */
    uint64      off_bucket;    /* byte offset of bucket_offsets (nbuckets+1) */
    uint64      off_k1;        /* byte offset of k1 array (int64[total_n]) */
    uint64      off_k2;        /* byte offset of k2 array (int64[total_n]) */
} SmolParallelHdr;

typedef struct SmolWorkerExtra
{
    dsm_handle  handle;
    uint32      first_bucket;
    uint32      nbuckets;
    uint32      total_buckets;
} SmolWorkerExtra;
static void smol_radix_sort_idx_u64(const uint64 *key, uint32 *idx, uint32 *tmp, Size n);

/* Build callback context and helpers */
typedef struct BuildCtxI16 { int16 **pkeys; Size *pnalloc; Size *pnkeys; } BuildCtxI16;
typedef struct BuildCtxI32 { int32 **pkeys; Size *pnalloc; Size *pnkeys; } BuildCtxI32;
typedef struct BuildCtxI64 { int64 **pkeys; Size *pnalloc; Size *pnkeys; } BuildCtxI64;
static void smol_build_cb_i16(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void smol_build_cb_i32(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void smol_build_cb_i64(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static int cmp_int16(const void *a, const void *b);
static int cmp_int32(const void *a, const void *b);
static int cmp_int64(const void *a, const void *b);
/* tuplesort-based single-col contexts */
typedef struct TsBuildCtxI16 { Tuplesortstate *ts; Size *pnkeys; } TsBuildCtxI16;
typedef struct TsBuildCtxI32 { Tuplesortstate *ts; Size *pnkeys; } TsBuildCtxI32;
typedef struct TsBuildCtxI64 { Tuplesortstate *ts; Size *pnkeys; } TsBuildCtxI64;
static void ts_build_cb_i16(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void ts_build_cb_i32(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void ts_build_cb_i64(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
/* 2-col builders */
typedef struct { int64 **pk1; int64 **pk2; Size *pcap; Size *pcount; Oid t1; Oid t2; } PairArrCtx;
typedef struct { Tuplesortstate *ts; TupleDesc tdesc; Size *pcount; Oid t1; Oid t2; } TsPairCtx;
static void smol_build_cb_pair(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void ts_build_cb_pair(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);

/* --- Handler: wire a minimal IndexAmRoutine --- */
Datum
smol_handler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *am = makeNode(IndexAmRoutine);
    SMOL_LOG("enter smol_handler");

    am->amstrategies = 5;            /* <, <=, =, >=, > */
    am->amsupport = 1;               /* comparator proc 1 */
    am->amoptsprocnum = 0;

    am->amcanorder = true;
    am->amcanorderbyop = false;
    am->amcanhash = false;
    am->amconsistentequality = true;
    am->amconsistentordering = true;
    am->amcanbackward = true;
    am->amcanunique = false;
    am->amcanmulticol = true;
    am->amoptionalkey = true;
    am->amsearcharray = false;
    am->amsearchnulls = false;
    am->amstorage = false;
    am->amclusterable = false;
    am->ampredlocks = false;
    am->amcanparallel = true;
    am->amcanbuildparallel = false;
    am->amcaninclude = false;
    am->amusemaintenanceworkmem = false;
    am->amsummarizing = false;
    am->amparallelvacuumoptions = 0;
    am->amkeytype = InvalidOid;

    am->ambuild = smol_build;
    am->ambuildempty = smol_buildempty;
    am->aminsert = smol_insert;
    am->aminsertcleanup = NULL;
    am->ambulkdelete = NULL;
    am->amvacuumcleanup = smol_vacuumcleanup;
    am->amcanreturn = smol_canreturn;
    am->amcostestimate = smol_costestimate;
    am->amgettreeheight = NULL;
    am->amoptions = smol_options;
    am->amproperty = NULL;
    am->ambuildphasename = NULL;
    am->amvalidate = smol_validate;
    am->amadjustmembers = NULL;
    am->ambeginscan = smol_beginscan;
    am->amrescan = smol_rescan;
    am->amgettuple = smol_gettuple; /* prototype: unimplemented */
    am->amgetbitmap = NULL;         /* no bitmap scans */
    am->amendscan = smol_endscan;
    am->ammarkpos = NULL;
    am->amrestrpos = NULL;

    am->amestimateparallelscan = smol_estimateparallelscan;
    am->aminitparallelscan = smol_initparallelscan;
    am->amparallelrescan = smol_parallelrescan;

    am->amtranslatestrategy = NULL;
    am->amtranslatecmptype = NULL;

    PG_RETURN_POINTER(am);
}

/* --- Minimal implementations --- */
static IndexBuildResult *
smol_build(Relation heap, Relation index, struct IndexInfo *indexInfo)
{
    IndexBuildResult *res = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
    Size nalloc = 0, nkeys = 0;
    Oid atttypid;
    Oid atttypid2 = InvalidOid;
    uint16 key_len, key_len2 = 0;
    int nkeyatts = index->rd_index->indnkeyatts;
    SMOL_LOGF("build start rel=%u idx=%u", RelationGetRelid(heap), RelationGetRelid(index));
    /* Phase timers */
    instr_time t_start, t_collect_end, t_sort_end, t_write_end;
    INSTR_TIME_SET_CURRENT(t_start);
    INSTR_TIME_SET_CURRENT(t_collect_end);
    INSTR_TIME_SET_CURRENT(t_sort_end);
    INSTR_TIME_SET_CURRENT(t_write_end);

    /* Enforce 1 or 2 fixed-width integer keys for this prototype */
    if (nkeyatts != 1 && nkeyatts != 2)
        ereport(ERROR, (errmsg("smol prototype supports 1 or 2 key columns only")));
    atttypid = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
    if (atttypid == INT2OID) key_len = sizeof(int16);
    else if (atttypid == INT4OID) key_len = sizeof(int32);
    else if (atttypid == INT8OID) key_len = sizeof(int64);
    else ereport(ERROR, (errmsg("smol prototype supports int2/int4/int8 only")));
    if (nkeyatts == 2)
    {
        atttypid2 = TupleDescAttr(RelationGetDescr(index), 1)->atttypid;
        if (atttypid2 == INT2OID) key_len2 = sizeof(int16);
        else if (atttypid2 == INT4OID) key_len2 = sizeof(int32);
        else if (atttypid2 == INT8OID) key_len2 = sizeof(int64);
        else ereport(ERROR, (errmsg("smol prototype supports int2/int4/int8 only (second key)")));
    }

    if (nkeyatts == 1 && atttypid == INT2OID)
    {
        Oid ltOp;
        Oid coll = TupleDescAttr(RelationGetDescr(index), 0)->attcollation;
        TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_LT_OPR);
        Tuplesortstate *ts;
        TsBuildCtxI16 cb; bool isnull; Datum val; Size i = 0; int16 *out;
        if (!OidIsValid(tce->lt_opr)) ereport(ERROR, (errmsg("no < operator for type %u", atttypid)));
        ltOp = tce->lt_opr;
        ts = tuplesort_begin_datum(atttypid, ltOp, coll, false, maintenance_work_mem, NULL, false);
        cb.ts = ts; cb.pnkeys = &nkeys;
        SMOL_LOG("collect int2 -> tuplesort");
        table_index_build_scan(heap, index, indexInfo, true, true, ts_build_cb_i16, (void *) &cb, NULL);
        INSTR_TIME_SET_CURRENT(t_collect_end);
        SMOL_LOGF("build phase: collect done tuples=%zu", nkeys);
        SMOL_LOGF("build phase: sort start n=%zu", nkeys);
        tuplesort_performsort(ts);
        INSTR_TIME_SET_CURRENT(t_sort_end);
        if (smol_debug_log)
        {
            instr_time d; INSTR_TIME_SET_ZERO(d); INSTR_TIME_ACCUM_DIFF(d, t_sort_end, t_collect_end);
            SMOL_LOGF("build phase: sort done ~%.3f ms", (double) INSTR_TIME_GET_MILLISEC(d));
        }
        out = (int16 *) palloc(nkeys * sizeof(int16));
        while (tuplesort_getdatum(ts, true, false, &val, &isnull, NULL))
            out[i++] = DatumGetInt16(val);
        SMOL_LOGF("build phase: write start n=%zu", nkeys);
        smol_build_tree_from_sorted(index, (const void *) out, nkeys, key_len);
        INSTR_TIME_SET_CURRENT(t_write_end);
        if (smol_debug_log)
        {
            instr_time d; INSTR_TIME_SET_ZERO(d); INSTR_TIME_ACCUM_DIFF(d, t_write_end, t_sort_end);
            SMOL_LOGF("build phase: write done ~%.3f ms", (double) INSTR_TIME_GET_MILLISEC(d));
        }
        pfree(out);
        tuplesort_end(ts);
    }
    else if (nkeyatts == 1 && atttypid == INT4OID)
    {
        Oid ltOp; Oid coll = TupleDescAttr(RelationGetDescr(index), 0)->attcollation;
        TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_LT_OPR);
        Tuplesortstate *ts; TsBuildCtxI32 cb; bool isnull; Datum val; Size i = 0; int32 *out;
        if (!OidIsValid(tce->lt_opr)) ereport(ERROR, (errmsg("no < operator for type %u", atttypid)));
        ltOp = tce->lt_opr;
        ts = tuplesort_begin_datum(atttypid, ltOp, coll, false, maintenance_work_mem, NULL, false);
        cb.ts = ts; cb.pnkeys = &nkeys;
        SMOL_LOG("collect int4 -> tuplesort");
        table_index_build_scan(heap, index, indexInfo, true, true, ts_build_cb_i32, (void *) &cb, NULL);
        INSTR_TIME_SET_CURRENT(t_collect_end);
        SMOL_LOGF("build phase: collect done tuples=%zu", nkeys);
        SMOL_LOGF("build phase: sort start n=%zu", nkeys);
        tuplesort_performsort(ts);
        INSTR_TIME_SET_CURRENT(t_sort_end);
        if (smol_debug_log)
        {
            instr_time d; INSTR_TIME_SET_ZERO(d); INSTR_TIME_ACCUM_DIFF(d, t_sort_end, t_collect_end);
            SMOL_LOGF("build phase: sort done ~%.3f ms", (double) INSTR_TIME_GET_MILLISEC(d));
        }
        out = (int32 *) palloc(nkeys * sizeof(int32));
        while (tuplesort_getdatum(ts, true, false, &val, &isnull, NULL))
            out[i++] = DatumGetInt32(val);
        SMOL_LOGF("build phase: write start n=%zu", nkeys);
        smol_build_tree_from_sorted(index, (const void *) out, nkeys, key_len);
        INSTR_TIME_SET_CURRENT(t_write_end);
        if (smol_debug_log)
        {
            instr_time d; INSTR_TIME_SET_ZERO(d); INSTR_TIME_ACCUM_DIFF(d, t_write_end, t_sort_end);
            SMOL_LOGF("build phase: write done ~%.3f ms", (double) INSTR_TIME_GET_MILLISEC(d));
        }
        pfree(out);
        tuplesort_end(ts);
    }
    else if (nkeyatts == 1) /* INT8OID */
    {
        Oid ltOp; Oid coll = TupleDescAttr(RelationGetDescr(index), 0)->attcollation;
        TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_LT_OPR);
        Tuplesortstate *ts; TsBuildCtxI64 cb; bool isnull; Datum val; Size i = 0; int64 *out;
        if (!OidIsValid(tce->lt_opr)) ereport(ERROR, (errmsg("no < operator for type %u", atttypid)));
        ltOp = tce->lt_opr;
        ts = tuplesort_begin_datum(atttypid, ltOp, coll, false, maintenance_work_mem, NULL, false);
        cb.ts = ts; cb.pnkeys = &nkeys;
        SMOL_LOG("collect int8 -> tuplesort");
        table_index_build_scan(heap, index, indexInfo, true, true, ts_build_cb_i64, (void *) &cb, NULL);
        INSTR_TIME_SET_CURRENT(t_collect_end);
        SMOL_LOGF("build phase: collect done tuples=%zu", nkeys);
        SMOL_LOGF("build phase: sort start n=%zu", nkeys);
        tuplesort_performsort(ts);
        INSTR_TIME_SET_CURRENT(t_sort_end);
        if (smol_debug_log)
        {
            instr_time d; INSTR_TIME_SET_ZERO(d); INSTR_TIME_ACCUM_DIFF(d, t_sort_end, t_collect_end);
            SMOL_LOGF("build phase: sort done ~%.3f ms", (double) INSTR_TIME_GET_MILLISEC(d));
        }
        out = (int64 *) palloc(nkeys * sizeof(int64));
        while (tuplesort_getdatum(ts, true, false, &val, &isnull, NULL))
            out[i++] = DatumGetInt64(val);
        SMOL_LOGF("build phase: write start n=%zu", nkeys);
        smol_build_tree_from_sorted(index, (const void *) out, nkeys, key_len);
        INSTR_TIME_SET_CURRENT(t_write_end);
        if (smol_debug_log)
        {
            instr_time d; INSTR_TIME_SET_ZERO(d); INSTR_TIME_ACCUM_DIFF(d, t_write_end, t_sort_end);
            SMOL_LOGF("build phase: write done ~%.3f ms", (double) INSTR_TIME_GET_MILLISEC(d));
        }
        pfree(out);
        tuplesort_end(ts);
    }
    else /* 2-column: collect pairs as int64 (parallel rows-LSD) */
    {
        /* 1) Leader collects raw arrays into DSM, partitioned by k1 high bits */
        const int BITS = 8; /* 256 buckets: reduces per-bucket overhead */
        const uint32 NB = (1u << BITS);
        Size cap = 0, n = 0;
        int64 *k1arr = NULL, *k2arr = NULL;
        PairArrCtx cctx = { &k1arr, &k2arr, &cap, &n, atttypid, atttypid2 };
        table_index_build_scan(heap, index, indexInfo, true, true, smol_build_cb_pair, (void *) &cctx, NULL);
        INSTR_TIME_SET_CURRENT(t_collect_end);
        if (n == 0)
        {
            INSTR_TIME_SET_CURRENT(t_sort_end);
            smol_build_tree2_from_sorted(index, k1arr, k2arr, n, key_len, key_len2);
            INSTR_TIME_SET_CURRENT(t_write_end);
            if (k1arr) pfree(k1arr); if (k2arr) pfree(k2arr);
        }
        else
        {
            /* Try DSM + workers; on failure, fall back to single-process sort */
            bool used_parallel = false;
            uint64 *count = (uint64 *) palloc0(NB * sizeof(uint64));
            uint64 *off = (uint64 *) palloc0((NB + 1) * sizeof(uint64));
            for (Size i = 0; i < n; i++)
            {
                uint64 u = (uint64) k1arr[i] ^ UINT64_C(0x8000000000000000);
                uint32 b = (uint32) (u >> (64 - BITS));
                count[b]++;
            }
            for (uint32 b = 0; b < NB; b++) off[b + 1] = off[b] + count[b];
            PG_TRY();
            {
                Size hdr_bytes = sizeof(SmolParallelHdr) + (NB + 1) * sizeof(uint64);
                Size bytes_k1 = n * sizeof(int64), bytes_k2 = n * sizeof(int64);
                Size total = hdr_bytes + bytes_k1 + bytes_k2;
                dsm_segment *seg = dsm_create(total, 0);
                char *base = (char *) dsm_segment_address(seg);
                SmolParallelHdr *hdr = (SmolParallelHdr *) base;
                hdr->magic = SMOL_META_MAGIC; hdr->nbuckets = NB; hdr->total_n = n;
                hdr->off_bucket = sizeof(SmolParallelHdr);
                hdr->off_k1 = hdr->off_bucket + (NB + 1) * sizeof(uint64);
                hdr->off_k2 = hdr->off_k1 + bytes_k1;
                uint64 *d_off = (uint64 *) (base + hdr->off_bucket);
                memcpy(d_off, off, (NB + 1) * sizeof(uint64));
                int64 *dk1 = (int64 *) (base + hdr->off_k1);
                int64 *dk2 = (int64 *) (base + hdr->off_k2);
                /* Place rows into DSM by bucket */
                memcpy(count, off, NB * sizeof(uint64));
                for (Size i = 0; i < n; i++)
                {
                    uint64 u = (uint64) k1arr[i] ^ UINT64_C(0x8000000000000000);
                    uint32 b = (uint32) (u >> (64 - BITS));
                    uint64 pos = count[b]++;
                    dk1[pos] = k1arr[i];
                    dk2[pos] = k2arr[i];
                }
                /* Launch workers to sort each bucket in place */
                int nworkers = 4; /* simple cap */
                BackgroundWorkerHandle *handles[16];
                if (nworkers > 16) nworkers = 16;
                uint32 buckets_per = (NB + nworkers - 1) / nworkers;
                int launched = 0;
                for (int w = 0; w < nworkers; w++)
                {
                    uint32 first = (uint32) (w * buckets_per);
                    if (first >= NB) break;
                    uint32 nb = (uint32) ((first + buckets_per <= NB) ? buckets_per : (NB - first));
                    BackgroundWorker bw;
                    memset(&bw, 0, sizeof(bw));
                    bw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
                    bw.bgw_start_time = BgWorkerStart_ConsistentState;
                    bw.bgw_restart_time = BGW_NEVER_RESTART;
                    snprintf(bw.bgw_name, BGW_MAXLEN, "smol parallel sort %d", w);
                    snprintf(bw.bgw_library_name, BGW_MAXLEN, "smol");
                    snprintf(bw.bgw_function_name, BGW_MAXLEN, "smol_parallel_sort_worker");
                    SmolWorkerExtra extra;
                    extra.handle = dsm_segment_handle(seg);
                    extra.first_bucket = first;
                    extra.nbuckets = nb;
                    extra.total_buckets = NB;
                    memcpy(bw.bgw_extra, &extra, sizeof(extra));
                    bw.bgw_main_arg = (Datum) 0;
                    bw.bgw_notify_pid = MyProcPid;
                    if (RegisterDynamicBackgroundWorker(&bw, &handles[launched]))
                        launched++;
                }
                SMOL_LOGF("launched bgworkers=%d (requested=%d) buckets=%u per_worker=%u", launched, nworkers, NB, buckets_per);
                for (int ihandle = 0; ihandle < launched; ihandle++)
                {
                    BgwHandleStatus st;
                    pid_t pid = 0;
                    /* Wait for startup */
                    for (;;)
                    {
                        st = GetBackgroundWorkerPid(handles[ihandle], &pid);
                        if (st == BGWH_STARTED || st == BGWH_STOPPED || st == BGWH_POSTMASTER_DIED)
                            break;
                        pg_usleep(10000L);
                    }
                    SMOL_LOGF("worker %d startup poll status=%d pid=%d", ihandle, (int) st, (int) pid);
                    if (st != BGWH_STARTED)
                        continue;
                    /* Poll for shutdown with bounded waits and periodic logging */
                    {
                        instr_time t0, t1;
                        int warned = 0;
                        INSTR_TIME_SET_CURRENT(t0);
                        for (;;)
                        {
                            st = GetBackgroundWorkerPid(handles[ihandle], &pid);
                            if (st == BGWH_STOPPED || st == BGWH_POSTMASTER_DIED)
                                break;
                            if (st != BGWH_STARTED)
                                break;
                            pg_usleep(10000L);
                            if (smol_debug_log && smol_wait_log_ms > 0)
                            {
                                INSTR_TIME_SET_CURRENT(t1);
                                double ms = (INSTR_TIME_GET_DOUBLE(t1) - INSTR_TIME_GET_DOUBLE(t0)) * 1000.0;
                                if (!warned && ms > smol_wait_log_ms)
                                {
                                    SMOL_LOGF("waiting for worker %d shutdown ~%.1f ms", ihandle, ms);
                                    warned = 1;
                                }
                            }
                        }
                        SMOL_LOGF("worker %d shutdown complete (status=%d)", ihandle, (int) st);
                    }
                }
                INSTR_TIME_SET_CURRENT(t_sort_end);
                smol_build_tree2_from_sorted(index, dk1, dk2, n, key_len, key_len2);
                INSTR_TIME_SET_CURRENT(t_write_end);
                dsm_detach(seg);
                used_parallel = true;
            }
            PG_CATCH();
            {
                /* Fallback: sort in-process without DSM */
                FlushErrorState();
                SMOL_LOG("parallel build unavailable; falling back to single-process sort");
                SMOL_LOGF("build phase: sort(start) pairs n=%zu", n);
                smol_sort_pairs_rows64(k1arr, k2arr, n);
                INSTR_TIME_SET_CURRENT(t_sort_end);
                if (smol_debug_log)
                {
                    instr_time d; INSTR_TIME_SET_ZERO(d); INSTR_TIME_ACCUM_DIFF(d, t_sort_end, t_collect_end);
                    SMOL_LOGF("build phase: sort(done) ~%.3f ms", (double) INSTR_TIME_GET_MILLISEC(d));
                }
                SMOL_LOGF("build phase: write start n=%zu", n);
                smol_build_tree2_from_sorted(index, k1arr, k2arr, n, key_len, key_len2);
                INSTR_TIME_SET_CURRENT(t_write_end);
                if (smol_debug_log)
                {
                    instr_time d; INSTR_TIME_SET_ZERO(d); INSTR_TIME_ACCUM_DIFF(d, t_write_end, t_sort_end);
                    SMOL_LOGF("build phase: write done ~%.3f ms", (double) INSTR_TIME_GET_MILLISEC(d));
                }
            }
            PG_END_TRY();
            if (k1arr) pfree(k1arr);
            if (k2arr) pfree(k2arr);
            pfree(count);
            pfree(off);
        }
    }

    /* Mark heap block 0 all-visible so synthetic TID (0,1) will be IOS */
    smol_mark_heap0_allvisible(heap);

    res->heap_tuples = (double) nkeys;
    res->index_tuples = (double) nkeys;
    /* Log a simple build profile when debug logging is enabled */
    {
        instr_time d_collect, d_sort, d_write, d_total;
        double ms_collect, ms_sort, ms_write, ms_total;
        INSTR_TIME_SET_ZERO(d_collect);
        INSTR_TIME_SET_ZERO(d_sort);
        INSTR_TIME_SET_ZERO(d_write);
        INSTR_TIME_SET_ZERO(d_total);
        INSTR_TIME_ACCUM_DIFF(d_collect, t_collect_end, t_start);
        INSTR_TIME_ACCUM_DIFF(d_sort, t_sort_end, t_collect_end);
        INSTR_TIME_ACCUM_DIFF(d_write, t_write_end, t_sort_end);
        INSTR_TIME_ACCUM_DIFF(d_total, t_write_end, t_start);
        ms_collect = (double) INSTR_TIME_GET_MILLISEC(d_collect);
        ms_sort = (double) INSTR_TIME_GET_MILLISEC(d_sort);
        ms_write = (double) INSTR_TIME_GET_MILLISEC(d_write);
        ms_total = (double) INSTR_TIME_GET_MILLISEC(d_total);
        SMOL_LOGF("build finish tuples=%zu profile: collect=%.3f ms sort=%.3f ms write=%.3f ms total~%.3f ms",
                  nkeys, ms_collect, ms_sort, ms_write, ms_total);
    }
    return res;
}

static void
smol_buildempty(Relation index)
{
    Buffer buf;
    Page page;
    SmolMeta *meta;
    SMOL_LOG("enter smol_buildempty");
    buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);
    PageInit(page, BLCKSZ, 0);
    meta = smol_meta_ptr(page);
    meta->magic = SMOL_META_MAGIC;
    meta->version = SMOL_META_VERSION;
    meta->nkeyatts = 1;
    meta->key_len1 = sizeof(int32);
    meta->key_len2 = 0;
    meta->root_blkno = InvalidBlockNumber;
    meta->height = 0;
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

static bool
smol_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
            Relation heapRel, IndexUniqueCheck checkUnique, bool indexUnchanged,
            struct IndexInfo *indexInfo)
{
    SMOL_LOG("aminsert called (read-only error)");
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("smol is read-only: aminsert is not supported")));
    return false;
}

static IndexScanDesc
smol_beginscan(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    SmolScanOpaque so;
    SmolMeta meta;
    scan = RelationGetIndexScan(index, nkeys, norderbys);
    /* executor expects a tuple desc */
    scan->xs_itupdesc = RelationGetDescr(index);
    so = (SmolScanOpaque) palloc0(sizeof(SmolScanOpaqueData));
    so->initialized = false;
    so->cur_blk = InvalidBlockNumber;
    so->cur_off = InvalidOffsetNumber;
    so->cur_buf = InvalidBuffer;
    so->have_pin = false;
    so->have_bound = false;
    so->bound_strict = false;
    so->atttypid = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
    so->atttypid2 = (RelationGetDescr(index)->natts >= 2) ? TupleDescAttr(RelationGetDescr(index), 1)->atttypid : InvalidOid;
    /* read meta */
    smol_meta_read(index, &meta);
    so->two_col = (meta.nkeyatts == 2);
    so->key_len = meta.key_len1;
    so->key_len2 = meta.key_len2;
    so->cur_group = 0;
    so->pos_in_group = 0;
    SMOL_LOGF("beginscan nkeys=%d key_len=%u", nkeys, so->key_len);

    /*
     * Prebuild a minimal index tuple with no nulls/varwidth.
     * We reuse this across all returned rows by memcpy-ing new key bytes.
     */
    {
        Size data_off = MAXALIGN(sizeof(IndexTupleData));
        Size off1 = data_off;
        Size off2 = 0;
        Size sz;
        int16 typlen1; bool byval1; char align1;
        int16 typlen2 = 0; bool byval2 = true; char align2 = TYPALIGN_INT;
        get_typlenbyvalalign(so->atttypid, &typlen1, &byval1, &align1);
        if (so->two_col)
            get_typlenbyvalalign(so->atttypid2, &typlen2, &byval2, &align2);
        so->align1 = align1;
        so->align2 = align2;
        if (!so->two_col)
        {
            sz = off1 + so->key_len;
        }
        else
        {
            off2 = att_align_nominal(off1 + so->key_len, align2);
            sz = off2 + so->key_len2;
        }
        so->itup = (IndexTuple) palloc0(sz);
        so->itup->t_info = (unsigned short) sz; /* no NULLs, no VARWIDTH */
        so->itup_data = (char *) so->itup + data_off;
        so->itup_off2 = so->two_col ? (uint16) (off2 - data_off) : 0;
    }
    so->prof_enabled = smol_profile_log;
    so->prof_calls = 0;
    so->prof_rows = 0;
    so->prof_pages = 0;
    so->prof_bytes = 0;
    so->prof_bsteps = 0;
    scan->opaque = so;
    return scan;
}

static void
smol_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    so->initialized = false;
    so->cur_blk = InvalidBlockNumber;
    so->cur_off = InvalidOffsetNumber;
    if (so->have_pin && BufferIsValid(so->cur_buf))
    {
        ReleaseBuffer(so->cur_buf);
        so->cur_buf = InvalidBuffer;
        so->have_pin = false;
    }
    so->have_bound = false;
    so->have_k2_eq = false;
    if (keys && nkeys > 0)
    {
        for (int i = 0; i < nkeys; i++)
        {
            ScanKey sk = &keys[i];
            if (sk->sk_attno == 1)
            {
                if (sk->sk_strategy == BTGreaterEqualStrategyNumber ||
                    sk->sk_strategy == BTGreaterStrategyNumber ||
                    sk->sk_strategy == BTEqualStrategyNumber)
                {
                    so->have_bound = true;
                    so->bound_strict = (sk->sk_strategy == BTGreaterStrategyNumber);
                    if (so->atttypid == INT2OID)
                        so->bound = (int64) DatumGetInt16(sk->sk_argument);
                    else if (so->atttypid == INT4OID)
                        so->bound = (int64) DatumGetInt32(sk->sk_argument);
                    else if (so->atttypid == INT8OID)
                        so->bound = DatumGetInt64(sk->sk_argument);
                    else
                        so->bound = DatumGetInt64(sk->sk_argument);
                }
            }
            else if (sk->sk_attno == 2 && sk->sk_strategy == BTEqualStrategyNumber)
            {
                so->have_k2_eq = true;
                Oid t2 = so->atttypid2;
                if (t2 == INT2OID) so->k2_eq = (int64) DatumGetInt16(sk->sk_argument);
                else if (t2 == INT4OID) so->k2_eq = (int64) DatumGetInt32(sk->sk_argument);
                else so->k2_eq = DatumGetInt64(sk->sk_argument);
            }
        }
    }
}

static bool
smol_gettuple(IndexScanDesc scan, ScanDirection dir)
{
    Relation idx = scan->indexRelation;
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    Page page;
    Buffer buf;
    if (so->prof_enabled)
        so->prof_calls++;

    if (!scan->xs_want_itup)
        ereport(ERROR, (errmsg("smol supports index-only scans only")));
    if (dir == NoMovementScanDirection)
        return false;

    /*
     * First-time init: position to first tuple/group >= bound.
     * We pin (but do not lock) the chosen leaf page and keep it pinned across
     * calls until exhausted, to minimize buffer manager overhead.
     */
    if (!so->initialized)
    {
        int64 lb;
        OffsetNumber maxoff;
        if (dir == BackwardScanDirection)
        {
            if (so->have_bound)
            {
                lb = so->bound;
                so->cur_blk = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
            }
            else
            {
                so->cur_blk = smol_rightmost_leaf(idx);
            }
            /* position within leaf: set to end; we'll walk backward */
            buf = ReadBuffer(idx, so->cur_blk);
            page = BufferGetPage(buf);
            {
            uint16 n = smol_leaf_nitems(page);
            if (so->have_bound)
            {
                so->cur_off = n;
                while (so->cur_off >= FirstOffsetNumber)
                {
                    char *keyp = smol_leaf_keyptr(page, so->cur_off, so->key_len);
                    if (smol_cmp_keyptr_bound(keyp, so->key_len, so->atttypid, so->bound) <= 0)
                        break;
                    so->cur_off--;
                }
            }
            else
            {
                so->cur_off = n;
            }
            so->cur_buf = buf; so->have_pin = true;
            so->initialized = true;
            SMOL_LOGF("init backward cur_blk=%u off=%u", so->cur_blk, so->cur_off);
            }
        }
        else
        {
            if (!so->two_col)
            {
                if (scan->parallel_scan)
                {
                    SmolParallelScan *ps = (SmolParallelScan *) ((char *) scan->parallel_scan + scan->parallel_scan->ps_offset_am);
                    /* claim first leaf from shared state */
                    for (;;)
                    {
                        uint32 curv = pg_atomic_read_u32(&ps->curr);
                        if (curv == 0u)
                        {
                            int64 lb = so->have_bound ? so->bound : PG_INT64_MIN;
                            BlockNumber left = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                            /* publish rightlink(left) so others don't also take left */
                            Buffer lbuf = ReadBuffer(idx, left);
                            Page lpg = BufferGetPage(lbuf);
                            BlockNumber rl = smol_page_opaque(lpg)->rightlink;
                            ReleaseBuffer(lbuf);
                            uint32 expect = 0u;
                            uint32 newv = (uint32) (BlockNumberIsValid(rl) ? rl : InvalidBlockNumber);
                            if (pg_atomic_compare_exchange_u32(&ps->curr, &expect, newv))
                            { so->cur_blk = left; break; }
                            continue;
                        }
                        if (curv == (uint32) InvalidBlockNumber)
                        { so->cur_blk = InvalidBlockNumber; break; }
                        /* claim current */
                        Buffer tbuf = ReadBuffer(idx, (BlockNumber) curv);
                        Page tpg = BufferGetPage(tbuf);
                        BlockNumber rl = smol_page_opaque(tpg)->rightlink;
                        ReleaseBuffer(tbuf);
                        uint32 expected = curv;
                        uint32 newv = (uint32) (BlockNumberIsValid(rl) ? rl : InvalidBlockNumber);
                        if (pg_atomic_compare_exchange_u32(&ps->curr, &expected, newv))
                        { so->cur_blk = (BlockNumber) curv; break; }
                    }
                    so->cur_off = FirstOffsetNumber;
                    so->initialized = true;
                    if (BlockNumberIsValid(so->cur_blk))
                    {
                        buf = ReadBuffer(idx, so->cur_blk);
                        page = BufferGetPage(buf);
                        so->cur_buf = buf; so->have_pin = true;
                    }
                }
                else
                {
                    lb = so->have_bound ? so->bound : PG_INT64_MIN;
                    so->cur_blk = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                    so->cur_off = FirstOffsetNumber;
                    so->initialized = true;
                    SMOL_LOGF("gettuple init cur_blk=%u bound=%ld", so->cur_blk, (long) lb);

                    /* seek within leaf to >= bound */
                    if (so->have_bound)
                    {
                        /* Pin leaf and binary-search to first >= or > bound */
                        uint16 n2, lo = FirstOffsetNumber, hi, ans = InvalidOffsetNumber;
                        buf = ReadBuffer(idx, so->cur_blk);
                        page = BufferGetPage(buf);
                        n2 = smol_leaf_nitems(page);
                        hi = n2;
                        while (lo <= hi)
                        {
                            uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                            char *keyp = smol_leaf_keyptr(page, mid, so->key_len);
                            int c = smol_cmp_keyptr_bound(keyp, so->key_len, so->atttypid, so->bound);
                            if (so->prof_enabled) so->prof_bsteps++;
                            if ((so->bound_strict ? (c > 0) : (c >= 0))) { ans = mid; if (mid == 0) break; hi = (uint16) (mid - 1); }
                            else lo = (uint16) (mid + 1);
                        }
                        so->cur_off = (ans != InvalidOffsetNumber) ? ans : (uint16) (n2 + 1);
                        so->cur_buf = buf; so->have_pin = true;
                        SMOL_LOGF("seeked (binsearch) within leaf off=%u", so->cur_off);
                    }
                    else
                    {
                        /* No bound: just pin the first leaf */
                        buf = ReadBuffer(idx, so->cur_blk);
                        page = BufferGetPage(buf);
                        so->cur_buf = buf; so->have_pin = true;
                    }
                }
            }
            else
            {
                if (scan->parallel_scan)
                {
                    SmolParallelScan *ps = (SmolParallelScan *) ((char *) scan->parallel_scan + scan->parallel_scan->ps_offset_am);
                    for (;;)
                    {
                        uint32 curv = pg_atomic_read_u32(&ps->curr);
                        if (curv == 0u)
                        {
                            int64 lb = so->have_bound ? so->bound : PG_INT64_MIN;
                            BlockNumber left = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                            Buffer lbuf = ReadBuffer(idx, left);
                            Page lpg = BufferGetPage(lbuf);
                            BlockNumber rl = smol_page_opaque(lpg)->rightlink;
                            ReleaseBuffer(lbuf);
                            uint32 expect = 0u;
                            uint32 newv = (uint32) (BlockNumberIsValid(rl) ? rl : InvalidBlockNumber);
                            if (pg_atomic_compare_exchange_u32(&ps->curr, &expect, newv))
                            { so->cur_blk = left; break; }
                            continue;
                        }
                        if (curv == (uint32) InvalidBlockNumber)
                        { so->cur_blk = InvalidBlockNumber; break; }
                        Buffer tbuf = ReadBuffer(idx, (BlockNumber) curv);
                        Page tpg = BufferGetPage(tbuf);
                        BlockNumber rl = smol_page_opaque(tpg)->rightlink;
                        ReleaseBuffer(tbuf);
                        uint32 expected = curv;
                        uint32 newv = (uint32) (BlockNumberIsValid(rl) ? rl : InvalidBlockNumber);
                        if (pg_atomic_compare_exchange_u32(&ps->curr, &expected, newv))
                        { so->cur_blk = (BlockNumber) curv; break; }
                    }
                    so->cur_group = 0;
                    so->pos_in_group = 0;
                    so->initialized = true;
                    if (BlockNumberIsValid(so->cur_blk))
                    {
                        buf = ReadBuffer(idx, so->cur_blk);
                        page = BufferGetPage(buf);
                        so->cur_buf = buf; so->have_pin = true;
                        so->leaf_n = 0; so->leaf_i = 0;
                        if (so->have_bound)
                        {
                            uint16 ng = smol2_leaf_ngroups(page);
                            uint16 lo = 0, hi = (ng > 0) ? (uint16) (ng - 1) : 0, ans = ng;
                            while (lo <= hi)
                            {
                                uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                                int c = smol_cmp_k1_group(page, mid, so->key_len, so->atttypid, so->bound);
                                if (so->prof_enabled) so->prof_bsteps++;
                                if ((so->bound_strict ? (c > 0) : (c >= 0))) { ans = mid; if (mid == 0) break; hi = (uint16) (mid - 1); }
                                else lo = (uint16) (mid + 1);
                            }
                            so->cur_group = ans;
                        }
                        else
                        {
                            so->cur_group = 0;
                        }
                        smol2_build_leaf_cache(so, page);
                    }
                }
                else
                {
                    lb = so->have_bound ? so->bound : PG_INT64_MIN;
                    so->cur_blk = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                    so->cur_group = 0;
                    so->pos_in_group = 0;
                    so->initialized = true;
                    if (so->have_bound)
                    {
                        /* Pin leaf and binary-search group directory on k1 (>= or > bound) */
                        uint16 ng, lo = 0, hi, ans = 0;
                        buf = ReadBuffer(idx, so->cur_blk);
                        page = BufferGetPage(buf);
                        ng = smol2_leaf_ngroups(page);
                        if (ng > 0)
                        {
                            hi = (uint16) (ng - 1);
                            ans = ng;
                            while (lo <= hi)
                            {
                                uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                                int c = smol_cmp_k1_group(page, mid, so->key_len, so->atttypid, so->bound);
                                if (so->prof_enabled) so->prof_bsteps++;
                                if ((so->bound_strict ? (c > 0) : (c >= 0))) { ans = mid; if (mid == 0) break; hi = (uint16) (mid - 1); }
                                else lo = (uint16) (mid + 1);
                            }
                            so->cur_group = ans;
                        }
                        so->cur_buf = buf; so->have_pin = true;
                        so->leaf_n = 0; so->leaf_i = 0;
                        smol2_build_leaf_cache(so, page);
                    }
                    else
                    {
                        buf = ReadBuffer(idx, so->cur_blk);
                        page = BufferGetPage(buf);
                        so->cur_buf = buf; so->have_pin = true;
                        so->leaf_n = 0; so->leaf_i = 0;
                        smol2_build_leaf_cache(so, page);
                    }
                }
            }
        }
    }

    while (BlockNumberIsValid(so->cur_blk))
    {
        SmolPageOpaqueData *op;
        BlockNumber next;
        /* Ensure current leaf is pinned; page pointer valid */
        if (!so->have_pin || !BufferIsValid(so->cur_buf))
        {
            so->cur_buf = ReadBuffer(idx, so->cur_blk);
            so->have_pin = true;
        }
        buf = so->cur_buf;
        page = BufferGetPage(buf);
        if (so->two_col)
        {
            if (so->leaf_i < so->leaf_n)
            {
                int64 v1 = so->leaf_k1[so->leaf_i];
                int64 v2 = so->leaf_k2[so->leaf_i];
                if (so->have_k2_eq && v2 != so->k2_eq)
                { so->leaf_i++; continue; }
                if (so->key_len == 2) { int16 t=(int16)v1; memcpy(so->itup_data,&t,2);} else if (so->key_len==4){int32 t=(int32)v1; memcpy(so->itup_data,&t,4);} else { memcpy(so->itup_data,&v1,8);} 
                if (so->key_len2 == 2) { int16 t=(int16)v2; memcpy(so->itup_data+so->itup_off2,&t,2);} else if (so->key_len2==4){int32 t=(int32)v2; memcpy(so->itup_data+so->itup_off2,&t,4);} else { memcpy(so->itup_data+so->itup_off2,&v2,8);} 
                scan->xs_itup = so->itup;
                ItemPointerSet(&(scan->xs_heaptid), 0, 1);
                so->leaf_i++;
                if (so->prof_enabled) { so->prof_rows++; so->prof_bytes += (uint64)(so->key_len + so->key_len2); }
                return true;
            }
        }
        else
        {
            uint16 n = smol_leaf_nitems(page);
            if (dir == BackwardScanDirection)
            {
                while (so->cur_off >= FirstOffsetNumber)
                {
                    /* Copy fixed-width value directly into prebuilt tuple (no Datum calls) */
                    char *keyp = smol_leaf_keyptr(page, so->cur_off, so->key_len);
                    if (so->key_len == 2) memcpy(so->itup_data, keyp, 2);
                    else if (so->key_len == 4) memcpy(so->itup_data, keyp, 4);
                    else memcpy(so->itup_data, keyp, 8);
                    if (so->prof_enabled)
                        so->prof_bytes += (uint64) so->key_len;
                    scan->xs_itup = so->itup;
                    ItemPointerSet(&(scan->xs_heaptid), 0, 1);
                    so->cur_off--;
                    if (so->prof_enabled) so->prof_rows++;
                    return true;
                }
            }
            else
            {
                while (so->cur_off <= n)
                {
                    char *keyp = smol_leaf_keyptr(page, so->cur_off, so->key_len);
                    if (so->key_len == 2) memcpy(so->itup_data, keyp, 2);
                    else if (so->key_len == 4) memcpy(so->itup_data, keyp, 4);
                    else memcpy(so->itup_data, keyp, 8);
                    if (so->prof_enabled)
                        so->prof_bytes += (uint64) so->key_len;
                    scan->xs_itup = so->itup;
                    ItemPointerSet(&(scan->xs_heaptid), 0, 1);
                    so->cur_off++;
                    if (so->prof_enabled) so->prof_rows++;
                    return true;
                }
            }
        }

        /* advance to next leaf */
        if (scan->parallel_scan && dir != BackwardScanDirection)
            {
            SmolParallelScan *ps = (SmolParallelScan *) ((char *) scan->parallel_scan + scan->parallel_scan->ps_offset_am);
            for (;;)
            {
                uint32 curv = pg_atomic_read_u32(&ps->curr);
                if (curv == 0u)
                {
                    int64 lb = so->have_bound ? so->bound : PG_INT64_MIN;
                    BlockNumber left = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                    Buffer lbuf = ReadBuffer(idx, left);
                    Page lpg = BufferGetPage(lbuf);
                    BlockNumber rl = smol_page_opaque(lpg)->rightlink;
                    ReleaseBuffer(lbuf);
                    uint32 expect = 0u;
                    uint32 newv = (uint32) (BlockNumberIsValid(rl) ? rl : InvalidBlockNumber);
                    if (pg_atomic_compare_exchange_u32(&ps->curr, &expect, newv))
                    {
                        next = left;
                        break;
                    }
                    continue;
                }
                if (curv == (uint32) InvalidBlockNumber)
                { next = InvalidBlockNumber; break; }
                /* Read rightlink to publish next */
                Buffer tbuf = ReadBuffer(idx, (BlockNumber) curv);
                Page tpg = BufferGetPage(tbuf);
                BlockNumber rl = smol_page_opaque(tpg)->rightlink;
                ReleaseBuffer(tbuf);
                uint32 expected = curv;
                uint32 newv = (uint32) (BlockNumberIsValid(rl) ? rl : InvalidBlockNumber);
                if (pg_atomic_compare_exchange_u32(&ps->curr, &expected, newv))
                { next = (BlockNumber) curv; break; }
            }
        }
        else
        {
            op = smol_page_opaque(page);
            next = (dir == BackwardScanDirection) ? smol_prev_leaf(idx, so->cur_blk) : op->rightlink;
        }
        if (so->have_pin && BufferIsValid(buf))
        {
            ReleaseBuffer(buf);
            so->have_pin = false;
            so->cur_buf = InvalidBuffer;
        }
        if (so->prof_enabled)
            so->prof_pages++;
        so->cur_blk = next;
        so->cur_off = (dir == BackwardScanDirection) ? InvalidOffsetNumber : FirstOffsetNumber;
        so->cur_group = 0;
        so->pos_in_group = 0;
        so->leaf_n = 0; so->leaf_i = 0;
        if (BlockNumberIsValid(so->cur_blk))
        {
            if (scan->parallel_scan && dir != BackwardScanDirection)
                so->cur_blk = next;
            /* Pre-pin next leaf and rebuild cache for two-col */
            Buffer nbuf = ReadBuffer(idx, so->cur_blk);
            Page np = BufferGetPage(nbuf);
            if (so->two_col)
            {
                if (so->have_bound)
                {
                    uint16 ng = smol2_leaf_ngroups(np);
                    uint16 lo = 0, hi = (ng > 0) ? (uint16) (ng - 1) : 0, ans = ng;
                    while (lo <= hi)
                    {
                        uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                        int c = smol_cmp_k1_group(np, mid, so->key_len, so->atttypid, so->bound);
                        if (so->prof_enabled) so->prof_bsteps++;
                        if (c >= 0) { ans = mid; if (mid == 0) break; hi = (uint16) (mid - 1); }
                        else lo = (uint16) (mid + 1);
                    }
                    so->cur_group = ans;
                }
                else
                {
                    so->cur_group = 0;
                }
                smol2_build_leaf_cache(so, np);
            }
            so->cur_buf = nbuf; so->have_pin = true;
            page = np;
            continue;
        }
        SMOL_LOGF("advance to %s leaf blk=%u", (dir == BackwardScanDirection ? "left" : "right"), next);
    }

    return false;
}

static void
smol_endscan(IndexScanDesc scan)
{
    SMOL_LOG("end scan");
    if (scan->opaque)
    {
        SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
        if (so->have_pin && BufferIsValid(so->cur_buf))
            ReleaseBuffer(so->cur_buf);
        if (so->leaf_k1) pfree(so->leaf_k1);
        if (so->leaf_k2) pfree(so->leaf_k2);
        if (so->itup)
            pfree(so->itup);
        if (so->prof_enabled)
            elog(LOG, "[smol] scan profile: calls=%lu rows=%lu leaf_pages=%lu bytes_copied=%lu binsearch_steps=%lu",
                 (unsigned long) so->prof_calls,
                 (unsigned long) so->prof_rows,
                 (unsigned long) so->prof_pages,
                 (unsigned long) so->prof_bytes,
                 (unsigned long) so->prof_bsteps);
        pfree(so);
    }
}

static bool
smol_canreturn(Relation index, int attno)
{
    /* Can return leading key columns (all key columns) */
    return attno >= 1 && attno <= RelationGetDescr(index)->natts;
}

static Size
smol_estimateparallelscan(Relation index, int nkeys, int norderbys)
{
    return (Size) sizeof(SmolParallelScan);
}

static void
smol_initparallelscan(void *target)
{
    SmolParallelScan *ps = (SmolParallelScan *) target;
    pg_atomic_init_u32(&ps->curr, 0u);
}

static void
smol_parallelrescan(IndexScanDesc scan)
{
    if (scan->parallel_scan)
    {
        SmolParallelScan *ps = (SmolParallelScan *) ((char *) scan->parallel_scan + scan->parallel_scan->ps_offset_am);
        pg_atomic_write_u32(&ps->curr, 0u);
    }
}

/* Options and validation stubs */
static bytea *
smol_options(Datum reloptions, bool validate)
{
    (void) reloptions; (void) validate;
    return NULL;
}

static bool
smol_validate(Oid opclassoid)
{
    (void) opclassoid;
    return true;
}

/* --- Helpers ----------------------------------------------------------- */
static void
smol_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                  Cost *indexStartupCost, Cost *indexTotalCost,
                  Selectivity *indexSelectivity, double *indexCorrelation,
                  double *indexPages)
{
    IndexOptInfo *idx;
    double pages;
    (void) root; (void) loop_count;
    idx = path->indexinfo;
    pages = (idx->pages > 0 ? idx->pages : 1);
    *indexStartupCost = 0.0;
    *indexPages = pages;
    /* Cheap, conservative default: modest per-page cost */
    *indexTotalCost = 1.0 + pages * 0.1;
    /* Default selectivity: half the table */
    *indexSelectivity = 0.5;
    *indexCorrelation = 0.0;

    /* Heuristics: if there is no clause on leading key (indexcol 0), discourage SMOL */
    {
        bool have_leading = false;
        ListCell *lc;
        foreach(lc, path->indexclauses)
        {
            IndexClause *ic = (IndexClause *) lfirst(lc);
            if (ic && ic->indexcol == 0)
            {
                have_leading = true;
                break;
            }
        }
        if (!have_leading)
        {
            *indexTotalCost += 1e6; /* steer planner away when scanning without leading-key quals */
        }
    }
}

/* VACUUM/cleanup stub: SMOL is read-only; nothing to reclaim. */
static IndexBulkDeleteResult *
smol_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    (void) info;
    (void) stats;
    return NULL;
}

static void
smol_meta_read(Relation idx, SmolMeta *out)
{
    Buffer buf;
    Page page;
    SmolMeta *m;
    buf = ReadBuffer(idx, 0);
    page = BufferGetPage(buf);
    m = smol_meta_ptr(page);
    *out = *m;
    ReleaseBuffer(buf);
    SMOL_LOGF("meta: magic=0x%x ver=%u nkeyatts=%u len1=%u len2=%u root=%u h=%u",
              out->magic, out->version, out->nkeyatts, out->key_len1, out->key_len2, out->root_blkno, out->height);
}

static void
smol_mark_heap0_allvisible(Relation heapRel)
{
    SMOL_LOG("mark heap blk0 all-visible");
    Buffer heapbuf;
    Buffer vmbuf = InvalidBuffer;
    Page page;
    if (RelationGetNumberOfBlocks(heapRel) == 0)
        return;
    heapbuf = ReadBuffer(heapRel, 0);
    LockBuffer(heapbuf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(heapbuf);
    if (!PageIsAllVisible(page))
    {
        PageSetAllVisible(page);
        MarkBufferDirty(heapbuf);
    }
    visibilitymap_pin(heapRel, 0, &vmbuf);
    (void) visibilitymap_set(heapRel,
                              0 /* heapBlk */, heapbuf,
                              InvalidXLogRecPtr,
                              vmbuf, InvalidTransactionId,
                              VISIBILITYMAP_ALL_VISIBLE);
    if (BufferIsValid(vmbuf))
        ReleaseBuffer(vmbuf);
    UnlockReleaseBuffer(heapbuf);
}

static Buffer
smol_extend(Relation idx)
{
    instr_time t0, t1;
    Buffer buf = ReadBufferExtended(idx, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    INSTR_TIME_SET_CURRENT(t0);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    INSTR_TIME_SET_CURRENT(t1);
    if (smol_debug_log && smol_wait_log_ms > 0)
    {
        double ms = (INSTR_TIME_GET_DOUBLE(t1) - INSTR_TIME_GET_DOUBLE(t0)) * 1000.0;
        if (ms > smol_wait_log_ms)
            SMOL_LOGF("slow LockBuffer(new) wait ~%.1f ms on blk=%u",
                      ms, BufferGetBlockNumber(buf));
    }
    return buf;
}

static void
smol_init_page(Buffer buf, bool leaf, BlockNumber rightlink)
{
    Page page;
    SmolPageOpaqueData *op;
    page = BufferGetPage(buf);
    PageInit(page, BLCKSZ, sizeof(SmolPageOpaqueData));
    op = smol_page_opaque(page);
    op->flags = leaf ? SMOL_F_LEAF : SMOL_F_INTERNAL;
    op->rightlink = rightlink;
    SMOL_LOGF("init page blk=%u leaf=%d rl=%u",
              BufferGetBlockNumber(buf), leaf ? 1 : 0, rightlink);
}

static void
smol_build_tree_from_sorted(Relation idx, const void *keys, Size nkeys, uint16 key_len)
{
    /*
     * Leaf layout (single-column):
     *   item[1] payload = [uint16 nkeys][key bytes...]
     * Keys are tightly packed, no per-tuple headers, no NULLs, no TIDs.
     * A rightlink in special space forms the right-sibling chain.
     */
    Buffer mbuf;
    Page mpage;
    SmolMeta *meta;
    Buffer rbuf;
    Page rpage;
    SMOL_LOGF("leaf-write(single) start nkeys=%zu", nkeys);
    /* init meta page if new */
    if (RelationGetNumberOfBlocks(idx) == 0)
    {
        mbuf = ReadBufferExtended(idx, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
        mpage = BufferGetPage(mbuf);
        PageInit(mpage, BLCKSZ, 0);
        meta = smol_meta_ptr(mpage);
        meta->magic = SMOL_META_MAGIC;
        meta->version = SMOL_META_VERSION;
        meta->nkeyatts = 1;
        meta->key_len1 = key_len;
        meta->key_len2 = 0;
        meta->root_blkno = InvalidBlockNumber;
        meta->height = 0;
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
        SMOL_LOG("initialized metapage");
    }

    if (nkeys == 0)
    {
        /* empty tree: meta root remains invalid; height 0 */
        SMOL_LOG("build: empty index");
        return;
    }

    /* Create leaf pages and record their refs and per-leaf highkeys */
    SmolLeafRef *leaves = NULL; Size nleaves = 0, aleaves = 0;
    int64 *leaf_high = NULL; Size ahigh = 0;
    Size i = 0;
    BlockNumber prev = InvalidBlockNumber;
    /* Reusable scratch buffer to avoid per-page palloc/free churn */
    char *scratch = (char *) palloc(BLCKSZ);
    int loop_guard = 0;
    while (i < nkeys)
    {
        Buffer buf;
        Page page;
        Size added = 0;
        Buffer pbuf;
        Page p;
        SmolPageOpaqueData *pop;
        BlockNumber cur;
        buf = smol_extend(idx);
        smol_init_page(buf, true, InvalidBlockNumber);
        page = BufferGetPage(buf);
        {
            Size fs = PageGetFreeSpace(page);
            Size avail = (fs > sizeof(ItemIdData)) ? (fs - sizeof(ItemIdData)) : 0;
            Size header = sizeof(uint16);
            /* Number of keys that fit in remaining free space */
            Size max_n = (avail > header) ? ((avail - header) / key_len) : 0;
            Size remaining = nkeys - i;
            Size n_this = (remaining < max_n) ? remaining : max_n;
            Size sz = header + n_this * key_len;
            if (sz > BLCKSZ)
                ereport(ERROR, (errmsg("smol: leaf payload exceeds page size")));
            if (n_this == 0)
                ereport(ERROR, (errmsg("smol: cannot fit any tuple on a leaf (key_len=%u avail=%zu)", key_len, (size_t) avail)));
            memcpy(scratch, &n_this, sizeof(uint16));
            memcpy(scratch + header, (const char *) keys + ((size_t)i * key_len), n_this * key_len);
            if (PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false) == InvalidOffsetNumber)
                ereport(ERROR, (errmsg("smol: failed to add leaf payload")));
            added = n_this;
            /* Record leaf highkey from the source array (last key on this page) */
            {
                int64 v;
                const char *src = (const char *) keys + ((size_t)(i + n_this - 1) * key_len);
                if (key_len == 2) { int16 t; memcpy(&t, src, 2); v = (int64) t; }
                else if (key_len == 4) { int32 t; memcpy(&t, src, 4); v = (int64) t; }
                else { int64 t; memcpy(&t, src, 8); v = t; }
                if (nleaves == ahigh)
                {
                    ahigh = (ahigh == 0 ? 8 : ahigh * 2);
                    leaf_high = (leaf_high == NULL)
                        ? (int64 *) palloc(ahigh * sizeof(int64))
                        : (int64 *) repalloc(leaf_high, ahigh * sizeof(int64));
                }
                leaf_high[nleaves] = v;
            }
            {
                Size old_i = i;
                i += n_this;
                if (i == old_i)
                {
                    loop_guard++;
                    if (loop_guard > 3)
                        ereport(ERROR, (errmsg("smol: leaf build progress stalled (i not advancing)")));
                }
                else
                    loop_guard = 0;
            }
        }
        /* Finish current page and release before linking prev to avoid nested locks */
        MarkBufferDirty(buf);
        cur = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);
        if (BlockNumberIsValid(prev))
        {
            /* link previous leaf to this (now unlocked current) */
            instr_time t0, t1;
            Buffer pbuf2 = ReadBuffer(idx, prev);
            INSTR_TIME_SET_CURRENT(t0);
            LockBuffer(pbuf2, BUFFER_LOCK_EXCLUSIVE);
            INSTR_TIME_SET_CURRENT(t1);
            if (smol_debug_log && smol_wait_log_ms > 0)
            {
                double ms = (INSTR_TIME_GET_DOUBLE(t1) - INSTR_TIME_GET_DOUBLE(t0)) * 1000.0;
                if (ms > smol_wait_log_ms)
                    SMOL_LOGF("slow LockBuffer(prev) wait ~%.1f ms on blk=%u", ms, prev);
            }
            {
                Page p2 = BufferGetPage(pbuf2);
                SmolPageOpaqueData *pop2 = smol_page_opaque(p2);
                pop2->rightlink = cur;
                MarkBufferDirty(pbuf2);
            }
            UnlockReleaseBuffer(pbuf2);
        }

        /* record leaf */
        if (nleaves == aleaves)
        {
            aleaves = (aleaves == 0 ? 8 : aleaves * 2);
            if (leaves == NULL)
            leaves = (SmolLeafRef *) palloc(aleaves * sizeof(SmolLeafRef));
            else
                leaves = (SmolLeafRef *) repalloc(leaves, aleaves * sizeof(SmolLeafRef));
        }
        leaves[nleaves].blk = cur;
        nleaves++;
        prev = cur;
        if (smol_debug_log)
        {
            double pct = (nkeys > 0) ? (100.0 * (double) i / (double) nkeys) : 100.0;
            SMOL_LOGF("leaf built blk=%u items=%zu progress=%.1f%%", cur, added, pct);
        }
    }

    /* If single leaf, set it as root */
    mbuf = ReadBuffer(idx, 0);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    mpage = BufferGetPage(mbuf);
    meta = smol_meta_ptr(mpage);
    if (nleaves == 1)
    {
        meta->root_blkno = leaves[0].blk;
        meta->height = 1;
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
        if (leaves) pfree(leaves);
        SMOL_LOGF("leaf-write(single) finish nleaves=1 height=1 root=%u", meta->root_blkno);
        if (smol_debug_log) smol_log_page_summary(idx);
        return;
    }

    /* Build upper levels until a single root remains */
    {
        BlockNumber rootblk = InvalidBlockNumber;
        uint16 levels = 0;
        smol_build_internal_levels(idx, &leaves[0].blk, leaf_high, nleaves, key_len, &rootblk, &levels);
        meta->root_blkno = rootblk;
        meta->height = (uint16) (1 + levels);
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
        if (leaves) pfree(leaves);
        if (leaf_high) pfree(leaf_high);
        pfree(scratch);
        SMOL_LOGF("leaf-write(single) finish nleaves=%zu height=%u root=%u", nleaves, meta->height, rootblk);
        if (smol_debug_log) smol_log_page_summary(idx);
        return;
    }
}

/* Build 2-column tree from sorted pairs (k1,k2) normalized to int64 */
static void
smol_build_tree2_from_sorted(Relation idx, const int64 *k1v, const int64 *k2v,
                             Size nkeys, uint16 key_len1, uint16 key_len2)
{
    /*
     * Leaf layout (two-column):
     *   item[1] payload =
     *     [uint16 ngroups]
     *     repeat ngroups times: [k1 (key_len1 bytes)][uint32 off][uint16 cnt]
     *     followed by a packed k2 array of length sum(cnt) with key_len2 bytes each
     *   For a run of equal k1 values, we store k1 once and a (off,cnt) pair
     *   describing the slice within the packed k2 array.
     */
    Buffer mbuf;
    Page mpage;
    SmolMeta *meta;
    Buffer rbuf;
    Page rpage;
    SMOL_LOGF("leaf-write(2col) start nkeys=%zu", nkeys);
    /* init meta page if new */
    if (RelationGetNumberOfBlocks(idx) == 0)
    {
        mbuf = ReadBufferExtended(idx, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
        mpage = BufferGetPage(mbuf);
        PageInit(mpage, BLCKSZ, 0);
        meta = smol_meta_ptr(mpage);
        meta->magic = SMOL_META_MAGIC;
        meta->version = SMOL_META_VERSION;
        meta->nkeyatts = 2;
        meta->key_len1 = key_len1;
        meta->key_len2 = key_len2;
        meta->root_blkno = InvalidBlockNumber;
        meta->height = 0;
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
        SMOL_LOG("initialized metapage (2-col)");
    }

    if (nkeys == 0)
        return;

    SmolLeafRef *leaves = NULL; Size nleaves = 0, aleaves = 0;
    int64 *leaf_high1 = NULL; Size ahigh1 = 0;
    Size i = 0;
    BlockNumber prev = InvalidBlockNumber;

    /* Reusable scratch buffer to avoid per-page palloc/free churn */
    char *scratch = (char *) palloc(BLCKSZ);
    while (i < nkeys)
    {
        Size begin_i = i;
        SMOL_LOGF("2col: page-loop begin i=%zu prev_blk=%u remaining=%zu", (size_t) i, (unsigned) prev, (size_t) (nkeys - i));
        Buffer buf = smol_extend(idx);
        SMOL_LOGF("2col: got new buffer blk=%u (after smol_extend)", (unsigned) BufferGetBlockNumber(buf));
        Page page;
        page = BufferGetPage(buf);
        SMOL_LOG("2col: calling smol_init_page(leaf, rightlink=Invalid) ...");
        smol_init_page(buf, true, InvalidBlockNumber);
        SMOL_LOG("2col: returned from smol_init_page");

        /* Page-local accumulators */
        uint16 ng = 0;
        Size hdrsz = (size_t) key_len1 + sizeof(uint32) + sizeof(uint16);
        Size k2_count = 0;
        /* Temporary arrays per page */
        /* We’ll accumulate into a scratch buffer at the end to avoid complex incremental packing */
        /* Reserve available space */
        SMOL_LOG("2col: calling PageGetFreeSpace(page) ...");
        Size avail = PageGetFreeSpace(page);
        SMOL_LOGF("2col: PageGetFreeSpace done avail=%zu (pre ItemId)", (size_t) avail);
        if (avail > sizeof(ItemIdData))
            avail -= sizeof(ItemIdData);
        else
            avail = 0;
        SMOL_LOGF("2col: usable avail after ItemId reservation=%zu", (size_t) avail);

        /* Vectors to hold group info this page (bounded) */
        #define MAX_GROUPS_PAGE 512
        int64 g_k1[MAX_GROUPS_PAGE];
        uint32 g_off[MAX_GROUPS_PAGE];
        uint16 g_cnt[MAX_GROUPS_PAGE];

        /* We'll fill groups until we run out of space */
        Size local_start = i;
        while (i < nkeys)
        {
            /* Determine the extent of the next k1 run */
            SMOL_LOGF("2col: group-scan i=%zu of %zu", (size_t) i, (size_t) nkeys);
            int64 curk1 = k1v[i];
            Size j = i;
            while (j < nkeys && k1v[j] == curk1)
                j++;
            Size run_cnt = j - i;
            SMOL_LOGF("2col: run detected k1=%ld run_cnt=%zu", (long) curk1, (size_t) run_cnt);

            /* Check if group fits as a whole */
            Size need_hdr = sizeof(uint16) + (ng + 1) * hdrsz; /* tentative ng+1 groups */
            Size need_k2 = (k2_count + run_cnt) * key_len2;
            Size need_total = need_hdr + need_k2;
            SMOL_LOGF("2col: fit-check ng=%u k2_count=%zu need_hdr=%zu need_k2=%zu need_total=%zu avail=%zu",
                      (unsigned) ng, (size_t) k2_count, (size_t) need_hdr, (size_t) need_k2, (size_t) need_total, (size_t) avail);
            if (need_total > avail)
            {
                /* If nothing yet, split the run to fit page */
                if (ng == 0)
                {
                    Size max_k2 = (avail > (sizeof(uint16) + hdrsz)) ?
                                  ((avail - (sizeof(uint16) + hdrsz)) / key_len2) : 0;
                    SMOL_LOGF("2col: ng=0 overflow; max_k2 fitting this page=%zu (hdrsz=%zu key_len2=%u)",
                              (size_t) max_k2, (size_t) hdrsz, (unsigned) key_len2);
                    if (max_k2 == 0)
                        ereport(ERROR, (errmsg("smol: page too small for 2-col group")));
                    g_k1[ng] = curk1;
                    g_off[ng] = 0;
                    g_cnt[ng] = (uint16) max_k2;
                    ng++;
                    k2_count += max_k2;
                    j = i + max_k2;
                    SMOL_LOGF("2col: split-run placed partial group k1=%ld cnt=%u; advancing j=%zu",
                              (long) curk1, (unsigned) g_cnt[ng-1], (size_t) j);
                }
                /* flush page */
                SMOL_LOG("2col: need_total > avail; breaking to flush page");
                break;
            }

            if (ng >= MAX_GROUPS_PAGE)
            {
                SMOL_LOGF("2col: MAX_GROUPS_PAGE hit (%u); break to flush", (unsigned) MAX_GROUPS_PAGE);
                break;
            }

            g_k1[ng] = curk1;
            g_off[ng] = (uint32) k2_count;
            g_cnt[ng] = (uint16) run_cnt;
            SMOL_LOGF("2col: add-group gi=%u k1=%ld off=%u cnt=%u", (unsigned) ng, (long) g_k1[ng], (unsigned) g_off[ng], (unsigned) g_cnt[ng]);
            ng++;
            k2_count += run_cnt;
            i = j;
            SMOL_LOGF("2col: after-add ng=%u k2_count=%zu i=%zu", (unsigned) ng, (size_t) k2_count, (size_t) i);
        }

        /* Now pack payload */
        Size blob_sz = sizeof(uint16) + (size_t) ng * hdrsz + (size_t) k2_count * key_len2;
        SMOL_LOGF("2col: packing payload ng=%u k2_count=%zu hdrsz=%zu key_len2=%u blob_sz=%zu",
                  (unsigned) ng, (size_t) k2_count, (size_t) hdrsz, (unsigned) key_len2, (size_t) blob_sz);
        if (blob_sz > BLCKSZ)
            ereport(ERROR, (errmsg("smol: 2-col leaf payload exceeds page size")));
        memcpy(scratch, &ng, sizeof(uint16));
        char *gd = scratch + sizeof(uint16);
        SMOL_LOG("2col: writing GroupDir to scratch");
        /* GroupDir */
        for (uint16 gi = 0; gi < ng; gi++)
        {
            char *ptr = gd + (size_t) gi * hdrsz;
            /* write k1 */
            if (key_len1 == 2) { int16 v = (int16) g_k1[gi]; memcpy(ptr, &v, 2); }
            else if (key_len1 == 4) { int32 v = (int32) g_k1[gi]; memcpy(ptr, &v, 4); }
            else { int64 v = (int64) g_k1[gi]; memcpy(ptr, &v, 8); }
            memcpy(ptr + key_len1, &g_off[gi], sizeof(uint32));
            memcpy(ptr + key_len1 + sizeof(uint32), &g_cnt[gi], sizeof(uint16));
            if ((gi & 0xF) == 0) SMOL_LOGF("2col: GroupDir[gi=%u] k1=%ld off=%u cnt=%u", (unsigned) gi, (long) g_k1[gi], (unsigned) g_off[gi], (unsigned) g_cnt[gi]);
        }
        /* k2 array: contiguous pack; bulk memcpy when 8-byte */
        char *k2b = gd + (size_t) ng * hdrsz;
        SMOL_LOGF("2col: packing k2 array count=%zu at scratch offset=%zu", (size_t) k2_count, (size_t) (k2b - scratch));
        if (key_len2 == 8)
        {
            Size bytes = (Size) (i - local_start) * 8;
            SMOL_LOGF("2col: memcpy k2 bulk bytes=%zu", (size_t) bytes);
            memcpy(k2b, (const char *) (k2v + local_start), bytes);
        }
        else
        {
            Size pos = 0;
            for (Size pidx = local_start; pidx < i; pidx++)
            {
                if (key_len2 == 2) { int16 v = (int16) k2v[pidx]; memcpy(k2b + pos*2, &v, 2); }
                else /* key_len2 == 4 */ { int32 v = (int32) k2v[pidx]; memcpy(k2b + pos*4, &v, 4); }
                pos++;
                if ((pos & 0x3FFF) == 0) SMOL_LOGF("2col: packed k2 pos=%zu", (size_t) pos);
            }
        }

        SMOL_LOG("2col: calling PageAddItem for leaf payload ...");
        if (PageAddItem(page, (Item) scratch, blob_sz, FirstOffsetNumber, false, false) == InvalidOffsetNumber)
            ereport(ERROR, (errmsg("smol: failed to add 2-col leaf payload")));
        SMOL_LOG("2col: PageAddItem returned OK");
        /* record last k1 for this page */
        {
            int64 v = (ng > 0) ? g_k1[ng - 1] : 0;
            if (nleaves == ahigh1)
            {
                ahigh1 = (ahigh1 == 0 ? 8 : ahigh1 * 2);
                leaf_high1 = (leaf_high1 == NULL)
                    ? (int64 *) palloc(ahigh1 * sizeof(int64))
                    : (int64 *) repalloc(leaf_high1, ahigh1 * sizeof(int64));
                SMOL_LOGF("2col: resized leaf_high1 to %zu", (size_t) ahigh1);
            }
            leaf_high1[nleaves] = v;
            SMOL_LOGF("2col: leaf_high1[%zu]=%ld", (size_t) nleaves, (long) v);
        }

        /* finish and release current leaf before linking prev to avoid nested locks */
        SMOL_LOGF("2col: calling MarkBufferDirty on blk=%u", (unsigned) BufferGetBlockNumber(buf));
        MarkBufferDirty(buf);
        BlockNumber cur = BufferGetBlockNumber(buf);
        SMOL_LOGF("2col: calling UnlockReleaseBuffer on blk=%u", (unsigned) cur);
        UnlockReleaseBuffer(buf);
        if (BlockNumberIsValid(prev))
        {
            instr_time t0, t1;
            SMOL_LOGF("2col: linking prev leaf %u -> %u (ReadBuffer prev)", (unsigned) prev, (unsigned) cur);
            Buffer pbuf = ReadBuffer(idx, prev);
            SMOL_LOG("2col: ReadBuffer(prev) returned; LockBuffer(EXCLUSIVE) ...");
            INSTR_TIME_SET_CURRENT(t0);
            LockBuffer(pbuf, BUFFER_LOCK_EXCLUSIVE);
            INSTR_TIME_SET_CURRENT(t1);
            if (smol_debug_log && smol_wait_log_ms > 0)
            {
                double ms = (INSTR_TIME_GET_DOUBLE(t1) - INSTR_TIME_GET_DOUBLE(t0)) * 1000.0;
                if (ms > smol_wait_log_ms)
                    SMOL_LOGF("slow LockBuffer(prev,2col) wait ~%.1f ms on blk=%u", ms, prev);
            }
            SMOL_LOG("2col: locked prev; BufferGetPage(prev) ...");
            Page p = BufferGetPage(pbuf);
            SMOL_LOG("2col: got page for prev; smol_page_opaque(prev) ...");
            SmolPageOpaqueData *pop = smol_page_opaque(p);
            pop->rightlink = cur;
            SMOL_LOGF("2col: set prev->rightlink=%u; MarkBufferDirty(prev) ...", (unsigned) cur);
            MarkBufferDirty(pbuf);
            SMOL_LOG("2col: UnlockReleaseBuffer(prev) ...");
            UnlockReleaseBuffer(pbuf);
            SMOL_LOG("2col: prev link done");
        }

        if (nleaves == aleaves)
        {
            aleaves = (aleaves == 0 ? 8 : aleaves * 2);
            if (leaves == NULL)
                leaves = (SmolLeafRef *) palloc(aleaves * sizeof(SmolLeafRef));
            else
                leaves = (SmolLeafRef *) repalloc(leaves, aleaves * sizeof(SmolLeafRef));
            SMOL_LOGF("2col: resized leaves array to %zu", (size_t) aleaves);
        }
        leaves[nleaves].blk = cur;
        nleaves++;
        prev = cur;
        /* stall guard: ensure i advanced this iteration */
        if (i == begin_i)
            ereport(ERROR, (errmsg("smol: 2-col leaf build progress stalled (i not advancing)")));
        SMOL_LOGF("2col: page-loop end; wrote leaf blk=%u, nleaves=%zu, next i=%zu", (unsigned) cur, (size_t) nleaves, (size_t) i);
    }

    /* set root */
    mbuf = ReadBuffer(idx, 0);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    mpage = BufferGetPage(mbuf);
    meta = smol_meta_ptr(mpage);
    if (nleaves == 1)
    {
        meta->root_blkno = leaves[0].blk;
        meta->height = 1;
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
        if (leaves) pfree(leaves);
        SMOL_LOGF("leaf-write(2col) finish nleaves=1 height=1 root=%u", meta->root_blkno);
        if (smol_debug_log) smol_log_page_summary(idx);
        return;
    }

    {
        BlockNumber rootblk = InvalidBlockNumber;
        uint16 levels = 0;
        smol_build_internal_levels(idx, &leaves[0].blk, leaf_high1, nleaves, key_len1, &rootblk, &levels);
        meta->root_blkno = rootblk;
        meta->height = (uint16) (1 + levels);
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
        if (leaves) pfree(leaves);
        if (leaf_high1) pfree(leaf_high1);
        pfree(scratch);
        SMOL_LOGF("leaf-write(2col) finish nleaves=%zu height=%u root=%u", nleaves, meta->height, meta->root_blkno);
        if (smol_debug_log) smol_log_page_summary(idx);
        return;
    }
}

static BlockNumber
smol_find_first_leaf(Relation idx, int64 lower_bound, Oid atttypid, uint16 key_len)
{
    SmolMeta meta;
    smol_meta_read(idx, &meta);
    BlockNumber cur = meta.root_blkno;
    uint16 levels = meta.height;
    while (levels > 1)
    {
        Buffer buf = ReadBuffer(idx, cur);
        Page page = BufferGetPage(buf);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
        BlockNumber child = InvalidBlockNumber;
        for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
        {
            char *itp = (char *) PageGetItem(page, PageGetItemId(page, off));
            BlockNumber c;
            memcpy(&c, itp, sizeof(BlockNumber));
            char *keyp = itp + sizeof(BlockNumber);
            if (smol_cmp_keyptr_bound(keyp, key_len, atttypid, lower_bound) >= 0)
            { child = c; break; }
        }
        if (!BlockNumberIsValid(child))
        {
            /* choose rightmost child */
            char *itp = (char *) PageGetItem(page, PageGetItemId(page, maxoff));
            memcpy(&child, itp, sizeof(BlockNumber));
        }
        ReleaseBuffer(buf);
        cur = child;
        levels--;
    }
    SMOL_LOGF("find_first_leaf: leaf=%u for bound=%ld height=%u", cur, (long) lower_bound, meta.height);
    return cur;
}

static Datum
smol_read_key_as_datum(Page page, OffsetNumber off, Oid atttypid, uint16 key_len)
{
    char *p = smol_leaf_keyptr(page, off, key_len);
    if (atttypid == INT2OID)
    {
        int16 v16; memcpy(&v16, p, sizeof(int16)); return Int16GetDatum(v16);
    }
    else if (atttypid == INT4OID)
    {
        int32 v32; memcpy(&v32, p, sizeof(int32)); return Int32GetDatum(v32);
    }
    else
    {
        int64 v64; memcpy(&v64, p, sizeof(int64)); return Int64GetDatum(v64);
    }
}


static inline int
smol_cmp_keyptr_bound(const char *keyp, uint16 key_len, Oid atttypid, int64 bound)
{
    if (atttypid == INT2OID)
    { int16 v; memcpy(&v, keyp, sizeof(int16)); return (v > bound) - (v < bound); }
    else if (atttypid == INT4OID)
    { int32 v; memcpy(&v, keyp, sizeof(int32)); return ((int64)v > bound) - ((int64)v < bound); }
    else
    { int64 v; memcpy(&v, keyp, sizeof(int64)); return (v > bound) - (v < bound); }
}

static inline uint16
smol_leaf_nitems(Page page)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 n;
    memcpy(&n, p, sizeof(uint16));
    return n;
}

static inline char *
smol_leaf_keyptr(Page page, uint16 idx, uint16 key_len)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 n;
    memcpy(&n, p, sizeof(uint16));
    if (idx < 1 || idx > n)
        return NULL;
    return p + sizeof(uint16) + ((size_t)(idx - 1)) * key_len;
}

/* Two-column helpers */
static inline uint16
smol2_leaf_ngroups(Page page)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 g;
    memcpy(&g, p, sizeof(uint16));
    return g;
}

static inline char *
smol2_group_k1_ptr(Page page, uint16 g, uint16 key_len1)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *base = (char *) PageGetItem(page, iid);
    char *gd = base + sizeof(uint16);
    size_t hdrsz = (size_t) key_len1 + sizeof(uint32) + sizeof(uint16);
    return gd + (size_t) g * hdrsz;
}

static inline void
smol2_group_offcnt(Page page, uint16 g, uint16 key_len1, uint32 *off, uint16 *cnt)
{
    char *k1p = smol2_group_k1_ptr(page, g, key_len1);
    char *p = k1p + key_len1;
    memcpy(off, p, sizeof(uint32));
    memcpy(cnt, p + sizeof(uint32), sizeof(uint16));
}

static inline char *
smol2_k2_base(Page page, uint16 key_len1, uint16 ngroups)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *base = (char *) PageGetItem(page, iid);
    char *gd = base + sizeof(uint16);
    size_t hdrsz = (size_t) key_len1 + sizeof(uint32) + sizeof(uint16);
    return gd + (size_t) ngroups * hdrsz;
}

static inline char *
smol2_k2_ptr(Page page, uint32 idx, uint16 key_len2, uint16 key_len1, uint16 ngroups)
{
    char *kb = smol2_k2_base(page, key_len1, ngroups);
    return kb + (size_t) idx * key_len2;
}

static inline char *
smol2_last_k1_ptr(Page page, uint16 key_len1)
{
    uint16 g = smol2_leaf_ngroups(page);
    if (g == 0) return NULL;
    return smol2_group_k1_ptr(page, g - 1, key_len1);
}

static inline int
smol_cmp_k1_group(Page page, uint16 g, uint16 key_len1, Oid atttypid, int64 bound)
{
    char *p = smol2_group_k1_ptr(page, g, key_len1);
    return smol_cmp_keyptr_bound(p, key_len1, atttypid, bound);
}

static void
smol2_build_leaf_cache(SmolScanOpaque so, Page page)
{
    uint16 ng = smol2_leaf_ngroups(page);
    uint16 start_g = 0;
    /* Honor leading-key lower bound by starting from cur_group when set */
    if (so->have_bound && so->cur_group < ng)
        start_g = so->cur_group;
    size_t hdrsz = (size_t) so->key_len + sizeof(uint32) + sizeof(uint16);
    uint32 total = 0;
    /* First pass: compute total rows */
    for (uint16 gi = start_g; gi < ng; gi++)
    {
        uint32 off; uint16 cnt;
        smol2_group_offcnt(page, gi, so->key_len, &off, &cnt);
        total += cnt;
    }
    if (so->leaf_k1) pfree(so->leaf_k1);
    if (so->leaf_k2) pfree(so->leaf_k2);
    so->leaf_k1 = (int64 *) palloc(total * sizeof(int64));
    so->leaf_k2 = (int64 *) palloc(total * sizeof(int64));
    so->leaf_n = total;
    so->leaf_i = 0;
    char *k2b = smol2_k2_base(page, so->key_len, ng);
    uint32 idx = 0;
    for (uint16 gi = start_g; gi < ng; gi++)
    {
        char *gk1 = smol2_group_k1_ptr(page, gi, so->key_len);
        int64 v1;
        if (so->key_len == 2) { int16 t; memcpy(&t, gk1, 2); v1 = (int64) t; }
        else if (so->key_len == 4) { int32 t; memcpy(&t, gk1, 4); v1 = (int64) t; }
        else { int64 t; memcpy(&t, gk1, 8); v1 = t; }
        uint32 off; uint16 cnt; smol2_group_offcnt(page, gi, so->key_len, &off, &cnt);
        for (uint32 j = 0; j < cnt; j++)
        {
            char *k2p = k2b + ((size_t)(off + j) * so->key_len2);
            int64 v2;
            if (so->key_len2 == 2) { int16 t; memcpy(&t, k2p, 2); v2 = (int64) t; }
            else if (so->key_len2 == 4) { int32 t; memcpy(&t, k2p, 4); v2 = (int64) t; }
            else { int64 t; memcpy(&t, k2p, 8); v2 = t; }
            so->leaf_k1[idx] = v1;
            so->leaf_k2[idx] = v2;
            idx++;
        }
    }
}

/* Build internal levels from a linear list of children (blk, highkey) until a single root remains. */
static void
smol_build_internal_levels(Relation idx,
                           BlockNumber *child_blks, const int64 *child_high,
                           Size nchildren, uint16 key_len,
                           BlockNumber *out_root, uint16 *out_levels)
{
    BlockNumber *cur_blks = child_blks;
    const int64 *cur_high = child_high;
    Size cur_n = nchildren;
    uint16 levels = 0;

    /* Temporary arrays for next level; allocate conservatively (worst case ~cur_n/2) */
    while (cur_n > 1)
    {
        /* Build a sequence of internal pages from current level */
        Size cap_next = (cur_n / 2) + 2;
        BlockNumber *next_blks = (BlockNumber *) palloc(cap_next * sizeof(BlockNumber));
        int64 *next_high = (int64 *) palloc(cap_next * sizeof(int64));
        Size next_n = 0;

        Size i = 0;
        while (i < cur_n)
        {
            Buffer ibuf = smol_extend(idx);
            smol_init_page(ibuf, false, InvalidBlockNumber);
            Page ipg = BufferGetPage(ibuf);
            Size item_sz = sizeof(BlockNumber) + key_len;
            char *item = (char *) palloc(item_sz);
            Size first_i = i;
            /* add as many children as fit */
            for (; i < cur_n; i++)
            {
                memcpy(item, &cur_blks[i], sizeof(BlockNumber));
                if (key_len == 2) { int16 t = (int16) cur_high[i]; memcpy(item + sizeof(BlockNumber), &t, 2); }
                else if (key_len == 4) { int32 t = (int32) cur_high[i]; memcpy(item + sizeof(BlockNumber), &t, 4); }
                else { int64 t = cur_high[i]; memcpy(item + sizeof(BlockNumber), &t, 8); }
                if (PageAddItem(ipg, (Item) item, item_sz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
                {
                    /* page full: back out to next page */
                    break;
                }
            }
            pfree(item);
            MarkBufferDirty(ibuf);
            BlockNumber iblk = BufferGetBlockNumber(ibuf);
            UnlockReleaseBuffer(ibuf);
            /* record new internal page and its highkey from last child inserted */
            {
                Size last = (i > first_i) ? (i - 1) : first_i;
                if (next_n >= cap_next)
                {
                    cap_next = cap_next * 2;
                    next_blks = (BlockNumber *) repalloc(next_blks, cap_next * sizeof(BlockNumber));
                    next_high = (int64 *) repalloc(next_high, cap_next * sizeof(int64));
                }
                next_blks[next_n] = iblk;
                next_high[next_n] = cur_high[last];
                next_n++;
            }
        }
        /* Prepare for next level */
        if (levels > 0)
        {
            /* cur_blks was a palloc we own (not original &leaves[0].blk). Free it. */
            pfree(cur_blks);
            pfree((void *) cur_high);
        }
        cur_blks = next_blks;
        cur_high = next_high;
        cur_n = next_n;
        levels++;
    }

    *out_root = cur_blks[0];
    *out_levels = levels;
    if (levels > 0)
    {
        pfree(cur_blks);
        pfree((void *) cur_high);
    }
}

/* Diagnostic: count page types and log a summary */
static void
smol_log_page_summary(Relation idx)
{
    BlockNumber nblocks = RelationGetNumberOfBlocks(idx);
    int nleaf = 0, ninternal = 0, nmeta = 0;
    for (BlockNumber blk = 0; blk < nblocks; blk++)
    {
        Buffer b = ReadBuffer(idx, blk);
        Page pg = BufferGetPage(b);
        if (blk == 0)
        {
            nmeta++;
        }
        else
        {
            SmolPageOpaqueData *op = (SmolPageOpaqueData *) PageGetSpecialPointer(pg);
            if (op->flags & SMOL_F_LEAF) nleaf++;
            else if (op->flags & SMOL_F_INTERNAL) ninternal++;
        }
        ReleaseBuffer(b);
    }
    SMOL_LOGF("page summary: blocks=%u meta=%d internal=%d leaf=%d",
              (unsigned) nblocks, nmeta, ninternal, nleaf);
}

/* Return rightmost leaf block number */
static BlockNumber
smol_rightmost_leaf(Relation idx)
{
    SmolMeta meta;
    smol_meta_read(idx, &meta);
    BlockNumber cur = meta.root_blkno;
    uint16 levels = meta.height;
    while (levels > 1)
    {
        Buffer buf = ReadBuffer(idx, cur);
        Page page = BufferGetPage(buf);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
        BlockNumber child;
        char *itp = (char *) PageGetItem(page, PageGetItemId(page, maxoff));
        memcpy(&child, itp, sizeof(BlockNumber));
        ReleaseBuffer(buf);
        cur = child;
        levels--;
    }
    return cur;
}

/* Return previous leaf sibling by scanning root (height<=2 prototype) */
static BlockNumber
smol_prev_leaf(Relation idx, BlockNumber cur)
{
    SmolMeta meta;
    Buffer rbuf;
    Page rpage;
    OffsetNumber maxoff;
    BlockNumber prev = InvalidBlockNumber;
    smol_meta_read(idx, &meta);
    if (meta.height <= 1)
        return InvalidBlockNumber;
    rbuf = ReadBuffer(idx, meta.root_blkno);
    rpage = BufferGetPage(rbuf);
    maxoff = PageGetMaxOffsetNumber(rpage);
    for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
    {
        char *itp = (char *) PageGetItem(rpage, PageGetItemId(rpage, off));
        BlockNumber child;
        memcpy(&child, itp, sizeof(BlockNumber));
        if (child == cur)
        {
            if (off > FirstOffsetNumber)
            {
                char *pitp = (char *) PageGetItem(rpage, PageGetItemId(rpage, off - 1));
                memcpy(&prev, pitp, sizeof(BlockNumber));
            }
            break;
        }
    }
    ReleaseBuffer(rbuf);
    return prev;
}

/* Build callbacks and comparators */
static void
ts_build_cb_i16(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsBuildCtxI16 *ctx = (TsBuildCtxI16 *) state;
    (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    tuplesort_putdatum(ctx->ts, values[0], false);
    (*ctx->pnkeys)++;
    if (smol_debug_log && smol_progress_log_every > 0 && (*ctx->pnkeys % (Size) smol_progress_log_every) == 0)
        SMOL_LOGF("collect int2: tuples=%zu", *ctx->pnkeys);
}

static void
ts_build_cb_i32(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsBuildCtxI32 *ctx = (TsBuildCtxI32 *) state;
    (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    tuplesort_putdatum(ctx->ts, values[0], false);
    (*ctx->pnkeys)++;
    if (smol_debug_log && smol_progress_log_every > 0 && (*ctx->pnkeys % (Size) smol_progress_log_every) == 0)
        SMOL_LOGF("collect int4: tuples=%zu", *ctx->pnkeys);
}

static void
ts_build_cb_i64(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsBuildCtxI64 *ctx = (TsBuildCtxI64 *) state;
    (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    tuplesort_putdatum(ctx->ts, values[0], false);
    (*ctx->pnkeys)++;
    if (smol_debug_log && smol_progress_log_every > 0 && (*ctx->pnkeys % (Size) smol_progress_log_every) == 0)
        SMOL_LOGF("collect int8: tuples=%zu", *ctx->pnkeys);
}

static void
smol_build_cb_i16(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsBuildCtxI16 *ctx = (TsBuildCtxI16 *) state;
    (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    tuplesort_putdatum(ctx->ts, values[0], false);
    (*ctx->pnkeys)++;
    if (smol_debug_log && smol_progress_log_every > 0 && (*ctx->pnkeys % (Size) smol_progress_log_every) == 0)
        SMOL_LOGF("collect int2: tuples=%zu", *ctx->pnkeys);
}

static void
smol_build_cb_i32(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsBuildCtxI32 *ctx = (TsBuildCtxI32 *) state;
    (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    tuplesort_putdatum(ctx->ts, values[0], false);
    (*ctx->pnkeys)++;
    if (smol_debug_log && smol_progress_log_every > 0 && (*ctx->pnkeys % (Size) smol_progress_log_every) == 0)
        SMOL_LOGF("collect int4: tuples=%zu", *ctx->pnkeys);
}

static void
smol_build_cb_i64(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsBuildCtxI64 *ctx = (TsBuildCtxI64 *) state;
    (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    tuplesort_putdatum(ctx->ts, values[0], false);
    (*ctx->pnkeys)++;
    if (smol_debug_log && smol_progress_log_every > 0 && (*ctx->pnkeys % (Size) smol_progress_log_every) == 0)
        SMOL_LOGF("collect int8: tuples=%zu", *ctx->pnkeys);
}

static int cmp_int16(const void *a, const void *b)
{ int16 ia = *(const int16 *) a, ib = *(const int16 *) b; return (ia > ib) - (ia < ib); }
static int cmp_int32(const void *a, const void *b)
{ int32 ia = *(const int32 *) a, ib = *(const int32 *) b; return (ia > ib) - (ia < ib); }
static int cmp_int64(const void *a, const void *b)
{ int64 ia = *(const int64 *) a, ib = *(const int64 *) b; return (ia > ib) - (ia < ib); }

/* (removed stale helpers from an older iteration) */

/* 2-col builder helpers */
static void
smol_build_cb_pair(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    PairArrCtx *c = (PairArrCtx *) state;
    (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0] || isnull[1]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    if (*c->pcount == *c->pcap)
    {
        Size newcap = (*c->pcap == 0 ? 1024 : *c->pcap * 2);
        int64 *newk1 = (*c->pcap == 0) ? (int64 *) palloc(newcap * sizeof(int64))
                                       : (int64 *) repalloc(*c->pk1, newcap * sizeof(int64));
        int64 *newk2 = (*c->pcap == 0) ? (int64 *) palloc(newcap * sizeof(int64))
                                       : (int64 *) repalloc(*c->pk2, newcap * sizeof(int64));
        *c->pcap = newcap; *c->pk1 = newk1; *c->pk2 = newk2;
    }
    int64 v1 = (c->t1 == INT2OID) ? (int64) DatumGetInt16(values[0])
               : (c->t1 == INT4OID) ? (int64) DatumGetInt32(values[0])
               : DatumGetInt64(values[0]);
    int64 v2 = (c->t2 == INT2OID) ? (int64) DatumGetInt16(values[1])
               : (c->t2 == INT4OID) ? (int64) DatumGetInt32(values[1])
               : DatumGetInt64(values[1]);
    (*c->pk1)[*c->pcount] = v1;
    (*c->pk2)[*c->pcount] = v2;
    (*c->pcount)++;
    if (smol_debug_log && smol_progress_log_every > 0 && (*c->pcount % (Size) smol_progress_log_every) == 0)
        SMOL_LOGF("collect pair: tuples=%zu", *c->pcount);
}

static void
ts_build_cb_pair(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsPairCtx *c = (TsPairCtx *) state;
    (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0] || isnull[1]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    /* Feed via a virtual slot to avoid per-call heap_form_tuple overhead */
    {
        TupleTableSlot *vslot = MakeSingleTupleTableSlot(c->tdesc, &TTSOpsVirtual);
        vslot->tts_values[0] = values[0]; vslot->tts_isnull[0] = false;
        vslot->tts_values[1] = values[1]; vslot->tts_isnull[1] = false;
        ExecStoreVirtualTuple(vslot);
        tuplesort_puttupleslot(c->ts, vslot);
        ExecDropSingleTupleTableSlot(vslot);
    }
    (*c->pcount)++;
    if (smol_debug_log && smol_progress_log_every > 0 && (*c->pcount % (Size) smol_progress_log_every) == 0)
        SMOL_LOGF("collect pair(ts): tuples=%zu", *c->pcount);
}

/* ---- Radix sort for fixed-width signed integers ------------------------- */
static inline uint64 smol_norm64(int64 v) { return (uint64) v ^ UINT64_C(0x8000000000000000); }
static inline uint32 smol_norm32(int32 v) { return (uint32) v ^ UINT32_C(0x80000000); }
static inline uint16 smol_norm16(int16 v) { return (uint16) v ^ UINT16_C(0x8000); }

static void smol_sort_int64(int64 *keys, Size n)
{
    if (n < 2) return;
    uint64 *norm = (uint64 *) palloc(n * sizeof(uint64));
    uint64 *tmpn = (uint64 *) palloc(n * sizeof(uint64));
    for (Size i = 0; i < n; i++) norm[i] = smol_norm64(keys[i]);
    uint32 count[256];
    for (int pass = 0; pass < 8; pass++)
    {
        memset(count, 0, sizeof(count));
        for (Size i = 0; i < n; i++) count[(norm[i] >> (pass * 8)) & 0xFF]++;
        uint32 sum = 0; for (int b = 0; b < 256; b++) { uint32 c = count[b]; count[b] = sum; sum += c; }
        for (Size i = 0; i < n; i++) tmpn[count[(norm[i] >> (pass * 8)) & 0xFF]++] = norm[i];
        uint64 *swap = norm; norm = tmpn; tmpn = swap;
    }
    /* Write back sorted order by converting back to signed */
    for (Size i = 0; i < n; i++) keys[i] = (int64) (norm[i] ^ UINT64_C(0x8000000000000000));
    pfree(norm); pfree(tmpn);
}

static void smol_sort_int32(int32 *keys, Size n)
{
    if (n < 2) return;
    uint32 *norm = (uint32 *) palloc(n * sizeof(uint32));
    uint32 *tmpn = (uint32 *) palloc(n * sizeof(uint32));
    for (Size i = 0; i < n; i++) norm[i] = smol_norm32(keys[i]);
    uint32 count[256];
    for (int pass = 0; pass < 4; pass++)
    {
        memset(count, 0, sizeof(count));
        for (Size i = 0; i < n; i++) count[(norm[i] >> (pass * 8)) & 0xFF]++;
        uint32 sum = 0; for (int b = 0; b < 256; b++) { uint32 c = count[b]; count[b] = sum; sum += c; }
        for (Size i = 0; i < n; i++) tmpn[count[(norm[i] >> (pass * 8)) & 0xFF]++] = norm[i];
        uint32 *swap = norm; norm = tmpn; tmpn = swap;
    }
    for (Size i = 0; i < n; i++) keys[i] = (int32) (norm[i] ^ UINT32_C(0x80000000));
    pfree(norm); pfree(tmpn);
}

static void smol_sort_int16(int16 *keys, Size n)
{
    if (n < 2) return;
    uint16 *norm = (uint16 *) palloc(n * sizeof(uint16));
    uint16 *tmpn = (uint16 *) palloc(n * sizeof(uint16));
    for (Size i = 0; i < n; i++) norm[i] = smol_norm16(keys[i]);
    uint32 count[256];
    for (int pass = 0; pass < 2; pass++)
    {
        memset(count, 0, sizeof(count));
        for (Size i = 0; i < n; i++) count[(norm[i] >> (pass * 8)) & 0xFF]++;
        uint32 sum = 0; for (int b = 0; b < 256; b++) { uint32 c = count[b]; count[b] = sum; sum += c; }
        for (Size i = 0; i < n; i++) tmpn[count[(norm[i] >> (pass * 8)) & 0xFF]++] = norm[i];
        uint16 *swap = norm; norm = tmpn; tmpn = swap;
    }
    for (Size i = 0; i < n; i++) keys[i] = (int16) (norm[i] ^ UINT16_C(0x8000));
    pfree(norm); pfree(tmpn);
}

/* Stable radix sort pairs (k1,k2) by ascending k1 then k2 */
/* Helper: sort idx by 64-bit unsigned key via 8 byte-wise stable passes */
static void
smol_radix_sort_idx_u64_16(const uint64 *key, uint32 *idx, uint32 *tmp, Size n)
{
    /* 16-bit digits: 4 stable passes per 64-bit key */
    enum { RAD = 65536 };
    static uint32 *count = NULL;
    static Size count_sz = 0;
    if (count_sz < RAD)
    {
        if (count) pfree(count);
        count = (uint32 *) palloc(RAD * sizeof(uint32));
        count_sz = RAD;
    }
    for (int p = 0; p < 4; p++)
    {
        memset(count, 0, RAD * sizeof(uint32));
        int shift = p * 16;
        for (Size i = 0; i < n; i++) { uint64 v = key[idx[i]]; count[(v >> shift) & 0xFFFF]++; }
        uint32 sum = 0; for (int b = 0; b < RAD; b++) { uint32 c = count[b]; count[b] = sum; sum += c; }
        for (Size i = 0; i < n; i++) { uint64 v = key[idx[i]]; tmp[count[(v >> shift) & 0xFFFF]++] = idx[i]; }
        uint32 *swap = idx; idx = tmp; tmp = swap;
    }
}

static void
smol_sort_pairs64(int64 *k1, int64 *k2, Size n)
{
    if (n < 2) return;
    uint64 *k1u = (uint64 *) palloc(n * sizeof(uint64));
    uint64 *k2u = (uint64 *) palloc(n * sizeof(uint64));
    for (Size i = 0; i < n; i++) { k1u[i] = smol_norm64(k1[i]); k2u[i] = smol_norm64(k2[i]); }
    uint32 *idx = (uint32 *) palloc(n * sizeof(uint32));
    uint32 *tmp = (uint32 *) palloc(n * sizeof(uint32));
    for (uint32 i = 0; i < (uint32) n; i++) idx[i] = i;
    /* Sort by k2, then by k1 (stable) using 16-bit digits */
    smol_radix_sort_idx_u64_16(k2u, idx, tmp, n);
    smol_radix_sort_idx_u64_16(k1u, idx, tmp, n);
    /* Apply permutation to k1/k2 in place */
    int64 *out1 = (int64 *) palloc(n * sizeof(int64));
    int64 *out2 = (int64 *) palloc(n * sizeof(int64));
    for (Size i = 0; i < n; i++) { out1[i] = k1[idx[i]]; out2[i] = k2[idx[i]]; }
    memcpy(k1, out1, n * sizeof(int64));
    memcpy(k2, out2, n * sizeof(int64));
    pfree(out1); pfree(out2);
    pfree(idx); pfree(tmp); pfree(k1u); pfree(k2u);
}

/* LSD radix sort on (k1,k2) pairs operating on the rows directly (no index array).
 * Stable and linear-time: 4 passes on k2 (low to high 16-bit digits), then 4 passes on k1. */
static void
smol_sort_pairs_rows64(int64 *k1, int64 *k2, Size n)
{
    if (n < 2) return;
    int64 *t1 = (int64 *) palloc(n * sizeof(int64));
    int64 *t2 = (int64 *) palloc(n * sizeof(int64));
    uint32 *count = (uint32 *) palloc0(65536 * sizeof(uint32));

    /* 4 passes on k2 (low to high) */
    for (int pass = 0; pass < 4; pass++)
    {
        memset(count, 0, 65536 * sizeof(uint32));
        for (Size i = 0; i < n; i++)
        {
            uint64 u = (uint64) k2[i] ^ UINT64_C(0x8000000000000000);
            uint16 d = (uint16) ((u >> (pass * 16)) & 0xFFFF);
            count[d]++;
        }
        uint32 sum = 0; for (int d = 0; d < 65536; d++) { uint32 c = count[d]; count[d] = sum; sum += c; }
        for (Size i = 0; i < n; i++)
        {
            uint64 u = (uint64) k2[i] ^ UINT64_C(0x8000000000000000);
            uint16 d = (uint16) ((u >> (pass * 16)) & 0xFFFF);
            uint32 pos = count[d]++;
            t1[pos] = k1[i];
            t2[pos] = k2[i];
        }
        /* swap */
        int64 *sw1 = k1; k1 = t1; t1 = sw1;
        int64 *sw2 = k2; k2 = t2; t2 = sw2;
    }

    /* 4 passes on k1 */
    for (int pass = 0; pass < 4; pass++)
    {
        memset(count, 0, 65536 * sizeof(uint32));
        for (Size i = 0; i < n; i++)
        {
            uint64 u = (uint64) k1[i] ^ UINT64_C(0x8000000000000000);
            uint16 d = (uint16) ((u >> (pass * 16)) & 0xFFFF);
            count[d]++;
        }
        uint32 sum = 0; for (int d = 0; d < 65536; d++) { uint32 c = count[d]; count[d] = sum; sum += c; }
        for (Size i = 0; i < n; i++)
        {
            uint64 u = (uint64) k1[i] ^ UINT64_C(0x8000000000000000);
            uint16 d = (uint16) ((u >> (pass * 16)) & 0xFFFF);
            uint32 pos = count[d]++;
            t1[pos] = k1[i];
            t2[pos] = k2[i];
        }
        /* swap */
        int64 *sw1 = k1; k1 = t1; t1 = sw1;
        int64 *sw2 = k2; k2 = t2; t2 = sw2;
    }

    /* if result currently in temp, copy back */
    /* After even number of swaps (8), data is back in original arrays; but keep safe. */
    pfree(t1);
    pfree(t2);
    pfree(count);
}

/* Fast path: radix sort packed (int16,int16) pairs as 32-bit keys */
static void
smol_sort_pairs16_16_packed(int64 *k1v, int64 *k2v, Size n)
{
    if (n < 2) return;
    uint32 *keys = (uint32 *) palloc(n * sizeof(uint32));
    uint32 *tmp = (uint32 *) palloc(n * sizeof(uint32));
    for (Size i = 0; i < n; i++)
    {
        uint16 a = smol_norm16((int16) k1v[i]);
        uint16 b = smol_norm16((int16) k2v[i]);
        keys[i] = ((uint32) a << 16) | (uint32) b;
    }
    enum { RAD = 65536 };
    uint32 *count = (uint32 *) palloc0(RAD * sizeof(uint32));
    /* low 16 */
    for (Size i = 0; i < n; i++) count[keys[i] & 0xFFFF]++;
    {
        uint32 sum = 0; for (int b = 0; b < RAD; b++) { uint32 c = count[b]; count[b] = sum; sum += c; }
    }
    for (Size i = 0; i < n; i++) tmp[count[keys[i] & 0xFFFF]++] = keys[i];
    /* high 16 */
    memset(count, 0, RAD * sizeof(uint32));
    for (Size i = 0; i < n; i++) count[(tmp[i] >> 16) & 0xFFFF]++;
    {
        uint32 sum = 0; for (int b = 0; b < RAD; b++) { uint32 c = count[b]; count[b] = sum; sum += c; }
    }
    for (Size i = 0; i < n; i++) keys[count[(tmp[i] >> 16) & 0xFFFF]++] = tmp[i];
    for (Size i = 0; i < n; i++)
    {
        uint32 w = keys[i];
        int16 a = (int16) ((w >> 16) ^ UINT16_C(0x8000));
        int16 b = (int16) ((w & 0xFFFF) ^ UINT16_C(0x8000));
        k1v[i] = (int64) a;
        k2v[i] = (int64) b;
    }
    pfree(count); pfree(tmp); pfree(keys);
}

/* Background worker: sort assigned bucket ranges in-place inside DSM arrays */
void
smol_parallel_sort_worker(Datum arg)
{
    SmolWorkerExtra extra;
    dsm_segment *seg;
    char *base;
    SmolParallelHdr *hdr;
    uint64 *bucket_off;
    int64 *dk1;
    int64 *dk2;

    (void) arg;

    /* Install basic signal handlers and unblock signals */
    pqsignal(SIGHUP, SIG_IGN);
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    if (MyBgworkerEntry == NULL)
        ereport(ERROR, (errmsg("smol worker: MyBgworkerEntry is NULL")));
    memcpy(&extra, MyBgworkerEntry->bgw_extra, sizeof(SmolWorkerExtra));

    /* Announce readiness (no DB connection needed) to release the leader */
    BackgroundWorkerInitializeConnection(NULL, NULL, 0);

    elog(LOG, "[smol] worker start: first_bucket=%u nbuckets=%u dsm=%u", extra.first_bucket, extra.nbuckets, (unsigned) extra.handle);
    seg = dsm_attach(extra.handle);
    if (seg == NULL)
        ereport(ERROR, (errmsg("smol worker: failed to attach DSM handle %u", (unsigned) extra.handle)));
    base = (char *) dsm_segment_address(seg);
    hdr = (SmolParallelHdr *) base;
    if (hdr->magic != SMOL_META_MAGIC)
    {
        /* Workers use the same magic as metapage for convenience */
        ereport(ERROR, (errmsg("smol worker: bad DSM magic: 0x%x", hdr->magic)));
    }

    bucket_off = (uint64 *) (base + hdr->off_bucket);
    dk1 = (int64 *) (base + hdr->off_k1);
    dk2 = (int64 *) (base + hdr->off_k2);

    for (uint32 b = extra.first_bucket; b < extra.first_bucket + extra.nbuckets && b < hdr->nbuckets; b++)
    {
        uint64 start = bucket_off[b];
        uint64 end = bucket_off[b + 1];
        Size len = (Size) (end - start);
        if (len <= 1)
            continue;
        /* Row-wise stable radix sort by (k1,k2) for this slice */
        smol_sort_pairs_rows64(dk1 + start, dk2 + start, len);
    }

    elog(LOG, "[smol] worker done: first_bucket=%u nbuckets=%u", extra.first_bucket, extra.nbuckets);
    dsm_detach(seg);
    proc_exit(0);
}
