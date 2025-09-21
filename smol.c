/*
 * smol.c
 *
 * SMOL — Read-only, space-efficient PostgreSQL index access method
 *
 * Goals
 * - Index-only scans: store only fixed-width key values; never store heap TIDs
 * - Read-only: disallow inserts/updates/deletes via the AM after build
 * - Ordered scans: support forward/backward scans with standard btree semantics
 * - Parallel scans: coordinate workers via a shared atomic page counter
 * - Efficiency: compact on-disk format; small index footprint; minimal heap I/O
 * - Logging: traceable when smol.debug_log is enabled; zero overhead when off
 *
 * High-level design
 * - On-disk metapage (block 0): magic/version, nkeyatts, optional first/last-key
 *   directory for attno=1 (int2/int4/int8). Data pages start at block 1.
 * - Data pages: packed, MAXALIGNed fixed-width key payloads only; one line pointer
 *   per item; no per-tuple headers, null bitmaps, or TIDs.
 * - Build: collect keys via table_index_build_scan, enforce fixed-width + no NULLs,
 *   sort with opclass comparator (proc 1), write pages, and mark heap block 0
 *   PD_ALL_VISIBLE then set its VM bit. A synthetic TID (0,1) is returned to the
 *   executor to keep the heap out of the picture.
 * - Scan: require xs_want_itup (IOS). Implement forward/backward and parallel
 *   scans. Lower-bound seek can use the metapage directory or page-level binary
 *   search over last keys.
 *
 * Crash safety is not provided (no WAL/FSM). This file focuses on correctness,
 * simplicity, and performance for read-mostly analytical workloads.
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "catalog/index.h"
#include "catalog/pg_type.h"
#include "commands/vacuum.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/plancat.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"
#include "access/tupmacs.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/genam.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/varbit.h"
#include "access/visibilitymap.h"
#include "commands/trigger.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "port/atomics.h"
#include "access/itup.h"

PG_MODULE_MAGIC;

/* ---- Logging helpers --------------------------------------------------- */

#ifndef SMOL_TRACE
#define SMOL_TRACE 1
#endif

static bool smol_debug_log = false; /* toggled by GUC smol.debug_log */
static int  smol_parallel_chunk_pages = 8; /* tunable via GUC */
static int  smol_prefetch_distance = 1;    /* tunable via GUC */
static int  smol_tuple_reset_period = 8192; /* amortize tuple pallocs */

#if SMOL_TRACE
#define SMOL_LOG(msg) \
    do { if (smol_debug_log) elog(LOG, "[smol] %s:%d: %s", __func__, __LINE__, (msg)); } while (0)
#define SMOL_LOGF(fmt, ...) \
    do { if (smol_debug_log) elog(LOG, "[smol] %s:%d: " fmt, __func__, __LINE__, __VA_ARGS__); } while (0)
#else
#define SMOL_LOG(msg)        ((void) 0)
#define SMOL_LOGF(fmt, ...)  ((void) 0)
#endif

/* Module load hook to define GUCs */
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

    DefineCustomIntVariable("smol.parallel_chunk_pages",
                            "Number of index pages per parallel chunk",
                            "Workers acquire work in chunks of this many pages to reduce atomic contention.",
                            &smol_parallel_chunk_pages,
                            8,
                            1,
                            65536,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.prefetch_distance",
                            "Number of pages to prefetch ahead in serial scans",
                            "In serial scans, smol will prefetch this many pages ahead to reduce I/O stalls.",
                            &smol_prefetch_distance,
                            1,
                            0,
                            256,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.tuple_reset_period",
                            "Reset tuple memory context after this many tuples",
                            "Avoid per-tuple resets by batching frees to amortize overhead.",
                            &smol_tuple_reset_period,
                            8192,
                            1,
                            1048576,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);
}

/* ---- On-disk layout ---------------------------------------------------- */

/* metapage magic/version */
#define SMOL_META_MAGIC   0x53534D4C /* 'SSML' */
#define SMOL_META_VERSION 2

typedef struct SmolMetaPageData
{
    uint32      magic;
    uint16      version;
    uint16      nkeyatts;   /* number of key attributes */
    uint16      natts;      /* total attributes incl. INCLUDE */
    uint16      dir_keylen; /* 0 (none), 2, 4, 8 for first-key directory entry size */
    uint32      dir_count;  /* number of entries (pages or groups) */
    uint16      dir_group;  /* grouping factor (pages per entry), 1 = per-page */
} SmolMetaPageData;

/*
 * smol index tuple structure - stores only the indexed values, no TID
 */
typedef struct SmolTuple
{
    /* Variable-length attribute data follows directly */
    char        data[1];
} SmolTuple;

/*
 * smol scan state
 */
typedef struct SmolScanOpaqueData
{
    bool        started;        /* Has scan been started? */
    Buffer      currBuf;        /* Current buffer */
    OffsetNumber currPos;       /* Current position in buffer */
    int         nkeys;          /* Number of scan keys */
    ScanKey     scankeys;       /* Array of scan keys */
    void       *shared_pscan;   /* pointer to AM shared parallel desc (DSM) */
    /* Per-scan buffers to avoid per-tuple alloc/free */
    Datum      *values_buf;     /* [natts] */
    bool       *isnull_buf;     /* [natts] */
    MemoryContext tuple_cxt;    /* reset per returned tuple */
    /* Cached descriptor data for tight packing */
    int         natts;
    int16      *attlen;         /* [natts] fixed-lengths */
    bool       *attbyval;       /* [natts] */
    int32      *offsets;        /* [natts] packed offsets */
    /* Parallel chunk scheduling */
    uint32      pchunk_next;
    uint32      pchunk_end;
    uint32      pchunk_size;
    int         prefetch_dist;  /* serial-scan prefetch distance */
    int         tuple_since_reset; /* counter to amortize resets */
    int         tuple_reset_period; /* GUC snapshot */
    /* Prebuilt index tuple buffer to avoid per-row palloc */
    IndexTuple  itup_buf;       /* reusable IndexTuple */
    int         itup_len;       /* total length */
    int         itup_data_off;  /* data start offset */
    int32      *itup_write_off; /* [natts] aligned write offsets within data */

    /* Directory for page/group min/max on first key (optional, cached from metapage) */
    int         dir_keylen;     /* 0 if none */
    uint32      dir_count;      /* number of directory entries */
    uint16      dir_group;      /* pages per entry (1 = per-page) */
    char       *dir_first;      /* [dir_count * dir_keylen] */
    char       *dir_last;       /* [dir_count * dir_keylen] */

    /* Simplified numeric bounds for first key (int2/int4/int8 only) */
    bool        has_lb;
    bool        lb_incl;
    int64       lb_i64;
    bool        has_ub;
    bool        ub_incl;
    int64       ub_i64;
} SmolScanOpaqueData;

typedef SmolScanOpaqueData *SmolScanOpaque;

/* ---- Parallel scan shared state ---------------------------------------- */
typedef struct SmolParallelSharedDesc
{
    pg_atomic_uint32 nextBlock; /* next block to scan (1-based, data pages) */
    uint32      endBlock;       /* one past last block */
} SmolParallelSharedDesc;

/* ---- Build-time state -------------------------------------------------- */

typedef struct SmolBuildEntry
{
    Datum      *values;   /* array of natts Datums (no NULLs allowed) */
} SmolBuildEntry;

typedef struct SmolBuildState
{
    Relation        heap;
    Relation        index;
    TupleDesc       itupdesc;
    MemoryContext   cxt;
    int             natts;
    int             nkeyatts;
    int64           nentries;
    int64           cap;
    SmolBuildEntry *arr;
    /* comparator helpers */
    FmgrInfo      **cmp_procs;   /* [nkeyatts] */
    Oid            *collations;  /* [nkeyatts] */
} SmolBuildState;

static SmolBuildState *smol_build_state_g = NULL; /* used by qsort comparator */

/* Helper function declarations */
static bool smol_tuple_matches_keys(IndexScanDesc scan, SmolTuple *itup);
static void smol_extract_tuple_values(IndexScanDesc scan, SmolTuple *itup);
static void smol_seek_lower_bound(IndexScanDesc scan);
static BlockNumber smol_find_lower_bound_block(IndexScanDesc scan);
static Datum smol_load_attr0(TupleDesc tupdesc, SmolTuple *stup);
static int  smol_build_entry_cmp(const void *pa, const void *pb);
static void smol_write_sorted_entries(SmolBuildState *bst);
static void smol_mark_heapblk0_allvisible(Relation heap);
static void smol_scan_load_dir(IndexScanDesc scan);
static void smol_update_bounds(IndexScanDesc scan);
static bool smol_dir_get_group_range(SmolScanOpaque so, BlockNumber blk, int64 *out_min, int64 *out_max);

/* Forward declarations */
static IndexBuildResult *smolbuild(Relation heap, Relation index, IndexInfo *indexInfo);
static void smolbuildempty(Relation index);
static bool smolinsert(Relation index, Datum *values, bool *isnull,
          ItemPointer ht_ctid, Relation heapRel,
          IndexUniqueCheck checkUnique,
          bool indexUnchanged,
          IndexInfo *indexInfo);
static void smolinsertcleanup(Relation index, IndexInfo *indexInfo);
static IndexBulkDeleteResult *smolbulkdelete(IndexVacuumInfo *info,
                                           IndexBulkDeleteResult *stats,
                                           IndexBulkDeleteCallback callback,
                                           void *callback_state);
static IndexBulkDeleteResult *smolvacuumcleanup(IndexVacuumInfo *info,
                                               IndexBulkDeleteResult *stats);
static bool smolcanreturn(Relation index, int attno);
static void smolcostestimate(PlannerInfo *root, IndexPath *path,
                           double loop_count, Cost *indexStartupCost,
                           Cost *indexTotalCost, Selectivity *indexSelectivity,
                           double *indexCorrelation, double *indexPages);
static bytea *smoloptions(Datum reloptions, bool validate);
static bool smolvalidate(Oid opclassoid);
static IndexScanDesc smolbeginscan(Relation index, int nkeys, int norderbys);
static void smolrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
                      ScanKey orderbys, int norderbys);
static bool smolgettuple(IndexScanDesc scan, ScanDirection dir);
static void smolendscan(IndexScanDesc scan);
static Size smol_estimate_parallelscan(Relation indexRelation, int nkeys, int norderbys);
static void smol_init_parallelscan(void *target);
static void smol_parallelrescan(IndexScanDesc scan);

/* trigger to enforce read-only tables */
PG_FUNCTION_INFO_V1(smol_block_writes);
Datum smol_block_writes(PG_FUNCTION_ARGS);

Datum
smol_block_writes(PG_FUNCTION_ARGS)
{
    if (!CALLED_AS_TRIGGER(fcinfo))
        ereport(ERROR, (errmsg("smol_block_writes must be called as a trigger")));
    ereport(ERROR, (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                    errmsg("table is sealed: writes are disabled")));
    PG_RETURN_NULL();
}

/*
 * Handler function: returns the access method's API struct
 */
PG_FUNCTION_INFO_V1(smol_handler);

Datum
smol_handler(PG_FUNCTION_ARGS)
{
    SMOL_LOG("enter smol_handler");
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 5;        /* <, <=, =, >=, > */
    amroutine->amsupport = 1;           /* comparator at proc 1 */
    amroutine->amoptsprocnum = 0;       /* No opclass options */
    amroutine->amcanorder = true;       /* ordered */
    amroutine->amcanorderbyop = false;  /* no ORDER BY operator support */
    amroutine->amcanbackward = true;    /* forward/backward scans */
    amroutine->amcanunique = false;     /* No unique indexes (read-only) */
    amroutine->amcanmulticol = true;    /* Support multi-column indexes */
    amroutine->amoptionalkey = true;    /* Support scans without restriction */
    amroutine->amsearcharray = false;   /* no array ops */
    amroutine->amsearchnulls = false;   /* no NULLs supported */
    amroutine->amstorage = false;       /* No storage type different from column */
    amroutine->amclusterable = false;   /* Cannot be clustered (read-only) */
    amroutine->ampredlocks = false;     /* No predicate locks needed */
    amroutine->amcanparallel = true;    /* support parallel scan */
    amroutine->amcaninclude = false;    /* No INCLUDE column support */
    amroutine->amusemaintenanceworkmem = false; /* No maintenance operations */
    amroutine->amsummarizing = false;   /* Not a summarizing index */
    amroutine->amparallelvacuumoptions = 0; /* No parallel vacuum */
    amroutine->amkeytype = InvalidOid;  /* Variable key types */

    /* Interface functions */
    amroutine->ambuild = smolbuild;
    amroutine->ambuildempty = smolbuildempty;
    amroutine->aminsert = smolinsert;
    amroutine->ambulkdelete = smolbulkdelete;
    amroutine->amvacuumcleanup = smolvacuumcleanup;
    amroutine->amcanreturn = smolcanreturn;
    amroutine->amcostestimate = smolcostestimate;
    amroutine->amoptions = smoloptions;
    amroutine->amproperty = NULL;
    amroutine->ambuildphasename = NULL;
    amroutine->amvalidate = smolvalidate;
    amroutine->amadjustmembers = NULL;
    amroutine->ambeginscan = smolbeginscan;
    amroutine->amrescan = smolrescan;
    amroutine->amgettuple = smolgettuple;
    amroutine->amgetbitmap = NULL;      /* No bitmap scan support */
    amroutine->amendscan = smolendscan;
    amroutine->ammarkpos = NULL;        /* No mark/restore support */
    amroutine->amrestrpos = NULL;
    amroutine->amestimateparallelscan = (amestimateparallelscan_function) smol_estimate_parallelscan;
    amroutine->aminitparallelscan = smol_init_parallelscan;
    amroutine->amparallelrescan = smol_parallelrescan;

    SMOL_LOG("leave smol_handler");
    PG_RETURN_POINTER(amroutine);
}

/*
 * Build a new smol index
 */
static void
smol_build_callback(Relation index,
                    ItemPointer tid,
                    Datum *values,
                    bool *isnull,
                    bool tupleIsAlive,
                    void *state)
{
    SMOL_LOG("enter smol_build_callback");
    SmolBuildState *bst = (SmolBuildState *) state;
    int         i;
    (void) index; (void) tid; (void) tupleIsAlive;

    SMOL_LOGF("callback state natts=%d nkeyatts=%d nentries=%ld cap=%ld",
              bst->natts, bst->nkeyatts, (long) bst->nentries, (long) bst->cap);

    /* No NULLs allowed at all */
    for (i = 0; i < bst->natts; i++)
    {
        if (isnull[i])
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("smol does not support NULL values")));
    }

    if (bst->nentries == bst->cap)
    {
        Size newcap = (bst->cap == 0) ? 1024 : (Size) bst->cap * 2;
        if (bst->cap == 0)
            bst->arr = (SmolBuildEntry *) palloc(sizeof(SmolBuildEntry) * newcap);
        else
            bst->arr = (SmolBuildEntry *) repalloc(bst->arr, sizeof(SmolBuildEntry) * newcap);
        bst->cap = newcap;
        SMOL_LOGF("grow entries array to cap=%ld", (long) bst->cap);
    }

    SmolBuildEntry *e = &bst->arr[bst->nentries++];
    e->values = (Datum *) MemoryContextAlloc(bst->cxt, sizeof(Datum) * bst->natts);
    SMOL_LOGF("store tuple #%ld", (long) (bst->nentries - 1));

    for (i = 0; i < bst->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(bst->itupdesc, i);
        if (attr->attbyval)
        {
            e->values[i] = values[i];
        }
        else
        {
            Size len = (attr->attlen == -1) ? VARSIZE_ANY(DatumGetPointer(values[i]))
                                            : (Size) attr->attlen;
            void *copy = MemoryContextAlloc(bst->cxt, len);
            memcpy(copy, DatumGetPointer(values[i]), len);
            e->values[i] = PointerGetDatum(copy);
        }
    }
}

static IndexBuildResult *
smolbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
    SMOL_LOG("enter smolbuild");
    IndexBuildResult *result;
    MemoryContext   cxt;
    MemoryContext   old;
    int             i;

    /* Enforce AccessExclusiveLock on heap during build */
    LockRelation(heap, AccessExclusiveLock);

    result = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));

    /* Build-time memory context */
    cxt = AllocSetContextCreate(CurrentMemoryContext, "smol build cxt",
                                ALLOCSET_DEFAULT_SIZES);
    old = MemoryContextSwitchTo(cxt);

    SMOL_LOGF("build start index=%u heap=%u", RelationGetRelid(index), RelationGetRelid(heap));

    SmolBuildState bst;
    memset(&bst, 0, sizeof(bst));
    bst.heap = heap;
    bst.index = index;
    bst.itupdesc = RelationGetDescr(index);
    bst.nkeyatts = index->rd_index->indnkeyatts;
    bst.natts = bst.nkeyatts; /* store only key attributes; no INCLUDE support */
    bst.cxt = cxt;
    bst.arr = NULL;
    bst.nentries = 0;
    bst.cap = 0;
    bst.cmp_procs = (FmgrInfo **) palloc0(sizeof(FmgrInfo *) * bst.nkeyatts);
    bst.collations = (Oid *) palloc0(sizeof(Oid) * bst.nkeyatts);

    /* Enforce fixed-width key attributes only (no varlena) */
    for (i = 0; i < bst.natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(bst.itupdesc, i);
        if (attr->attlen == -1)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("smol supports fixed-width types only"),
                     errdetail("Attribute %d (%s) is variable-length.",
                               i + 1, NameStr(attr->attname))));
    }

    for (i = 0; i < bst.nkeyatts; i++)
    {
        bst.cmp_procs[i] = index_getprocinfo(index, i + 1, 1 /* comparator */);
        bst.collations[i] = index->rd_indcollation[i];
    }

    /* Scan table and collect entries */
    SMOL_LOG("collect entries via table_index_build_scan");
    (void) table_index_build_scan(heap, index, indexInfo,
                                  true /* allow_sync */, true /* progress */,
                                  smol_build_callback, (void *) &bst, NULL);

    result->heap_tuples = bst.nentries; /* all rows considered live */

    /* Sort entries according to key comparators */
    smol_build_state_g = &bst;
    if (bst.nentries > 1)
    {
        SMOL_LOGF("sorting %ld entries", (long) bst.nentries);
        qsort(bst.arr, bst.nentries, sizeof(SmolBuildEntry), smol_build_entry_cmp);
    }

    /* Write metapage and data pages */
    SMOL_LOG("write sorted entries to pages");
    smol_write_sorted_entries(&bst);

    /* Ensure executor skips heap by marking heap block 0 all-visible */
    SMOL_LOG("marking heap block 0 all-visible");
    smol_mark_heapblk0_allvisible(heap);

    result->index_tuples = bst.nentries;

    MemoryContextSwitchTo(old);
    MemoryContextDelete(cxt);
    SMOL_LOGF("build finish index=%u entries=%ld",
              RelationGetRelid(index), (long) result->index_tuples);
    return result;
}

static void
smolbuildempty(Relation index)
{
    SMOL_LOG("enter smolbuildempty");
    /* Initialize metapage for a new, empty index */
    Buffer buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(buf);
    PageInit(page, BufferGetPageSize(buf), 0);

    SmolMetaPageData *meta = (SmolMetaPageData *) PageGetContents(page);
    meta->magic = SMOL_META_MAGIC;
    meta->version = SMOL_META_VERSION;
    meta->nkeyatts = index->rd_index->indnkeyatts;
    meta->natts = RelationGetNumberOfAttributes(index);
    meta->dir_keylen = 0;
    meta->dir_count = 0;
    meta->dir_group = 1;

    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}

/*
 * FIXED: Improved tuple insertion with better size calculation
 */
static bool
smolinsert(Relation index, Datum *values, bool *isnull,
          ItemPointer ht_ctid, Relation heapRel,
          IndexUniqueCheck checkUnique,
          bool indexUnchanged,
          IndexInfo *indexInfo)
{
    SMOL_LOG("aminsert called (read-only)");
    /* Read-only: disallow inserts */
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("smol is read-only: aminsert is not supported")));
    return false; /* not reached */
}

static void
smolinsertcleanup(Relation index, IndexInfo *indexInfo)
{
}

static IndexBulkDeleteResult *
smolbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
              IndexBulkDeleteCallback callback, void *callback_state)
{
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
    
    return stats;
}

static IndexBulkDeleteResult *
smolvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    if (stats == NULL)
        stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
        
    return stats;
}

static bool
smolcanreturn(Relation index, int attno)
{
    SMOL_LOGF("amcanreturn attno=%d", attno);
    TupleDesc indexTupdesc = RelationGetDescr(index);
    
    if (attno >= 1 && attno <= indexTupdesc->natts)
        return true;
    
    return false;
}

static void
smolcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                Cost *indexStartupCost, Cost *indexTotalCost,
                Selectivity *indexSelectivity, double *indexCorrelation,
                double *indexPages)
{
    Oid         indexoid = path->indexinfo->indexoid;
    Relation    index = RelationIdGetRelation(indexoid);
    BlockNumber numPages;
    
    numPages = RelationGetNumberOfBlocks(index);
    /* Favor SMOL lightly to encourage usage and parallel planning on large indexes */
    *indexStartupCost = 0.05;
    /* Estimate scanned pages: if there seems to be a lower bound, assume ~20% of pages, else 50% */
    double scanFrac = 0.5;
    if (path->indexinfo->indrestrictinfo != NIL)
        scanFrac = 0.2;
    *indexTotalCost = (Cost) numPages * scanFrac;
    *indexSelectivity = 0.1; /* placeholder */
    *indexCorrelation = 0.0; /* unknown */
    *indexPages = numPages;
    
    RelationClose(index);
}

static bytea *
smoloptions(Datum reloptions, bool validate)
{
    return NULL;
}

static Size
smol_estimate_parallelscan(Relation indexRelation, int nkeys, int norderbys)
{
    return sizeof(SmolParallelSharedDesc);
}

static void
smol_init_parallelscan(void *target)
{
    SmolParallelSharedDesc *ps = (SmolParallelSharedDesc *) target;
    memset(ps, 0, sizeof(SmolParallelSharedDesc));
}

static void
smol_parallelrescan(IndexScanDesc scan)
{
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    ParallelIndexScanDesc piscan = scan->parallel_scan;
    if (!piscan)
        return;
    SmolParallelSharedDesc *ps = (SmolParallelSharedDesc *) OffsetToPointer(piscan, piscan->ps_offset);
    so->shared_pscan = (void *) ps;
    /* Initialize shared bounds once. Do not re-init atomics in each worker. */
    if (ps->endBlock == 0)
    {
        BlockNumber nblocks = RelationGetNumberOfBlocks(scan->indexRelation);
        uint32 start = 1;
        uint32 end = (uint32) nblocks;
        pg_atomic_write_u32(&ps->nextBlock, start);
        ps->endBlock = end;
    }
    so->started = false;
}

static bool
smolvalidate(Oid opclassoid)
{
    return true;
}

static IndexScanDesc
smolbeginscan(Relation index, int nkeys, int norderbys)
{
    SMOL_LOGF("ambeginscan nkeys=%d norderbys=%d", nkeys, norderbys);
    IndexScanDesc scan;
    SmolScanOpaque so;

    scan = RelationGetIndexScan(index, nkeys, norderbys);
    
    so = (SmolScanOpaque) palloc0(sizeof(SmolScanOpaqueData));
    so->started = false;
    so->currBuf = InvalidBuffer;
    so->currPos = InvalidOffsetNumber;
    so->nkeys = 0;
    so->scankeys = NULL;
    so->shared_pscan = NULL;
    so->values_buf = NULL;
    so->isnull_buf = NULL;
    so->tuple_cxt = AllocSetContextCreate(CurrentMemoryContext,
                                          "smol tuple cxt",
                                          ALLOCSET_SMALL_SIZES);
    so->natts = RelationGetDescr(index)->natts;
    so->attlen = (int16 *) palloc(sizeof(int16) * so->natts);
    so->attbyval = (bool *) palloc(sizeof(bool) * so->natts);
    so->offsets = (int32 *) palloc(sizeof(int32) * so->natts);
    so->itup_write_off = (int32 *) palloc(sizeof(int32) * so->natts);
    {
        int i;
        int32 off = 0;
        for (i = 0; i < so->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(index), i);
            so->attlen[i] = attr->attlen;
            so->attbyval[i] = attr->attbyval;
            so->offsets[i] = off;
            off += attr->attlen;
        }
    }
    so->pchunk_next = so->pchunk_end = 0;
    so->pchunk_size = smol_parallel_chunk_pages; /* pages per worker chunk */
    so->prefetch_dist = smol_prefetch_distance;
    so->tuple_since_reset = 0;
    so->tuple_reset_period = smol_tuple_reset_period;
    
    scan->opaque = so;

    /* Load metapage directory (if present) and compute initial bounds */
    smol_scan_load_dir(scan);
    smol_update_bounds(scan);

    /* Build a reusable IndexTuple buffer with correct alignment */
    {
        TupleDesc tupdesc = RelationGetDescr(index);
        /* Prepare dummy values for an initial form to get header/len */
        Datum *dvals = (Datum *) palloc(sizeof(Datum) * so->natts);
        bool  *dnull = (bool *) palloc(sizeof(bool) * so->natts);
        int i;
        for (i = 0; i < so->natts; i++)
        {
            dnull[i] = false;
            dvals[i] = (Datum) 0;
        }
        /* Allocate once; this tuple will be reused per row */
        MemoryContext oldcxt = MemoryContextSwitchTo(CurrentMemoryContext);
        IndexTuple itup = index_form_tuple(tupdesc, dvals, dnull);
        MemoryContextSwitchTo(oldcxt);
        pfree(dvals);
        pfree(dnull);

        so->itup_buf = itup;
        so->itup_len = IndexTupleSize(itup);
        so->itup_data_off = IndexInfoFindDataOffset(itup->t_info);

        /* Compute aligned write offsets inside itup data area */
        int32 offa = so->itup_data_off;
        for (i = 0; i < so->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            char align = attr->attalign;
            if (align == 's')
                offa = (offa + 1) & ~1;
            else if (align == 'i')
                offa = (offa + 3) & ~3;
            else if (align == 'd')
                offa = MAXALIGN(offa);
            /* 'c' means no alignment */
            so->itup_write_off[i] = offa;
            offa += attr->attlen;
        }
    }
    
    return scan;
}

static void
smolrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
          ScanKey orderbys, int norderbys)
{
    SMOL_LOGF("amrescan nscankeys=%d norderbys=%d", nscankeys, norderbys);
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    
    if (BufferIsValid(so->currBuf))
    {
        ReleaseBuffer(so->currBuf);
        so->currBuf = InvalidBuffer;
    }
    
    so->started = false;
    so->currPos = InvalidOffsetNumber;
    
    if (nscankeys > 0)
    {
        if (so->scankeys)
            pfree(so->scankeys);
        so->scankeys = (ScanKey) palloc(nscankeys * sizeof(ScanKeyData));
        memcpy(so->scankeys, scankey, nscankeys * sizeof(ScanKeyData));
        so->nkeys = nscankeys;
    }
    else
    {
        if (so->scankeys)
        {
            pfree(so->scankeys);
            so->scankeys = NULL;
        }
        so->nkeys = 0;
    }

    /* Prepare per-scan reusable buffers sized to number of attributes */
    if (so->values_buf == NULL || so->isnull_buf == NULL)
    {
        so->values_buf = (Datum *) palloc(sizeof(Datum) * so->natts);
        so->isnull_buf = (bool *) palloc(sizeof(bool) * so->natts);
    }

    /* Recompute bounds if scan keys changed */
    smol_update_bounds(scan);
}

/*
 * FIXED: Improved tuple scanning with better validation and debugging
 */
static bool
smolgettuple(IndexScanDesc scan, ScanDirection dir)
{
    SMOL_LOGF("amgettuple dir=%d", dir);
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    Relation index = scan->indexRelation;
    Page page;
    SmolTuple *itup;
    OffsetNumber maxoff;
    BlockNumber nblocks;
    
    /* Only support index-only scans */
    if (!scan->xs_want_itup)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("smol supports index-only scans only")));

    if (!so->started)
    {
        nblocks = RelationGetNumberOfBlocks(index);
        SMOL_LOGF("scan start nblocks=%u", nblocks);
        if (nblocks <= 1) /* only metapage exists */
            return false;
        /* Wire up shared parallel state if available and forward */
        if (dir == ForwardScanDirection && scan->parallel_scan && so->shared_pscan == NULL)
        {
            ParallelIndexScanDesc piscan = scan->parallel_scan;
            SmolParallelSharedDesc *ps = (SmolParallelSharedDesc *) OffsetToPointer(piscan, piscan->ps_offset);
            so->shared_pscan = (void *) ps;
            if (ps->endBlock == 0)
            {
                pg_atomic_write_u32(&ps->nextBlock, 1);
                ps->endBlock = (uint32) nblocks;
            }
        }
        if (dir == ForwardScanDirection && scan->parallel_scan && so->shared_pscan)
        {
            /* Acquire a small chunk from the shared counter */
            SmolParallelSharedDesc *ps = (SmolParallelSharedDesc *) so->shared_pscan;
            /* Adjust shared start block to lower bound if applicable */
            if (so->nkeys > 0)
            {
                uint32 cur = pg_atomic_read_u32(&ps->nextBlock);
                if (cur <= 1)
                {
                    BlockNumber lb = smol_find_lower_bound_block(scan);
                    uint32 want = (uint32) Max((BlockNumber)1, lb);
                    uint32 expected = cur;
                    /* Try to advance nextBlock to lower bound */
                    while (expected < want && !pg_atomic_compare_exchange_u32(&ps->nextBlock, &expected, want))
                    {
                        /* expected updated with current value; loop if still behind */
                        ;
                    }
                }
            }
            if (so->pchunk_next >= so->pchunk_end)
            {
                uint32 base = pg_atomic_fetch_add_u32(&ps->nextBlock, so->pchunk_size);
                so->pchunk_next = base;
                so->pchunk_end = Min(base + so->pchunk_size, ps->endBlock);
                if (so->pchunk_next >= so->pchunk_end)
                    return false;
            }
            uint32 blk = so->pchunk_next++;
            /* Prefetch a few pages ahead within this chunk (start) */
            if (so->prefetch_dist > 0 && index->rd_smgr)
            {
                int k;
                for (k = 0; k < so->prefetch_dist; k++)
                {
                    uint32 pblk = blk + (uint32) k;
                    if (pblk < so->pchunk_end)
                        smgrprefetch(index->rd_smgr, MAIN_FORKNUM, (BlockNumber) pblk);
                    else
                        break;
                }
            }
            so->currBuf = ReadBuffer(index, (BlockNumber) blk);
            LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
            so->currPos = FirstOffsetNumber;
            so->started = true;
        }
        else
        {
            if (dir == BackwardScanDirection)
            {
                /* Start from the last data page for backward scans */
                BlockNumber lastblk = nblocks - 1;
                so->currBuf = ReadBuffer(index, lastblk);
                LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
                Page page0 = BufferGetPage(so->currBuf);
                so->currPos = PageGetMaxOffsetNumber(page0);
                /* Prefetch a few pages back for backward scan start */
                if (so->prefetch_dist > 0 && index->rd_smgr)
                {
                    int k;
                    for (k = 1; k <= so->prefetch_dist; k++)
                    {
                        if (lastblk >= (BlockNumber) k)
                            smgrprefetch(index->rd_smgr, MAIN_FORKNUM, lastblk - (BlockNumber) k);
                        else
                            break;
                    }
                }
                so->started = true;
            }
            else
            {
                smol_seek_lower_bound(scan);
                if (!BufferIsValid(so->currBuf))
                {
                    /* fallback */
                    so->currBuf = ReadBuffer(index, 1);
                    LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
                    so->currPos = FirstOffsetNumber;
                    so->started = true;
                }
                /* Prefetch a few pages ahead at serial-forward scan start */
                if (so->prefetch_dist > 0 && index->rd_smgr)
                {
                    BlockNumber curblk = BufferGetBlockNumber(so->currBuf);
                    BlockNumber nb = RelationGetNumberOfBlocks(index);
                    int k;
                    for (k = 1; k <= so->prefetch_dist; k++)
                    {
                        BlockNumber pblk = curblk + (BlockNumber) k;
                        if (pblk < nb)
                            smgrprefetch(index->rd_smgr, MAIN_FORKNUM, pblk);
                        else
                            break;
                    }
                }
            }
        }
    }
    
    while (BufferIsValid(so->currBuf))
    {
        page = BufferGetPage(so->currBuf);
        maxoff = PageGetMaxOffsetNumber(page);

        /* Page-level pruning using directory min/max on first key */
        if (so->dir_keylen > 0 && (so->has_lb || so->has_ub))
        {
            int64 gmin = 0, gmax = 0;
            BlockNumber curblk = BufferGetBlockNumber(so->currBuf);
            if (smol_dir_get_group_range(so, curblk, &gmin, &gmax))
            {
                bool below_lb = so->has_lb && (so->lb_incl ? (gmax < so->lb_i64) : (gmax <= so->lb_i64));
                bool above_ub = so->has_ub && (so->ub_incl ? (gmin > so->ub_i64) : (gmin >= so->ub_i64));
                if (below_lb)
                {
                    /* Entire page (group) below lower bound: skip page */
                    if (dir == BackwardScanDirection)
                        so->currPos = 0; /* will move to prev page */
                    else
                        so->currPos = maxoff + 1; /* will move to next page */
                }
                else if (above_ub)
                {
                    /* Entire page (group) above upper bound */
                    UnlockReleaseBuffer(so->currBuf);
                    so->currBuf = InvalidBuffer;
                    if (dir == BackwardScanDirection)
                    {
                        /* continue to previous page */
                        BlockNumber nextblk = curblk - 1;
                        if (nextblk >= 1)
                        {
                            so->currBuf = ReadBuffer(index, nextblk);
                            LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
                            Page p = BufferGetPage(so->currBuf);
                            so->currPos = PageGetMaxOffsetNumber(p);
                            continue; /* re-evaluate on new page */
                        }
                        return false;
                    }
                    else
                    {
                        /* In forward scans, we've passed the upper bound */
                        return false;
                    }
                }
            }
        }
        
        /* Iterate over items on the current page in requested direction */
        while ((dir != BackwardScanDirection && so->currPos <= maxoff) ||
               (dir == BackwardScanDirection && so->currPos >= FirstOffsetNumber))
        {
            ItemId itemid = PageGetItemId(page, so->currPos);
            SMOL_LOGF("page=%u pos=%u used=%d dead=%d", BufferGetBlockNumber(so->currBuf), so->currPos, ItemIdIsUsed(itemid), ItemIdIsDead(itemid));
            
            if (ItemIdIsUsed(itemid) && !ItemIdIsDead(itemid))
            {
                itup = (SmolTuple *) PageGetItem(page, itemid);
                
                /* Check if tuple matches scan keys */
                if (so->nkeys == 0 || smol_tuple_matches_keys(scan, itup))
                {
                    /* Extract values for index-only scan */
                    smol_extract_tuple_values(scan, itup);
                    /* Synthesize a constant TID on heap block 0 */
                    ItemPointerSetBlockNumber(&scan->xs_heaptid, 0);
                    ItemPointerSetOffsetNumber(&scan->xs_heaptid, FirstOffsetNumber);
                    SMOL_LOGF("return tuple at page=%u pos=%u", BufferGetBlockNumber(so->currBuf), so->currPos);
                    if (dir == BackwardScanDirection)
                        so->currPos--;
                    else
                        so->currPos++;
                    return true;
                }
                else
                {
                    SMOL_LOGF("tuple did not match scankeys at pos=%u", so->currPos);
                }
            }
            if (dir == BackwardScanDirection)
                so->currPos--;
            else
                so->currPos++;
        }
        
        /* Move to next page */
        BlockNumber currblk2 = BufferGetBlockNumber(so->currBuf);
        UnlockReleaseBuffer(so->currBuf);
        so->currBuf = InvalidBuffer;
        if (dir == ForwardScanDirection && scan->parallel_scan && so->shared_pscan)
        {
            SmolParallelSharedDesc *ps = (SmolParallelSharedDesc *) so->shared_pscan;
            if (so->pchunk_next >= so->pchunk_end)
            {
                uint32 base = pg_atomic_fetch_add_u32(&ps->nextBlock, so->pchunk_size);
                so->pchunk_next = base;
                so->pchunk_end = Min(base + so->pchunk_size, ps->endBlock);
                if (so->pchunk_next >= so->pchunk_end)
                    break;
            }
            uint32 blk = so->pchunk_next++;
            /* Optional light prefetch within the current chunk */
            if (so->prefetch_dist > 0 && index->rd_smgr)
            {
                int k;
                for (k = 1; k <= so->prefetch_dist; k++)
                {
                    uint32 pblk = blk + k;
                    if (pblk < so->pchunk_end)
                        smgrprefetch(index->rd_smgr, MAIN_FORKNUM, (BlockNumber) pblk);
                    else
                        break;
                }
            }
            so->currBuf = ReadBuffer(index, (BlockNumber) blk);
            LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
            so->currPos = FirstOffsetNumber;
        }
        else
        {
            BlockNumber nextblk = (dir == BackwardScanDirection) ? (currblk2 - 1) : (currblk2 + 1);
            nblocks = RelationGetNumberOfBlocks(index);
            if ((dir != BackwardScanDirection && nextblk < nblocks) ||
                (dir == BackwardScanDirection && nextblk >= 1))
            {
                SMOL_LOGF("advance to %s page %u", (dir == BackwardScanDirection ? "prev" : "next"), nextblk);
                if (dir != BackwardScanDirection && so->prefetch_dist > 0 && index->rd_smgr)
                {
                    int k;
                    for (k = 1; k <= so->prefetch_dist; k++)
                    {
                        BlockNumber pblk = nextblk + k;
                        if (pblk < nblocks)
                            smgrprefetch(index->rd_smgr, MAIN_FORKNUM, pblk);
                        else
                            break;
                    }
                }
                so->currBuf = ReadBuffer(index, nextblk);
                LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
                if (dir == BackwardScanDirection)
                {
                    Page p = BufferGetPage(so->currBuf);
                    so->currPos = PageGetMaxOffsetNumber(p);
                }
                else
                {
                    so->currPos = FirstOffsetNumber;
                }
            }
            else
            {
                break;
            }
        }
    }
    
    return false;
}

static void
smolendscan(IndexScanDesc scan)
{
    SMOL_LOG("amendscan");
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    
    if (BufferIsValid(so->currBuf))
        ReleaseBuffer(so->currBuf);
    
    if (so->scankeys)
        pfree(so->scankeys);

    if (((SmolScanOpaque) scan->opaque)->itup_buf)
        pfree(((SmolScanOpaque) scan->opaque)->itup_buf);

    if (so->values_buf)
        pfree(so->values_buf);
    if (so->isnull_buf)
        pfree(so->isnull_buf);
    if (so->dir_first)
        pfree(so->dir_first);
    if (so->dir_last)
        pfree(so->dir_last);
    if (so->tuple_cxt)
        MemoryContextDelete(so->tuple_cxt);
    
    pfree(so);
}

/*
 * FIXED: Improved key matching with better data extraction
 */
static bool
smol_tuple_matches_keys(IndexScanDesc scan, SmolTuple *itup)
{
    SMOL_LOGF("tuple_matches_keys nkeys=%d", ((SmolScanOpaque) scan->opaque)->nkeys);
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    TupleDesc indexTupdesc = RelationGetDescr(scan->indexRelation);
    int keyno;
    
    if (so->nkeys == 0)
        return true;
    
    for (keyno = 0; keyno < so->nkeys; keyno++)
    {
        ScanKey key = &so->scankeys[keyno];
        int attno = key->sk_attno - 1; /* Convert to 0-based */
        Datum value = (Datum) 0;
        bool isnull = false;
        char *dataptr;
        Form_pg_attribute attr;
        int j;
        
        if (attno >= 0 && attno < indexTupdesc->natts)
        {
            /* FIXED: More robust data extraction */
            dataptr = itup->data + so->offsets[attno];
            attr = TupleDescAttr(indexTupdesc, attno);
            /* Fast path: integer types (int2/int4/int8) direct compare by strategy for any key */
            if (attr->atttypid == INT2OID || attr->atttypid == INT4OID || attr->atttypid == INT8OID)
            {
                int cmpv = 0;
                if (attr->atttypid == INT2OID)
                {
                    int16 v; memcpy(&v, dataptr, 2);
                    int16 a = DatumGetInt16(key->sk_argument);
                    cmpv = (v > a) - (v < a);
                }
                else if (attr->atttypid == INT4OID)
                {
                    int32 v; memcpy(&v, dataptr, 4);
                    int32 a = DatumGetInt32(key->sk_argument);
                    cmpv = (v > a) - (v < a);
                }
                else /* INT8OID */
                {
                    int64 v; memcpy(&v, dataptr, 8);
                    int64 a = DatumGetInt64(key->sk_argument);
                    cmpv = (v > a) - (v < a);
                }
                bool ok = false;
                switch (key->sk_strategy)
                {
                    case 1: ok = (cmpv < 0); break;
                    case 2: ok = (cmpv <= 0); break;
                    case 3: ok = (cmpv == 0); break;
                    case 4: ok = (cmpv >= 0); break;
                    case 5: ok = (cmpv > 0); break;
                    default: ok = false; break;
                }
                if (!ok)
                    return false;
                else
                    continue;
            }
            if (so->attbyval[attno])
            {
                Datum tmp = (Datum) 0;
                memcpy(&tmp, dataptr, Min((Size) so->attlen[attno], sizeof(Datum)));
                value = tmp;
            }
            else
            {
                value = PointerGetDatum(dataptr);
            }
        }
        else
        {
            isnull = true;
        }
        
        /* FIXED: Better null handling and function calls */
        if (isnull)
        {
            if (!(key->sk_flags & SK_ISNULL))
            {
                SMOL_LOGF("key %d requires non-null", keyno + 1);
                return false;
            }
        }
        else
        {
            if (!DatumGetBool(FunctionCall2Coll(&key->sk_func,
                                               key->sk_collation,
                                               value,
                                               key->sk_argument)))
            {
                SMOL_LOGF("key %d compare failed", keyno + 1);
                return false;
            }
        }
    }
    
    SMOL_LOG("all keys matched");
    return true;
}

/*
 * FIXED: Improved tuple value extraction with alignment handling
 */
static void
smol_extract_tuple_values(IndexScanDesc scan, SmolTuple *stup)
{
    SMOL_LOGF("extract tuple values natts=%d", RelationGetDescr(scan->indexRelation)->natts);
    TupleDesc   tupdesc = RelationGetDescr(scan->indexRelation);
    int         natts   = ((SmolScanOpaque) scan->opaque)->natts;
    Datum      *values;
    bool       *isnull;
    char       *dataptr;
    int         i;

    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    values = so->values_buf;
    isnull = so->isnull_buf;

    dataptr = stup->data;

    for (i = 0; i < natts; i++)
    {
        isnull[i] = false; /* no NULLs supported */

        if (so->attbyval[i])
        {
            Datum tmp = (Datum) 0;
            memcpy(&tmp, dataptr, Min((Size) so->attlen[i], sizeof(Datum)));
            values[i] = tmp;
        }
        else
        {
            values[i] = PointerGetDatum(dataptr);
        }

        /* Tightly packed: advance by fixed width */
        dataptr += so->attlen[i];
    }

    /* Populate prebuilt IndexTuple buffer in-place (no per-row palloc) */
    {
        char *base = (char *) so->itup_buf;
        for (i = 0; i < natts; i++)
        {
            char *dst = base + so->itup_write_off[i];
            if (so->attbyval[i])
                store_att_byval(dst, values[i], so->attlen[i]);
            else
                memcpy(dst, DatumGetPointer(values[i]), so->attlen[i]);
        }
        scan->xs_itup = so->itup_buf;
    }
    scan->xs_itupdesc = tupdesc;
    SMOL_LOG("materialized xs_itup");

    scan->xs_hitup = NULL;
    scan->xs_recheck = false;
    ItemPointerSetInvalid(&scan->xs_heaptid);

    /* values/isnull buffers are reused across tuples */

    /* Amortize context resets to reduce per-tuple overhead */
    so->tuple_since_reset++;
    if (so->tuple_since_reset >= so->tuple_reset_period)
    {
        MemoryContextReset(so->tuple_cxt);
        so->tuple_since_reset = 0;
    }
}

/* ---- Local helpers ----------------------------------------------------- */

static int
smol_build_entry_cmp(const void *pa, const void *pb)
{
    SMOL_LOG("cmp entries");
    const SmolBuildEntry *a = (const SmolBuildEntry *) pa;
    const SmolBuildEntry *b = (const SmolBuildEntry *) pb;
    SmolBuildState *bst = smol_build_state_g;
    int i;

    /* Specialize first-key int2/int4/int8 compare to avoid fmgr overhead */
    if (bst->nkeyatts > 0)
    {
        Form_pg_attribute attr0 = TupleDescAttr(bst->itupdesc, 0);
        if (attr0->attbyval && (attr0->attlen == 2 || attr0->attlen == 4 || attr0->attlen == 8))
        {
            if (attr0->attlen == 2)
            {
                int16 va = DatumGetInt16(a->values[0]);
                int16 vb = DatumGetInt16(b->values[0]);
                if (va < vb) return -1; else if (va > vb) return 1;
            }
            else if (attr0->attlen == 4)
            {
                int32 va = DatumGetInt32(a->values[0]);
                int32 vb = DatumGetInt32(b->values[0]);
                if (va < vb) return -1; else if (va > vb) return 1;
            }
            else /* 8 */
            {
                int64 va = DatumGetInt64(a->values[0]);
                int64 vb = DatumGetInt64(b->values[0]);
                if (va < vb) return -1; else if (va > vb) return 1;
            }
            /* tie-break on remaining keys using generic comparator */
            for (i = 1; i < bst->nkeyatts; i++)
            {
                Datum va = a->values[i];
                Datum vb = b->values[i];
                int32 cmp = DatumGetInt32(FunctionCall2Coll(bst->cmp_procs[i],
                                                            bst->collations[i],
                                                            va, vb));
                if (cmp < 0) return -1; else if (cmp > 0) return 1;
            }
            return 0;
        }
    }

    for (i = 0; i < bst->nkeyatts; i++)
    {
        Datum va = a->values[i];
        Datum vb = b->values[i];
        int32 cmp = DatumGetInt32(FunctionCall2Coll(bst->cmp_procs[i],
                                                    bst->collations[i],
                                                    va, vb));
        if (cmp < 0)
            return -1;
        else if (cmp > 0)
            return 1;
    }
    return 0;
}

static void
smol_write_sorted_entries(SmolBuildState *bst)
{
    SMOL_LOG("enter write_sorted_entries");
    Buffer      buf;
    Page        page;
    OffsetNumber offnum;
    int64       i;
    Size        pagesize = BLCKSZ; /* assume default */
    Size        freespace = 0;
    bool        have_data_page = false;

    /* Ensure metapage exists at block 0 */
    if (RelationGetNumberOfBlocks(bst->index) == 0)
    {
        buf = ReadBufferExtended(bst->index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
        PageInit(page, BufferGetPageSize(buf), 0);
        SmolMetaPageData *meta = (SmolMetaPageData *) PageGetContents(page);
        meta->magic = SMOL_META_MAGIC;
        meta->version = SMOL_META_VERSION;
        meta->nkeyatts = bst->nkeyatts;
        meta->natts = bst->natts;
        meta->dir_keylen = 0;
        meta->dir_count = 0;
        meta->dir_group = 1;
        SMOL_LOG("initialized metapage");
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }

    /* Prepare optional first/last-key directory for fixed-width first key */
    int dir_keylen = 0;
    Oid key0 = TupleDescAttr(bst->itupdesc, 0)->atttypid;
    if (key0 == INT2OID) dir_keylen = 2;
    else if (key0 == INT4OID) dir_keylen = 4;
    else if (key0 == INT8OID) dir_keylen = 8;
    char *dir_first = NULL;
    char *dir_last  = NULL;
    Size dircap = 0;
    Size dircnt = 0;
    bool have_lastkey = false;
    int16 last16 = 0; int32 last32 = 0; int64 last64 = 0;

    for (i = 0; i < bst->nentries; i++)
    {
        /* form SmolTuple in memory */
        Size tupsize = 0;
        char *ptr;
        int j;
        for (j = 0; j < bst->natts; j++)
        {
            Form_pg_attribute attr = TupleDescAttr(bst->itupdesc, j);
            /* Fixed-width only; varlena disallowed at build */
            Size len = (Size) attr->attlen;
            tupsize += len; /* tightly packed */
        }

        SmolTuple *stup = (SmolTuple *) palloc(tupsize);
        ptr = (char *) stup;
        for (j = 0; j < bst->natts; j++)
        {
            Form_pg_attribute attr = TupleDescAttr(bst->itupdesc, j);
            Size len = (Size) attr->attlen;
            if (attr->attbyval)
                store_att_byval(ptr, bst->arr[i].values[j], len);
            else
                memcpy(ptr, DatumGetPointer(bst->arr[i].values[j]), len);
            ptr += len; /* tightly packed */
        }

        /* ensure a data page buffer is available with space */
        if (!have_data_page)
        {
            buf = ReadBufferExtended(bst->index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            page = BufferGetPage(buf);
            PageInit(page, BufferGetPageSize(buf), 0);
            freespace = PageGetFreeSpace(page);
            have_data_page = true;
            SMOL_LOGF("opened new data page %u", BufferGetBlockNumber(buf));

            /* directory entry for first key on this new page */
            if (dir_keylen > 0)
            {
                if (dircnt == dircap)
                {
                    Size newcap = dircap ? dircap * 2 : 128;
                    if (dircap == 0)
                    {
                        dir_first = (char *) palloc(newcap * dir_keylen);
                        dir_last  = (char *) palloc(newcap * dir_keylen);
                    }
                    else
                    {
                        dir_first = (char *) repalloc(dir_first, newcap * dir_keylen);
                        dir_last  = (char *) repalloc(dir_last,  newcap * dir_keylen);
                    }
                    dircap = newcap;
                }
                /* read first key from current entry values array */
                Datum v0 = bst->arr[i].values[0];
                char *dst = dir_first + dircnt * dir_keylen;
                /* store little-endian host bytes; consumer will memcpy and compare */
                switch (dir_keylen)
                {
                    case 2: { int16 t = DatumGetInt16(v0); memcpy(dst, &t, 2); } break;
                    case 4: { int32 t = DatumGetInt32(v0); memcpy(dst, &t, 4); } break;
                    case 8: { int64 t = DatumGetInt64(v0); memcpy(dst, &t, 8); } break;
                }
                dircnt++;
                have_lastkey = false;
            }
        }

        if (tupsize + sizeof(ItemIdData) > freespace)
        {
            /* finalize current page */
            if (dir_keylen > 0 && have_lastkey)
            {
                char *ldst = dir_last + (dircnt - 1) * dir_keylen;
                switch (dir_keylen)
                {
                    case 2: memcpy(ldst, &last16, 2); break;
                    case 4: memcpy(ldst, &last32, 4); break;
                    case 8: memcpy(ldst, &last64, 8); break;
                }
                have_lastkey = false;
            }
            MarkBufferDirty(buf);
            UnlockReleaseBuffer(buf);
            have_data_page = false;
            /* allocate a new page */
            buf = ReadBufferExtended(bst->index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            page = BufferGetPage(buf);
            PageInit(page, BufferGetPageSize(buf), 0);
            have_data_page = true;
            freespace = PageGetFreeSpace(page);
            SMOL_LOGF("rollover to new data page %u", BufferGetBlockNumber(buf));
        }

        offnum = PageAddItem(page, (Item) stup, tupsize, InvalidOffsetNumber, false, false);
        if (offnum == InvalidOffsetNumber)
        {
            UnlockReleaseBuffer(buf);
            pfree(stup);
            ereport(ERROR, (errmsg("failed to add item to index page")));
        }

        freespace = PageGetFreeSpace(page);
        SMOL_LOGF("wrote tuple size=%zu at page=%u off=%u freespace=%zu", (size_t) tupsize, BufferGetBlockNumber(buf), offnum, (size_t) freespace);
        if (dir_keylen > 0)
        {
            Datum v0 = bst->arr[i].values[0];
            switch (dir_keylen)
            {
                case 2: last16 = DatumGetInt16(v0); break;
                case 4: last32 = DatumGetInt32(v0); break;
                case 8: last64 = DatumGetInt64(v0); break;
            }
            have_lastkey = true;
        }
        pfree(stup);
    }

    if (have_data_page)
    {
        MarkBufferDirty(buf);
        UnlockReleaseBuffer(buf);
    }
    (void) pagesize; /* quiet not-used if asserts off */

    /* finalize last page's last key */
    if (dir_keylen > 0 && have_lastkey)
    {
        char *ldst = dir_last + (dircnt - 1) * dir_keylen;
        switch (dir_keylen)
        {
            case 2: memcpy(ldst, &last16, 2); break;
            case 4: memcpy(ldst, &last32, 4); break;
            case 8: memcpy(ldst, &last64, 8); break;
        }
    }

    /* Write directories into metapage if available and fit (with grouping if needed) */
    if (dir_keylen > 0 && dircnt > 0)
    {
        Buffer mbuf = ReadBuffer(bst->index, 0);
        LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
        Page mpage = BufferGetPage(mbuf);
        SmolMetaPageData *meta = (SmolMetaPageData *) PageGetContents(mpage);
        Size avail = BLCKSZ - MAXALIGN(sizeof(PageHeaderData)) - sizeof(SmolMetaPageData);
        Size per_entry = (Size)dir_keylen * 2;
        uint32 max_entries = (per_entry > 0) ? (uint32) (avail / per_entry) : 0;
        if (max_entries > 0)
        {
            uint32 groups = (dircnt <= max_entries) ? (uint32) dircnt : max_entries;
            uint32 group_sz = (uint32) ((dircnt + groups - 1) / groups); /* ceil */
            /* Compress per-page dir into groups */
            Size bytes = (Size) groups * dir_keylen;
            char *cfirst = (char *) palloc(bytes);
            char *clast  = (char *) palloc(bytes);
            uint32 g;
            for (g = 0; g < groups; g++)
            {
                uint32 start = g * group_sz;
                uint32 end = start + group_sz;
                if (end > (uint32) dircnt)
                    end = (uint32) dircnt;
                if (start >= (uint32) dircnt)
                {
                    /* shouldn't happen */
                    start = (uint32) dircnt - 1;
                    end = (uint32) dircnt;
                }
                memcpy(cfirst + g * dir_keylen, dir_first + start * dir_keylen, dir_keylen);
                memcpy(clast  + g * dir_keylen, dir_last  + (end - 1) * dir_keylen, dir_keylen);
            }

            char *dst = ((char *) meta) + sizeof(SmolMetaPageData);
            memcpy(dst, cfirst, bytes);
            memcpy(dst + bytes, clast, bytes);
            meta->dir_keylen = dir_keylen;
            meta->dir_count = groups;
            meta->dir_group = (uint16) ((group_sz == 0) ? 1 : group_sz);
            MarkBufferDirty(mbuf);
            pfree(cfirst);
            pfree(clast);
        }
        UnlockReleaseBuffer(mbuf);
    }
    if (dir_first) pfree(dir_first);
    if (dir_last) pfree(dir_last);
}

/* Load first attribute (key 0) from tuple */
static Datum
smol_load_attr0(TupleDesc tupdesc, SmolTuple *stup)
{
    Form_pg_attribute attr = TupleDescAttr(tupdesc, 0);
    char *dataptr = (char *) stup;
    Datum d = (Datum) 0;
    if (attr->attbyval)
    {
        memcpy(&d, dataptr, Min((Size) attr->attlen, sizeof(Datum)));
        return d;
    }
    else
    {
        return PointerGetDatum(dataptr);
    }
}

/* Seek to lower bound page if possible (only attno=1 bounds used) */
static void
smol_seek_lower_bound(IndexScanDesc scan)
{
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    Relation index = scan->indexRelation;
    TupleDesc tupdesc = RelationGetDescr(index);
    BlockNumber nblocks = RelationGetNumberOfBlocks(index);
    int i;
    bool have_lb = false;
    int lb_strategy = 0; /* 3:=,4:>=,5:> */
    Datum lb_value = (Datum) 0;
    Oid coll = index->rd_indcollation[0];
    FmgrInfo *cmp = NULL;

    for (i = 0; i < so->nkeys; i++)
    {
        ScanKey key = &so->scankeys[i];
        if (key->sk_attno == 1 && (key->sk_strategy == 3 || key->sk_strategy == 4 || key->sk_strategy == 5))
        {
            have_lb = true;
            lb_strategy = key->sk_strategy;
            lb_value = key->sk_argument;
            (void) lb_strategy; /* currently unused beyond presence */
            break;
        }
    }

    if (!have_lb)
    {
        so->currBuf = ReadBuffer(index, 1);
        LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
        so->currPos = FirstOffsetNumber;
        so->started = true;
        return;
    }

    cmp = index_getprocinfo(index, 1, 1);

    BlockNumber low = 1;
    BlockNumber high = nblocks - 1; /* inclusive */

    /* If metapage has a directory for fixed-width keys, use it */
    Buffer mbuf = ReadBuffer(index, 0);
    LockBuffer(mbuf, BUFFER_LOCK_SHARE);
    Page mpage = BufferGetPage(mbuf);
    SmolMetaPageData *meta = (SmolMetaPageData *) PageGetContents(mpage);
    char *mdir = ((char *) meta) + sizeof(SmolMetaPageData);
    int dkeylen = meta->dir_keylen;
    uint32 dcount = meta->dir_count;
    uint16 dgroup = (meta->dir_group == 0) ? 1 : meta->dir_group;
    if (dkeylen > 0 && dcount > 0)
    {
        /* use last-key directory to find first page whose last >= bound */
        const char *mdir_last = mdir + (size_t)dcount * dkeylen;
        int32 target32 = 0; int64 target64 = 0; int16 target16 = 0;
        if (dkeylen == 2) target16 = DatumGetInt16(lb_value);
        else if (dkeylen == 4) target32 = DatumGetInt32(lb_value);
        else if (dkeylen == 8) target64 = DatumGetInt64(lb_value);

        int64 lo = 0, hi = (int64)dcount - 1;
        while (lo < hi)
        {
            int64 mid = lo + (hi - lo) / 2;
            const char *entry = mdir_last + (size_t)mid * dkeylen;
            int32 cmpdir;
            if (dkeylen == 2)
            {
                int16 v; memcpy(&v, entry, 2);
                cmpdir = (v < target16) ? -1 : (v > target16) ? 1 : 0;
            }
            else if (dkeylen == 4)
            {
                int32 v; memcpy(&v, entry, 4);
                cmpdir = (v < target32) ? -1 : (v > target32) ? 1 : 0;
            }
            else
            {
                int64 v; memcpy(&v, entry, 8);
                cmpdir = (v < target64) ? -1 : (v > target64) ? 1 : 0;
            }
            if (cmpdir < 0)
                lo = mid + 1; /* lastkey < bound, move right */
            else
                hi = mid;      /* lastkey >= bound, keep left */
        }
        low = (BlockNumber) (lo * dgroup) + 1; /* data pages start at 1 */
        if (low > high) low = high;
    }
    UnlockReleaseBuffer(mbuf);

    while (low < high)
    {
        BlockNumber mid = low + (high - low) / 2;
        Buffer buf = ReadBuffer(index, mid);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buf);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
        OffsetNumber pos = maxoff;
        int32 c = -1;

        while (pos >= FirstOffsetNumber)
        {
            ItemId itemid = PageGetItemId(page, pos);
            if (ItemIdIsUsed(itemid) && !ItemIdIsDead(itemid))
                break;
            pos--;
        }

        if (pos >= FirstOffsetNumber)
        {
            SmolTuple *stup = (SmolTuple *) PageGetItem(page, PageGetItemId(page, pos));
            Datum lastkey = smol_load_attr0(tupdesc, stup);
            c = DatumGetInt32(FunctionCall2Coll(cmp, coll, lastkey, lb_value));
        }

        UnlockReleaseBuffer(buf);

        if (c < 0)
            low = mid + 1; /* lastkey < bound */
        else
            high = mid;     /* lastkey >= bound */
    }

    so->currBuf = ReadBuffer(index, low);
    LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
    /* Within-page lower bound search (assumes dense used items) */
    if (have_lb)
    {
        Page page = BufferGetPage(so->currBuf);
        OffsetNumber lo = FirstOffsetNumber;
        OffsetNumber hi = PageGetMaxOffsetNumber(page);
        while (lo < hi)
        {
            OffsetNumber mid = lo + (hi - lo) / 2;
            SmolTuple *stup = (SmolTuple *) PageGetItem(page, PageGetItemId(page, mid));
            Datum key = smol_load_attr0(tupdesc, stup);
            int32 c = DatumGetInt32(FunctionCall2Coll(cmp, coll, key, lb_value));
            if (c < 0)
                lo = mid + 1;
            else
                hi = mid;
        }
        so->currPos = lo;
    }
    else
    {
        so->currPos = FirstOffsetNumber;
    }
    so->started = true;
}

/* Compute the first data block to scan based on lower-bound scankey on attno=1. */
static BlockNumber
smol_find_lower_bound_block(IndexScanDesc scan)
{
    Relation index = scan->indexRelation;
    TupleDesc tupdesc = RelationGetDescr(index);
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    BlockNumber nblocks = RelationGetNumberOfBlocks(index);
    int i;
    bool have_lb = false;
    int lb_strategy = 0; /* 3:=,4:>=,5:> */
    Datum lb_value = (Datum) 0;
    Oid coll = index->rd_indcollation[0];
    FmgrInfo *cmp = NULL;

    if (nblocks <= 1)
        return 1;

    for (i = 0; i < so->nkeys; i++)
    {
        ScanKey key = &so->scankeys[i];
        if (key->sk_attno == 1 && (key->sk_strategy == 3 || key->sk_strategy == 4 || key->sk_strategy == 5))
        {
            have_lb = true;
            lb_strategy = key->sk_strategy;
            lb_value = key->sk_argument;
            (void) lb_strategy;
            break;
        }
    }
    if (!have_lb)
        return 1;

    cmp = index_getprocinfo(index, 1, 1);

    BlockNumber low = 1;
    BlockNumber high = nblocks - 1; /* inclusive */

    /* Consult metapage directory if present */
    Buffer mbuf = ReadBuffer(index, 0);
    LockBuffer(mbuf, BUFFER_LOCK_SHARE);
    Page mpage = BufferGetPage(mbuf);
    SmolMetaPageData *meta = (SmolMetaPageData *) PageGetContents(mpage);
    char *mdir = ((char *) meta) + sizeof(SmolMetaPageData);
    int dkeylen = meta->dir_keylen;
    uint32 dcount = meta->dir_count;
    uint16 dgroup = (meta->dir_group == 0) ? 1 : meta->dir_group;
    if (dkeylen > 0 && dcount > 0)
    {
        const char *mdir_last = mdir + (size_t)dcount * dkeylen;
        int32 target32 = 0; int64 target64 = 0; int16 target16 = 0;
        if (dkeylen == 2) target16 = DatumGetInt16(lb_value);
        else if (dkeylen == 4) target32 = DatumGetInt32(lb_value);
        else if (dkeylen == 8) target64 = DatumGetInt64(lb_value);

        int64 lo = 0, hi = (int64)dcount - 1;
        while (lo < hi)
        {
            int64 mid = lo + (hi - lo) / 2;
            const char *entry = mdir_last + (size_t)mid * dkeylen;
            int32 cmpdir;
            if (dkeylen == 2)
            { int16 v; memcpy(&v, entry, 2); cmpdir = (v < target16) ? -1 : (v > target16) ? 1 : 0; }
            else if (dkeylen == 4)
            { int32 v; memcpy(&v, entry, 4); cmpdir = (v < target32) ? -1 : (v > target32) ? 1 : 0; }
            else
            { int64 v; memcpy(&v, entry, 8); cmpdir = (v < target64) ? -1 : (v > target64) ? 1 : 0; }
            if (cmpdir < 0)
                lo = mid + 1;
            else
                hi = mid;
        }
        low = (BlockNumber) (lo * dgroup) + 1;
        if (low > high) low = high;
    }
    UnlockReleaseBuffer(mbuf);

    /* Fallback: page-level binary search by last key */
    while (low < high)
    {
        BlockNumber mid = low + (high - low) / 2;
        Buffer buf = ReadBuffer(index, mid);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Page page = BufferGetPage(buf);
        OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
        OffsetNumber pos = maxoff;
        int32 c = -1;
        while (pos >= FirstOffsetNumber)
        {
            ItemId itemid = PageGetItemId(page, pos);
            if (ItemIdIsUsed(itemid) && !ItemIdIsDead(itemid))
                break;
            pos--;
        }
        if (pos >= FirstOffsetNumber)
        {
            SmolTuple *stup = (SmolTuple *) PageGetItem(page, PageGetItemId(page, pos));
            Datum lastkey = smol_load_attr0(tupdesc, stup);
            c = DatumGetInt32(FunctionCall2Coll(cmp, coll, lastkey, lb_value));
        }
        UnlockReleaseBuffer(buf);
        if (c < 0)
            low = mid + 1;
        else
            high = mid;
    }
    return low;
}
static void
smol_mark_heapblk0_allvisible(Relation heap)
{
    SMOL_LOG("enter mark_heapblk0_allvisible");
    Buffer heapBuf;
    Buffer vmbuf = InvalidBuffer;
    Page   heapPage;

    /* Pin and lock heap block 0, set PD_ALL_VISIBLE */
    heapBuf = ReadBuffer(heap, 0);
    LockBuffer(heapBuf, BUFFER_LOCK_EXCLUSIVE);
    heapPage = BufferGetPage(heapBuf);
    PageSetAllVisible(heapPage);
    MarkBufferDirty(heapBuf);
    SMOL_LOG("set PD_ALL_VISIBLE on heap block 0");

    /* Pin VM page and set all-visible bit */
    visibilitymap_pin(heap, 0, &vmbuf);
    (void) visibilitymap_set(heap,
                             0 /* heapBlk */, heapBuf,
                             InvalidXLogRecPtr,
                             vmbuf, InvalidTransactionId,
                             VISIBILITYMAP_ALL_VISIBLE);
    SMOL_LOG("set VM bit for heap block 0");

    UnlockReleaseBuffer(heapBuf);
    if (BufferIsValid(vmbuf))
        ReleaseBuffer(vmbuf);
}
/* ---- Parallel scan shared state ---------------------------------------- */
/* (moved near top with other typedefs) */

/* ---- Directory and bounds helpers ------------------------------------- */

static void
smol_scan_load_dir(IndexScanDesc scan)
{
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    Relation index = scan->indexRelation;
    Buffer mbuf;
    Page mpage;
    SmolMetaPageData *meta;
    char *mdir;
    Size bytes;

    so->dir_keylen = 0;
    so->dir_count = 0;
    so->dir_group = 1;
    if (so->dir_first)
    {
        pfree(so->dir_first);
        so->dir_first = NULL;
    }
    if (so->dir_last)
    {
        pfree(so->dir_last);
        so->dir_last = NULL;
    }

    if (RelationGetNumberOfBlocks(index) == 0)
        return;

    mbuf = ReadBuffer(index, 0);
    LockBuffer(mbuf, BUFFER_LOCK_SHARE);
    mpage = BufferGetPage(mbuf);
    meta = (SmolMetaPageData *) PageGetContents(mpage);
    if (meta->magic != SMOL_META_MAGIC)
    {
        UnlockReleaseBuffer(mbuf);
        return;
    }
    if (meta->dir_keylen > 0 && meta->dir_count > 0)
    {
        so->dir_keylen = meta->dir_keylen;
        so->dir_count = meta->dir_count;
        so->dir_group = (meta->dir_group == 0) ? 1 : meta->dir_group;
        bytes = (Size) so->dir_count * so->dir_keylen;
        so->dir_first = (char *) palloc(bytes);
        so->dir_last  = (char *) palloc(bytes);
        mdir = ((char *) meta) + sizeof(SmolMetaPageData);
        memcpy(so->dir_first, mdir, bytes);
        memcpy(so->dir_last,  mdir + bytes, bytes);
    }
    UnlockReleaseBuffer(mbuf);
}

static void
smol_update_bounds(IndexScanDesc scan)
{
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation);
    Form_pg_attribute a0 = TupleDescAttr(tupdesc, 0);
    int16 attlen = a0->attlen;
    int i;

    so->has_lb = so->has_ub = false;
    so->lb_incl = so->ub_incl = false;
    so->lb_i64 = so->ub_i64 = 0;

    if (so->nkeys == 0)
        return;

    if (!(attlen == 2 || attlen == 4 || attlen == 8))
        return; /* only support pruning for fixed-width integers */

    for (i = 0; i < so->nkeys; i++)
    {
        ScanKey key = &so->scankeys[i];
        int64 v;
        if (key->sk_attno != 1)
            continue;
        if (attlen == 2)
            v = (int64) DatumGetInt16(key->sk_argument);
        else if (attlen == 4)
            v = (int64) DatumGetInt32(key->sk_argument);
        else
            v = (int64) DatumGetInt64(key->sk_argument);

        switch (key->sk_strategy)
        {
            case 1: /* < */
                if (!so->has_ub || v < so->ub_i64 || (v == so->ub_i64 && !so->ub_incl))
                {
                    so->has_ub = true;
                    so->ub_incl = false;
                    so->ub_i64 = v;
                }
                break;
            case 2: /* <= */
                if (!so->has_ub || v < so->ub_i64 || (v == so->ub_i64 && so->ub_incl == false))
                {
                    so->has_ub = true;
                    so->ub_incl = true;
                    so->ub_i64 = v;
                }
                break;
            case 3: /* = */
                so->has_lb = true; so->lb_incl = true; so->lb_i64 = v;
                so->has_ub = true; so->ub_incl = true; so->ub_i64 = v;
                break;
            case 4: /* >= */
                if (!so->has_lb || v > so->lb_i64 || (v == so->lb_i64 && so->lb_incl == false))
                {
                    so->has_lb = true;
                    so->lb_incl = true;
                    so->lb_i64 = v;
                }
                break;
            case 5: /* > */
                if (!so->has_lb || v > so->lb_i64 || (v == so->lb_i64 && so->lb_incl == true))
                {
                    so->has_lb = true;
                    so->lb_incl = false;
                    so->lb_i64 = v;
                }
                break;
            default:
                break;
        }
    }
}

static bool
smol_dir_get_group_range(SmolScanOpaque so, BlockNumber blk, int64 *out_min, int64 *out_max)
{
    if (so->dir_keylen <= 0 || so->dir_count == 0)
        return false;
    uint32 gidx = (uint32) ((blk - 1) / Max((uint16)1, so->dir_group));
    if (gidx >= so->dir_count)
        gidx = so->dir_count - 1;
    const char *fptr = so->dir_first + (size_t) gidx * so->dir_keylen;
    const char *lptr = so->dir_last  + (size_t) gidx * so->dir_keylen;
    int16 v16; int32 v32; int64 v64;
    switch (so->dir_keylen)
    {
        case 2:
            memcpy(&v16, fptr, 2); *out_min = (int64) v16;
            memcpy(&v16, lptr, 2); *out_max = (int64) v16;
            break;
        case 4:
            memcpy(&v32, fptr, 4); *out_min = (int64) v32;
            memcpy(&v32, lptr, 4); *out_max = (int64) v32;
            break;
        case 8:
            memcpy(&v64, fptr, 8); *out_min = v64;
            memcpy(&v64, lptr, 8); *out_max = v64;
            break;
        default:
            return false;
    }
    return true;
}
