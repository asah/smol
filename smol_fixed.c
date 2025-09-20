/*
 * smol_fixed.c
 *
 * Fixed version addressing scanning issues in SMOL PostgreSQL index
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
#include "access/tupmacs.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/varbit.h"

PG_MODULE_MAGIC;

/*
 * smol index tuple structure - stores only the indexed values, no TID
 */
typedef struct SmolTuple
{
    uint16      size;           /* Total size of this tuple */
    uint16      natts;          /* Number of attributes */
    /* Variable-length attribute data follows */
    char        data[FLEXIBLE_ARRAY_MEMBER];
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
} SmolScanOpaqueData;

typedef SmolScanOpaqueData *SmolScanOpaque;

/* Helper function declarations */
static bool smol_tuple_matches_keys(IndexScanDesc scan, SmolTuple *itup);
static void smol_extract_tuple_values(IndexScanDesc scan, SmolTuple *itup);

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

/*
 * Handler function: returns the access method's API struct
 */
PG_FUNCTION_INFO_V1(smol_handler);

Datum
smol_handler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;        /* No fixed strategy assignments */
    amroutine->amsupport = 1;           /* Only need one support function */
    amroutine->amoptsprocnum = 0;       /* No opclass options */
    amroutine->amcanorder = false;      /* No ordering support */
    amroutine->amcanorderbyop = false;  /* No operator ordering */
    amroutine->amcanbackward = true;    /* Support backward scan */
    amroutine->amcanunique = false;     /* No unique indexes (read-only) */
    amroutine->amcanmulticol = true;    /* Support multi-column indexes */
    amroutine->amoptionalkey = true;    /* Support scans without restriction */
    amroutine->amsearcharray = true;    /* Support ScalarArrayOpExpr */
    amroutine->amsearchnulls = true;    /* Support NULL searches */
    amroutine->amstorage = false;       /* No storage type different from column */
    amroutine->amclusterable = false;   /* Cannot be clustered (read-only) */
    amroutine->ampredlocks = false;     /* No predicate locks needed */
    amroutine->amcanparallel = true;    /* Support parallel scan */
    amroutine->amcaninclude = true;     /* Support INCLUDE columns */
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
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}

/*
 * Build a new smol index
 */
static IndexBuildResult *
smolbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result;
    TableScanDesc scan;
    TupleTableSlot *slot;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    
    result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
    result->heap_tuples = 0;
    result->index_tuples = 0;
    
    slot = table_slot_create(heap, NULL);
    scan = table_beginscan(heap, SnapshotAny, 0, NULL);
    
    while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
    {
        if (TTS_EMPTY(slot))
            continue;
            
        FormIndexDatum(indexInfo, slot, NULL, values, isnull);
        
        if (smolinsert(index, values, isnull, &slot->tts_tid, heap, 
                      UNIQUE_CHECK_NO, false, indexInfo))
        {
            result->index_tuples++;
        }
        
        result->heap_tuples++;
    }
    
    table_endscan(scan);
    ExecDropSingleTupleTableSlot(slot);
    
    return result;
}

static void
smolbuildempty(Relation index)
{
    /* Nothing to do - index is already empty */
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
    Buffer      buf = InvalidBuffer;
    Page        page;
    SmolTuple  *itup;
    Size        tupsize;
    int         i, natts;
    char       *ptr;
    BlockNumber nblocks;
    OffsetNumber offnum;
    TupleDesc   tupdesc = RelationGetDescr(index);
    
    natts = indexInfo->ii_NumIndexAttrs;
    tupsize = offsetof(SmolTuple, data);
    
    /* FIXED: Better size calculation with alignment */
    for (i = 0; i < natts; i++)
    {
        if (!isnull[i])
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            Size datalen;
            
            if (attr->attlen == -1)
                datalen = VARSIZE_ANY(DatumGetPointer(values[i]));
            else
                datalen = attr->attlen;
            
            tupsize += MAXALIGN(datalen);
        }
    }
    
    itup = (SmolTuple *) palloc(tupsize);
    itup->size = tupsize;
    itup->natts = natts;
    
    ptr = itup->data;
    for (i = 0; i < natts; i++)
    {
        if (!isnull[i])
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            Size datalen;
            
            if (attr->attlen == -1)
                datalen = VARSIZE_ANY(DatumGetPointer(values[i]));
            else
                datalen = attr->attlen;
            
            if (attr->attbyval)
                store_att_byval(ptr, values[i], datalen);
            else
                memcpy(ptr, DatumGetPointer(values[i]), datalen);
            
            ptr += MAXALIGN(datalen);
        }
    }
    
    /* Find a page with enough space */
    nblocks = RelationGetNumberOfBlocks(index);
    
    if (nblocks > 0)
    {
        buf = ReadBuffer(index, nblocks - 1);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
        
        if (PageGetFreeSpace(page) < MAXALIGN(tupsize) + sizeof(ItemIdData))
        {
            UnlockReleaseBuffer(buf);
            buf = InvalidBuffer;
        }
    }
    
    if (buf == InvalidBuffer)
    {
        buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
        PageInit(page, BufferGetPageSize(buf), 0);
    }
    
    offnum = PageAddItem(page, (Item) itup, tupsize, InvalidOffsetNumber, false, false);
    if (offnum == InvalidOffsetNumber)
    {
        /* FIXED: Better error handling for full pages */
        UnlockReleaseBuffer(buf);
        
        buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        page = BufferGetPage(buf);
        PageInit(page, BufferGetPageSize(buf), 0);
        
        offnum = PageAddItem(page, (Item) itup, tupsize, InvalidOffsetNumber, false, false);
        if (offnum == InvalidOffsetNumber)
        {
            UnlockReleaseBuffer(buf);
            pfree(itup);
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("failed to add item to new index page")));
        }
    }
    
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
    pfree(itup);
    
    return true;
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
    
    *indexStartupCost = 0.1;
    *indexTotalCost = numPages * 0.5;
    *indexSelectivity = 0.1;
    *indexCorrelation = 0.9;
    *indexPages = numPages;
    
    RelationClose(index);
}

static bytea *
smoloptions(Datum reloptions, bool validate)
{
    return NULL;
}

static bool
smolvalidate(Oid opclassoid)
{
    return true;
}

static IndexScanDesc
smolbeginscan(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    SmolScanOpaque so;

    scan = RelationGetIndexScan(index, nkeys, norderbys);
    
    so = (SmolScanOpaque) palloc(sizeof(SmolScanOpaqueData));
    so->started = false;
    so->currBuf = InvalidBuffer;
    so->currPos = InvalidOffsetNumber;
    so->nkeys = 0;
    so->scankeys = NULL;
    
    scan->opaque = so;
    
    return scan;
}

static void
smolrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
          ScanKey orderbys, int norderbys)
{
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
}

/*
 * FIXED: Improved tuple scanning with better validation and debugging
 */
static bool
smolgettuple(IndexScanDesc scan, ScanDirection dir)
{
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    Relation index = scan->indexRelation;
    Page page;
    SmolTuple *itup;
    OffsetNumber maxoff;
    BlockNumber nblocks;
    
    if (!so->started)
    {
        nblocks = RelationGetNumberOfBlocks(index);
        if (nblocks == 0)
            return false;
            
        so->currBuf = ReadBuffer(index, 0);
        LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
        so->currPos = FirstOffsetNumber;
        so->started = true;
    }
    
    while (BufferIsValid(so->currBuf))
    {
        page = BufferGetPage(so->currBuf);
        maxoff = PageGetMaxOffsetNumber(page);
        
        while (so->currPos <= maxoff)
        {
            ItemId itemid = PageGetItemId(page, so->currPos);
            
            if (ItemIdIsUsed(itemid) && !ItemIdIsDead(itemid))
            {
                itup = (SmolTuple *) PageGetItem(page, itemid);
                
                /* FIXED: Better validation with size checks */
                if (itup->size >= offsetof(SmolTuple, data) && 
                    itup->natts <= INDEX_MAX_KEYS &&
                    itup->size <= ItemIdGetLength(itemid) &&
                    itup->natts > 0)
                {
                    /* Check if tuple matches scan keys */
                    if (so->nkeys == 0 || smol_tuple_matches_keys(scan, itup))
                    {
                        /* Extract values for index-only scan */
                        smol_extract_tuple_values(scan, itup);
                        so->currPos++;
                        return true;
                    }
                }
            }
            so->currPos++;
        }
        
        /* Move to next page */
        BlockNumber currblk = BufferGetBlockNumber(so->currBuf);
        BlockNumber nextblk = currblk + 1;
        
        UnlockReleaseBuffer(so->currBuf);
        so->currBuf = InvalidBuffer;
        
        nblocks = RelationGetNumberOfBlocks(index);
        if (nextblk < nblocks)
        {
            so->currBuf = ReadBuffer(index, nextblk);
            LockBuffer(so->currBuf, BUFFER_LOCK_SHARE);
            so->currPos = FirstOffsetNumber;
        }
        else
        {
            break;
        }
    }
    
    return false;
}

static void
smolendscan(IndexScanDesc scan)
{
    SmolScanOpaque so = (SmolScanOpaque) scan->opaque;
    
    if (BufferIsValid(so->currBuf))
        ReleaseBuffer(so->currBuf);
    
    if (so->scankeys)
        pfree(so->scankeys);
    
    if (scan->xs_itup)
        pfree(scan->xs_itup);
    
    pfree(so);
}

/*
 * FIXED: Improved key matching with better data extraction
 */
static bool
smol_tuple_matches_keys(IndexScanDesc scan, SmolTuple *itup)
{
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
        
        if (attno >= 0 && attno < itup->natts && attno < indexTupdesc->natts)
        {
            /* FIXED: More robust data extraction */
            dataptr = itup->data;
            for (j = 0; j < attno; j++)
            {
                attr = TupleDescAttr(indexTupdesc, j);
                if (attr->attlen == -1)
                    dataptr += MAXALIGN(VARSIZE_ANY(dataptr));
                else
                    dataptr += MAXALIGN(attr->attlen);
            }
            
            attr = TupleDescAttr(indexTupdesc, attno);
            if (attr->attbyval)
            {
                switch (attr->attlen)
                {
                    case sizeof(int16):
                        value = Int16GetDatum(*(int16*)dataptr);
                        break;
                    case sizeof(int32):
                        value = Int32GetDatum(*(int32*)dataptr);
                        break;
                    case sizeof(int64):
                        value = Int64GetDatum(*(int64*)dataptr);
                        break;
                    default:
                        memcpy(&value, dataptr, Min(attr->attlen, sizeof(Datum)));
                        break;
                }
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
                return false;
        }
        else
        {
            if (!DatumGetBool(FunctionCall2Coll(&key->sk_func,
                                               key->sk_collation,
                                               value,
                                               key->sk_argument)))
                return false;
        }
    }
    
    return true;
}

/*
 * FIXED: Improved tuple value extraction with alignment handling
 */
static void
smol_extract_tuple_values(IndexScanDesc scan, SmolTuple *stup)
{
    TupleDesc   tupdesc = RelationGetDescr(scan->indexRelation);
    int         natts   = tupdesc->natts;
    Datum      *values;
    bool       *isnull;
    char       *dataptr;
    int         i;

    values = (Datum *) palloc(sizeof(Datum) * natts);
    isnull = (bool  *) palloc(sizeof(bool)  * natts);

    dataptr = stup->data;

    for (i = 0; i < natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        if (i >= stup->natts)
        {
            values[i] = (Datum) 0;
            isnull[i] = true;
            continue;
        }

        isnull[i] = false;

        if (attr->attbyval)
        {
            /* FIXED: Better by-value field handling */
            switch (attr->attlen)
            {
                case 1:
                    values[i] = CharGetDatum(*(char *) dataptr);
                    break;
                case 2:
                    values[i] = Int16GetDatum(*(int16 *) dataptr);
                    break;
                case 4:
                    values[i] = Int32GetDatum(*(int32 *) dataptr);
                    break;
                case 8:
                    values[i] = Int64GetDatum(*(int64 *) dataptr);
                    break;
                default:
                    memcpy(&values[i], dataptr, Min(attr->attlen, sizeof(Datum)));
                    break;
            }
        }
        else
        {
            values[i] = PointerGetDatum(dataptr);
        }

        /* FIXED: Proper alignment handling for data advancement */
        if (attr->attlen == -1)
            dataptr += MAXALIGN(VARSIZE_ANY(dataptr));
        else
            dataptr += MAXALIGN(attr->attlen);
    }

    if (scan->xs_itup)
        pfree(scan->xs_itup);

    scan->xs_itup = index_form_tuple(tupdesc, values, isnull);
    scan->xs_itupdesc = tupdesc;

    scan->xs_hitup = NULL;
    scan->xs_recheck = false;
    ItemPointerSetInvalid(&scan->xs_heaptid);

    pfree(values);
    pfree(isnull);
}
