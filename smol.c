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
#include <stdint.h>
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
/* Planner cost GUCs */
static double smol_cost_page = 0.02;
static double smol_cost_tup = 0.002;
static double smol_selec_eq = 0.01;
static double smol_selec_range = 0.10;
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

    DefineCustomRealVariable("smol.cost_page",
                             "Estimated per-page cost for SMOL IOS reads",
                             NULL,
                             &smol_cost_page,
                             0.02, 0.0, 1.0,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);

    DefineCustomRealVariable("smol.cost_tup",
                             "Estimated per-tuple CPU cost for SMOL IOS",
                             NULL,
                             &smol_cost_tup,
                             0.002, 0.0, 1.0,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);

    DefineCustomRealVariable("smol.selec_eq",
                             "Heuristic selectivity for equality on leading key",
                             NULL,
                             &smol_selec_eq,
                             0.01, 0.0, 1.0,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);

    DefineCustomRealVariable("smol.selec_range",
                             "Heuristic selectivity for range on leading key",
                             NULL,
                             &smol_selec_range,
                             0.10, 0.0, 1.0,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);
}

/* Global comparator context for 2-col row sorting (single-threaded build) */
static char *g_k1buf = NULL, *g_k2buf = NULL;
static uint16 g_key_len1 = 0, g_key_len2 = 0;
static bool g_byval1 = false, g_byval2 = false;
static FmgrInfo g_cmp1, g_cmp2;
static Oid g_coll1 = InvalidOid, g_coll2 = InvalidOid;
static int
smol_pair_qsort_cmp(const void *pa, const void *pb)
{
    uint32 ia = *(const uint32 *) pa, ib = *(const uint32 *) pb;
    char *a1 = g_k1buf + (size_t) ia * g_key_len1;
    char *b1 = g_k1buf + (size_t) ib * g_key_len1;
    Datum da1 = g_byval1 ? (g_key_len1==1?CharGetDatum(*a1): g_key_len1==2?Int16GetDatum(*(int16*)a1): g_key_len1==4?Int32GetDatum(*(int32*)a1): Int64GetDatum(*(int64*)a1)) : PointerGetDatum(a1);
    Datum db1 = g_byval1 ? (g_key_len1==1?CharGetDatum(*b1): g_key_len1==2?Int16GetDatum(*(int16*)b1): g_key_len1==4?Int32GetDatum(*(int32*)b1): Int64GetDatum(*(int64*)b1)) : PointerGetDatum(b1);
    int32 r1 = DatumGetInt32(FunctionCall2Coll(&g_cmp1, g_coll1, da1, db1)); if (r1 != 0) return r1;
    char *a2 = g_k2buf + (size_t) ia * g_key_len2;
    char *b2 = g_k2buf + (size_t) ib * g_key_len2;
    Datum da2 = g_byval2 ? (g_key_len2==1?CharGetDatum(*a2): g_key_len2==2?Int16GetDatum(*(int16*)a2): g_key_len2==4?Int32GetDatum(*(int32*)a2): Int64GetDatum(*(int64*)a2)) : PointerGetDatum(a2);
    Datum db2 = g_byval2 ? (g_key_len2==1?CharGetDatum(*b2): g_key_len2==2?Int16GetDatum(*(int16*)b2): g_key_len2==4?Int32GetDatum(*(int32*)b2): Int64GetDatum(*(int64*)b2)) : PointerGetDatum(b2);
    int32 r2 = DatumGetInt32(FunctionCall2Coll(&g_cmp2, g_coll2, da2, db2)); return r2;
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
    uint16      inc_count;  /* number of INCLUDE attrs (single-col path) */
    uint16      inc_len[16];/* lengths for INCLUDE attrs (fixed-width only) */
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
    Datum       bound_datum;    /* bound as Datum for comparator */
    FmgrInfo    cmp_fmgr;       /* comparator proc 1 */
    Oid         collation;      /* leading key collation */
    bool        key_byval;      /* byval property of leading key */
    int16       key_typlen;     /* typlen of leading key (1,2,4,8) */
    bool        have_k1_eq;     /* true when leading key equality present */
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
    /* fixed-size copy helpers to avoid per-row branches */
    void      (*copy1_fn)(char *dst, const char *src);
    void      (*copy2_fn)(char *dst, const char *src);
    /* INCLUDE metadata (single-col path) */
    uint16      ninclude;
    uint16      inc_len[16];
    char        inc_align[16];
    uint16      inc_offs[16];   /* offsets inside tuple data area (from data_off) */
    void      (*inc_copy[16])(char *dst, const char *src);

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
    uint32      leaf_cap;       /* capacity of leaf_k1/leaf_k2 arrays */
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
static void smol_build_tree1_inc_from_sorted(Relation idx, const int64 *keys, const char * const *incs,
                             Size nkeys, uint16 key_len, int inc_count, const uint16 *inc_lens);
/* removed: static void smol_build_tree2_from_sorted(...); */
static void smol_build_internal_levels(Relation idx,
                                       BlockNumber *child_blks, const int64 *child_high,
                                       Size nchildren, uint16 key_len,
                                       BlockNumber *out_root, uint16 *out_levels);
static BlockNumber smol_find_first_leaf(Relation idx, int64 lower_bound, Oid atttypid, uint16 key_len);
static BlockNumber smol_rightmost_leaf(Relation idx);
static BlockNumber smol_prev_leaf(Relation idx, BlockNumber cur);
static int smol_cmp_keyptr_bound(const char *keyp, uint16 key_len, Oid atttypid, int64 bound);
static int smol_cmp_keyptr_bound_generic(FmgrInfo *cmp, Oid collation, const char *keyp, uint16 key_len, bool key_byval, Datum bound);
static bytea *smol_options(Datum reloptions, bool validate);
static bool smol_validate(Oid opclassoid);
static uint16 smol_leaf_nitems(Page page);
static char *smol_leaf_keyptr(Page page, uint16 idx, uint16 key_len);
/* Two-column row-major helpers (generic fixed-length)
 * On-disk 2-key leaf payload layout:
 *   [uint16 nrows][row0: k1||k2][row1: k1||k2]...
 * k1/k2 are fixed-size slices; lengths come from SmolMeta.key_len1/2.
 * Leaf has exactly one ItemId holding the payload.
 */
static inline uint16 smol12_leaf_nrows(Page page)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 n; memcpy(&n, p, sizeof(uint16)); return n;
}
static inline char *smol12_row_ptr(Page page, uint16 row, uint16 key_len1, uint16 key_len2)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *base = (char *) PageGetItem(page, iid);
    size_t off = sizeof(uint16) + (size_t)(row - 1) * ((size_t) key_len1 + (size_t) key_len2);
    return base + off;
}
static inline char *smol12_row_k1_ptr(Page page, uint16 row, uint16 key_len1, uint16 key_len2)
{ return smol12_row_ptr(page, row, key_len1, key_len2) + 0; }
static inline char *smol12_row_k2_ptr(Page page, uint16 row, uint16 key_len1, uint16 key_len2)
{ return smol12_row_ptr(page, row, key_len1, key_len2) + key_len1; }
static inline void smol_copy2(char *dst, const char *src);
static inline void smol_copy4(char *dst, const char *src);
static inline void smol_copy8(char *dst, const char *src);
static inline void smol_copy16(char *dst, const char *src);
static inline void smol_copy_small(char *dst, const char *src, uint16 len);
/* forward decls for normalizers */
static inline uint64 smol_norm64(int64 v);
static inline uint32 smol_norm32(int32 v);
static inline uint16 smol_norm16(int16 v);
/* single-col + INCLUDE helpers */
static inline char *smol1_payload(Page page) { ItemId iid = PageGetItemId(page, FirstOffsetNumber); return (char *) PageGetItem(page, iid); }
static inline char *smol1_inc_ptr(Page page, uint16 key_len, uint16 n, const uint16 *inc_lens, uint16 ninc, uint16 inc_idx, uint32 row)
{
    char *base = smol1_payload(page) + sizeof(uint16) + (size_t) n * key_len;
    for (uint16 i = 0; i < inc_idx; i++) base += (size_t) n * inc_lens[i];
    return base + (size_t) row * inc_lens[inc_idx];
}

/* Page summary (diagnostic) */
static void smol_log_page_summary(Relation idx);

/* Sorting helpers for build path */
static void smol_sort_pairs_rows64(int64 *k1, int64 *k2, Size n);
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
/* tuplesort-based single-col contexts */
typedef struct TsBuildCtxI16 { Tuplesortstate *ts; Size *pnkeys; } TsBuildCtxI16;
typedef struct TsBuildCtxI32 { Tuplesortstate *ts; Size *pnkeys; } TsBuildCtxI32;
typedef struct TsBuildCtxI64 { Tuplesortstate *ts; Size *pnkeys; } TsBuildCtxI64;
static void ts_build_cb_i16(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void ts_build_cb_i32(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
/* int8 tuplesort callback not used in current build path */
/* generic tuplesort collector for arbitrary fixed-length types */
typedef struct TsBuildCtxAny { Tuplesortstate *ts; Size *pnkeys; } TsBuildCtxAny;
static void ts_build_cb_any(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
/* 2-col generic builders */
typedef struct { char **pk1; char **pk2; Size *pcap; Size *pcount; uint16 len1; uint16 len2; bool byval1; bool byval2; } PairArrCtx;
static void smol_build_cb_pair(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
/* Single-key + INCLUDE collection (generic fixed-length include attrs) */
typedef struct CollectIncCtx { int64 **pk; char **pi[16]; uint16 ilen[16]; bool ibyval[16]; Size *pcap; Size *pcount; int incn; } CollectIncCtx;
static void smol_build_cb_inc(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);

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
    am->amcaninclude = true;
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
    Size nkeys = 0;
    Oid atttypid;
    Oid atttypid2 = InvalidOid;
    uint16 key_len, key_len2 = 0;
    int nkeyatts = index->rd_index->indnkeyatts;
    int natts = RelationGetDescr(index)->natts;
    int ninclude = natts - nkeyatts;
    SMOL_LOGF("build start rel=%u idx=%u", RelationGetRelid(heap), RelationGetRelid(index));
    /* Phase timers */
    instr_time t_start, t_collect_end, t_sort_end, t_write_end;
    INSTR_TIME_SET_CURRENT(t_start);
    INSTR_TIME_SET_CURRENT(t_collect_end);
    INSTR_TIME_SET_CURRENT(t_sort_end);
    INSTR_TIME_SET_CURRENT(t_write_end);

    /* Enforce 1 or 2 key columns (fixed-width types only) */
    if (nkeyatts != 1 && nkeyatts != 2)
        ereport(ERROR, (errmsg("smol prototype supports 1 or 2 key columns only")));
    atttypid = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
    {
        int16 typlen; bool byval; char align;
        get_typlenbyvalalign(atttypid, &typlen, &byval, &align);
        if (typlen <= 0)
            ereport(ERROR, (errmsg("smol supports fixed-length key types only (attno=1)")));
        key_len = (uint16) typlen;
    }
    if (nkeyatts == 2)
    {
        atttypid2 = TupleDescAttr(RelationGetDescr(index), 1)->atttypid;
        {
            int16 typlen; bool byval; char align;
            get_typlenbyvalalign(atttypid2, &typlen, &byval, &align);
            if (typlen <= 0)
                ereport(ERROR, (errmsg("smol supports fixed-length key types only (attno=2)")));
            key_len2 = (uint16) typlen;
        }
        if (ninclude > 0)
            ereport(ERROR, (errmsg("smol INCLUDE columns currently supported only for single-key indexes")));
    }
    if (ninclude < 0)
        ereport(ERROR, (errmsg("invalid include count")));

    if (nkeyatts == 1 && ninclude > 0)
    {
        /* Single-key with INCLUDE fixed-width ints */
        int inc_count = ninclude;
        uint16 inc_lens[16]; bool inc_byval[16];
        if (inc_count > 16)
            ereport(ERROR, (errmsg("smol supports up to 16 INCLUDE columns")));
        for (int i = 0; i < inc_count; i++)
        {
            Oid t = TupleDescAttr(RelationGetDescr(index), nkeyatts + i)->atttypid;
            int16 typlen; bool byval; char align;
            get_typlenbyvalalign(t, &typlen, &byval, &align);
            if (typlen <= 0)
                ereport(ERROR, (errmsg("smol INCLUDE supports fixed-length types only (attno=%d)", nkeyatts + i + 1)));
            inc_lens[i] = (uint16) typlen; inc_byval[i] = byval;
        }
        /* Collect keys + includes into arrays */
        Size cap = 0, n = 0;
        int64 *karr = NULL;
        char *incarr[16]; memset(incarr, 0, sizeof(incarr));
        CollectIncCtx cctx; cctx.pk = &karr; for (int i=0;i<inc_count;i++){ cctx.pi[i] = &incarr[i]; cctx.ilen[i] = inc_lens[i]; cctx.ibyval[i] = inc_byval[i]; } cctx.pcap=&cap; cctx.pcount=&n; cctx.incn=inc_count;
        table_index_build_scan(heap, index, indexInfo, true, true, smol_build_cb_inc, (void *) &cctx, NULL);
        INSTR_TIME_SET_CURRENT(t_collect_end);
        SMOL_LOGF("build: collected rows=%zu (key+%d includes)", (size_t) n, inc_count);
        /* Build permutation via radix sort */
        if (n > 0)
        {
            uint64 *norm = (uint64 *) palloc(n * sizeof(uint64));
            uint32 *idx = (uint32 *) palloc(n * sizeof(uint32));
            uint32 *tmp = (uint32 *) palloc(n * sizeof(uint32));
            for (Size i = 0; i < n; i++) { norm[i] = smol_norm64(karr[i]); idx[i] = (uint32) i; }
            smol_radix_sort_idx_u64(norm, idx, tmp, n);
            pfree(norm); pfree(tmp);
            /* Apply permutation into new arrays */
            int64 *sk = (int64 *) palloc(n * sizeof(int64));
            char *sinc[16]; for (int i=0;i<inc_count;i++) sinc[i] = (char *) palloc(((Size) n) * inc_lens[i]);
            for (Size i = 0; i < n; i++)
            {
                uint32 j = idx[i]; sk[i] = karr[j];
                for (int c = 0; c < inc_count; c++)
                {
                    memcpy(sinc[c] + ((size_t) i * inc_lens[c]), incarr[c] + ((size_t) j * inc_lens[c]), inc_lens[c]);
                }
            }
            pfree(idx);
            SMOL_LOGF("build phase: write start n=%zu (includes=%d)", (size_t) n, inc_count);
            /* Write tree and meta with include info */
            /* Reuse single-column writer with include support */
            /* Implement below: smol_build_tree1_inc_from_sorted */
            smol_build_tree1_inc_from_sorted(index, sk, (const char * const *) sinc, n, key_len, inc_count, inc_lens);
            for (int i=0;i<inc_count;i++) pfree(sinc[i]);
            pfree(sk);
        }
        for (int i=0;i<inc_count;i++) if (incarr[i]) pfree(incarr[i]);
        if (karr) pfree(karr);
        INSTR_TIME_SET_CURRENT(t_write_end);
    }
    else if (nkeyatts == 1 && atttypid == INT2OID)
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
    else if (nkeyatts == 1)
    {
        /* Generic fixed-length single-key path (non-varlena) */
        int16 typlen; bool byval; char typalign; Oid coll;
        TypeCacheEntry *tce; Oid ltOp; Tuplesortstate *ts; bool isnull; Datum val; Size i = 0;
        get_typlenbyvalalign(atttypid, &typlen, &byval, &typalign);
        if (typlen <= 0)
            ereport(ERROR, (errmsg("smol supports fixed-length types only (typlen=%d)", (int) typlen)));
        key_len = (uint16) typlen;
        coll = TupleDescAttr(RelationGetDescr(index), 0)->attcollation;
        tce = lookup_type_cache(atttypid, TYPECACHE_LT_OPR);
        if (!OidIsValid(tce->lt_opr)) ereport(ERROR, (errmsg("no < operator for type %u", atttypid)));
        ltOp = tce->lt_opr;
        ts = tuplesort_begin_datum(atttypid, ltOp, coll, false, maintenance_work_mem, NULL, false);
        TsBuildCtxAny gcb; gcb.ts = ts; gcb.pnkeys = &nkeys;
        table_index_build_scan(heap, index, indexInfo, true, true, ts_build_cb_any, (void *) &gcb, NULL);
        INSTR_TIME_SET_CURRENT(t_collect_end);
        tuplesort_performsort(ts);
        INSTR_TIME_SET_CURRENT(t_sort_end);
        /* materialize into contiguous bytes */
        {
            char *out = (char *) palloc(((Size) nkeys) * key_len);
            while (tuplesort_getdatum(ts, true, false, &val, &isnull, NULL))
            {
                char *dst = out + ((size_t) i * key_len);
                if (byval)
                {
                    switch (key_len)
                    {
                        case 1: { char v = DatumGetChar(val); memcpy(dst, &v, 1); break; }
                        case 2: { int16 v = DatumGetInt16(val); memcpy(dst, &v, 2); break; }
                        case 4: { int32 v = DatumGetInt32(val); memcpy(dst, &v, 4); break; }
                        case 8: { int64 v = DatumGetInt64(val); memcpy(dst, &v, 8); break; }
                        default: ereport(ERROR, (errmsg("unexpected byval typlen=%u", (unsigned) key_len)));
                    }
                }
                else
                {
                    memcpy(dst, DatumGetPointer(val), key_len);
                }
                i++;
            }
            smol_build_tree_from_sorted(index, (const void *) out, nkeys, key_len);
            pfree(out);
        }
        tuplesort_end(ts);
        INSTR_TIME_SET_CURRENT(t_write_end);
    }
    else /* 2-column: collect generic fixed-length pairs and write row-major */
    {
        Size cap = 0, n = 0;
        char *k1buf = NULL, *k2buf = NULL;
        PairArrCtx cctx = { &k1buf, &k2buf, &cap, &n, key_len, key_len2, false, false };
        {
            int16 l; bool bv; char al;
            get_typlenbyvalalign(atttypid, &l, &bv, &al); cctx.byval1 = bv;
            get_typlenbyvalalign(atttypid2, &l, &bv, &al); cctx.byval2 = bv;
        }
        table_index_build_scan(heap, index, indexInfo, true, true, smol_build_cb_pair, (void *) &cctx, NULL);
        INSTR_TIME_SET_CURRENT(t_collect_end);
        if (n > 0)
        {
            FmgrInfo cmp1, cmp2; Oid coll1 = TupleDescAttr(RelationGetDescr(index), 0)->attcollation; Oid coll2 = TupleDescAttr(RelationGetDescr(index), 1)->attcollation;
            fmgr_info_copy(&cmp1, index_getprocinfo(index, 1, 1), CurrentMemoryContext);
            fmgr_info_copy(&cmp2, index_getprocinfo(index, 2, 1), CurrentMemoryContext);
            uint32 *idx = (uint32 *) palloc(n * sizeof(uint32)); for (Size i=0;i<n;i++) idx[i] = (uint32) i;
            /* set global comparator context */
            g_k1buf = k1buf; g_k2buf = k2buf; g_key_len1 = key_len; g_key_len2 = key_len2; g_byval1 = cctx.byval1; g_byval2 = cctx.byval2; g_coll1 = coll1; g_coll2 = coll2; memcpy(&g_cmp1, &cmp1, sizeof(FmgrInfo)); memcpy(&g_cmp2, &cmp2, sizeof(FmgrInfo));
            qsort(idx, n, sizeof(uint32), smol_pair_qsort_cmp);
            /* init meta if new */
            if (RelationGetNumberOfBlocks(index) == 0)
            {
                Buffer mb = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
                LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE); Page pg = BufferGetPage(mb); PageInit(pg, BLCKSZ, 0);
                SmolMeta *m = smol_meta_ptr(pg); m->magic=SMOL_META_MAGIC; m->version=SMOL_META_VERSION; m->nkeyatts=2; m->key_len1=key_len; m->key_len2=key_len2; m->root_blkno=InvalidBlockNumber; m->height=0; MarkBufferDirty(mb); UnlockReleaseBuffer(mb);
            }
            /* Write leaves in generic row-major 2-key layout:
             * payload: [uint16 nrows][row0: k1||k2][row1: k1||k2]...
             * One ItemId (FirstOffsetNumber) per leaf.
             */
            Size i = 0; BlockNumber prev = InvalidBlockNumber; char *scratch = (char *) palloc(BLCKSZ);
            while (i < n)
            {
                Buffer buf = smol_extend(index); Page page = BufferGetPage(buf); smol_init_page(buf, true, InvalidBlockNumber);
                Size fs = PageGetFreeSpace(page); Size avail = (fs > sizeof(ItemIdData)) ? (fs - sizeof(ItemIdData)) : 0;
                Size header = sizeof(uint16); Size perrow = (Size) key_len + (Size) key_len2;
                Size maxn = (avail > header) ? ((avail - header) / perrow) : 0; Size rem = n - i; Size n_this = (rem < maxn) ? rem : maxn; if (n_this == 0) ereport(ERROR,(errmsg("smol: two-col row too large for page")));
                memcpy(scratch, &n_this, sizeof(uint16)); char *p = scratch + sizeof(uint16);
                for (Size j=0;j<n_this;j++) { uint32 id = idx[i+j]; memcpy(p, k1buf + (size_t) id * key_len, key_len); p += key_len; memcpy(p, k2buf + (size_t) id * key_len2, key_len2); p += key_len2; }
                Size sz = (Size) (p - scratch);
                if (PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false) == InvalidOffsetNumber)
                    ereport(ERROR,(errmsg("smol: failed to add two-col row payload")));
                MarkBufferDirty(buf); BlockNumber cur = BufferGetBlockNumber(buf); UnlockReleaseBuffer(buf);
                if (BlockNumberIsValid(prev)) { Buffer pb = ReadBuffer(index, prev); LockBuffer(pb, BUFFER_LOCK_EXCLUSIVE); Page pp=BufferGetPage(pb); smol_page_opaque(pp)->rightlink=cur; MarkBufferDirty(pb); UnlockReleaseBuffer(pb);} prev = cur; i += n_this;
            }
            Buffer mb = ReadBuffer(index, 0); LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE); Page pg = BufferGetPage(mb); SmolMeta *m = smol_meta_ptr(pg); m->root_blkno = 1; m->height = 1; MarkBufferDirty(mb); UnlockReleaseBuffer(mb);
            pfree(idx);
            pfree(scratch);
        }
        if (k1buf)
            pfree(k1buf);
        if (k2buf)
            pfree(k2buf);
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
    so->have_k1_eq = false;
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
        /* read meta to get INCLUDE lengths (single-key include only)
         * and fetch per-attribute alignment from the index tupledesc
         * so that our prebuilt tuple layout matches index_getattr logic.
         */
        {
            SmolMeta m; smol_meta_read(index, &m);
            so->ninclude = (so->two_col ? 0 : m.inc_count);
            for (uint16 i=0;i<so->ninclude;i++)
            {
                so->inc_len[i] = m.inc_len[i];
                /* include attrs follow key attrs in the index tupdesc */
                Form_pg_attribute att = TupleDescAttr(RelationGetDescr(index), 1 /* key attr */ + i);
                so->inc_align[i] = att->attalign;
            }
        }
        so->align1 = align1;
        so->align2 = align2;
        if (!so->two_col)
        {
            /* Layout: key followed by INCLUDE attrs, each aligned per attalign */
            sz = off1 + so->key_len;
            Size cur = sz;
            for (uint16 i=0;i<so->ninclude;i++)
            {
                cur = att_align_nominal(cur, so->inc_align[i]);
                so->inc_offs[i] = (uint16) (cur - data_off);
                cur += so->inc_len[i];
            }
            sz = cur;
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
        /* set copy helpers */
        so->copy1_fn = (so->key_len == 2) ? smol_copy2 : (so->key_len == 4) ? smol_copy4 : smol_copy8;
        if (so->two_col)
            so->copy2_fn = (so->key_len2 == 2) ? smol_copy2 : (so->key_len2 == 4) ? smol_copy4 : smol_copy8;
        for (uint16 i=0;i<so->ninclude;i++)
            so->inc_copy[i] = (so->inc_len[i] == 2) ? smol_copy2 : (so->inc_len[i] == 4) ? smol_copy4 : smol_copy8;
        /* comparator + key type props */
        so->collation = TupleDescAttr(RelationGetDescr(index), 0)->attcollation;
        get_typlenbyvalalign(so->atttypid, &so->key_typlen, &so->key_byval, &so->align1);
        fmgr_info_copy(&so->cmp_fmgr, index_getprocinfo(index, 1, 1), CurrentMemoryContext);
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
    so->have_k1_eq = false;
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
                    so->have_k1_eq = (sk->sk_strategy == BTEqualStrategyNumber);
                    so->bound_datum = sk->sk_argument;
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
        /* Defensive: initialize bounds from scan->keyData if rescan not called yet */
        if (!so->have_bound && scan->numberOfKeys > 0 && scan->keyData)
            smol_rescan(scan, scan->keyData, scan->numberOfKeys, scan->orderByData, scan->numberOfOrderBys);
        /* no local variables needed here */
        if (dir == BackwardScanDirection)
        {
            if (so->have_bound)
                so->cur_blk = smol_rightmost_leaf(idx); /* start at rightmost, then step back within leaf by bound */
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
                    if (smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, keyp, so->key_len, so->key_byval, so->bound_datum) <= 0)
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
                            /* Use actual lower bound when available to avoid over-emitting from the first leaf */
                            int64 lb = PG_INT64_MIN;
                            if (so->have_bound)
                            {
                                if (so->atttypid == INT2OID) lb = (int64) DatumGetInt16(so->bound_datum);
                                else if (so->atttypid == INT4OID) lb = (int64) DatumGetInt32(so->bound_datum);
                                else if (so->atttypid == INT8OID) lb = DatumGetInt64(so->bound_datum);
                            }
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
                        /* Seek within first claimed leaf to the bound (forward scans) */
                        if (so->have_bound && dir != BackwardScanDirection && !so->two_col)
                        {
                            uint16 n2 = smol_leaf_nitems(page);
                            uint16 lo2 = FirstOffsetNumber, hi2 = n2, ans2 = InvalidOffsetNumber;
                            while (lo2 <= hi2)
                            {
                                uint16 mid2 = (uint16) (lo2 + ((hi2 - lo2) >> 1));
                                char *kp2 = smol_leaf_keyptr(page, mid2, so->key_len);
                                int cc = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, kp2, so->key_len, so->key_byval, so->bound_datum);
                                if (so->prof_enabled) so->prof_bsteps++;
                                if ((so->bound_strict ? (cc > 0) : (cc >= 0))) { ans2 = mid2; if (mid2 == 0) break; hi2 = (uint16) (mid2 - 1); }
                                else lo2 = (uint16) (mid2 + 1);
                            }
                            so->cur_off = (ans2 != InvalidOffsetNumber) ? ans2 : (uint16) (n2 + 1);
                        }
                    }
                }
                else
                {
                    so->cur_blk = smol_find_first_leaf(idx, 0 /*unused*/ , so->atttypid, so->key_len);
                    so->cur_off = FirstOffsetNumber;
                    so->initialized = true;
                    SMOL_LOGF("gettuple init cur_blk=%u", so->cur_blk);

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
                            int c = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, keyp, so->key_len, so->key_byval, so->bound_datum);
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
                            int64 lb;
                            if (so->have_bound)
                            {
                                if (so->atttypid == INT2OID) lb = (int64) DatumGetInt16(so->bound_datum);
                                else if (so->atttypid == INT4OID) lb = (int64) DatumGetInt32(so->bound_datum);
                                else if (so->atttypid == INT8OID) lb = DatumGetInt64(so->bound_datum);
                                else lb = PG_INT64_MIN;
                            }
                            else lb = PG_INT64_MIN;
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
                        so->leaf_n = smol12_leaf_nrows(page); so->leaf_i = 0;
                        if (so->have_bound)
                        {
                            uint16 lo = FirstOffsetNumber, hi = so->leaf_n, ans = InvalidOffsetNumber;
                            while (lo <= hi)
                            {
                                uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                                char *k1p = smol12_row_k1_ptr(page, mid, so->key_len, so->key_len2);
                                int c = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, k1p, so->key_len, so->key_byval, so->bound_datum);
                                if (so->prof_enabled) so->prof_bsteps++;
                                if ((so->bound_strict ? (c > 0) : (c >= 0))) { ans = mid; if (mid == 0) break; hi = (uint16) (mid - 1); }
                                else lo = (uint16) (mid + 1);
                            }
                            so->leaf_i = (ans != InvalidOffsetNumber) ? (uint32) (ans - 1) : (uint32) so->leaf_n;
                        }
                    }
                }
                else
                {
                    so->cur_blk = smol_find_first_leaf(idx, 0, so->atttypid, so->key_len);
                    so->cur_group = 0;
                    so->pos_in_group = 0;
                    so->initialized = true;
                    if (so->have_bound)
                    {
                        /* Pin leaf and binary-search rows on k1 (>= or > bound) */
                        uint16 lo = FirstOffsetNumber, hi, ans = InvalidOffsetNumber;
                        buf = ReadBuffer(idx, so->cur_blk);
                        page = BufferGetPage(buf);
                        so->leaf_n = smol12_leaf_nrows(page);
                        hi = so->leaf_n;
                        while (lo <= hi)
                        {
                            uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                            char *k1p = smol12_row_k1_ptr(page, mid, so->key_len, so->key_len2);
                            int c = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, k1p, so->key_len, so->key_byval, so->bound_datum);
                            if (so->prof_enabled) so->prof_bsteps++;
                            if ((so->bound_strict ? (c > 0) : (c >= 0))) { ans = mid; if (mid == 0) break; hi = (uint16) (mid - 1); }
                            else lo = (uint16) (mid + 1);
                        }
                        so->cur_buf = buf; so->have_pin = true;
                        so->leaf_i = (ans != InvalidOffsetNumber) ? (uint32) (ans - 1) : (uint32) so->leaf_n;
                    }
                    else
                    {
                        buf = ReadBuffer(idx, so->cur_blk);
                        page = BufferGetPage(buf);
                        so->cur_buf = buf; so->have_pin = true;
                        so->leaf_n = smol12_leaf_nrows(page); so->leaf_i = 0;
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
                uint16 row = (uint16) (so->leaf_i + 1);
                char *k1p = smol12_row_k1_ptr(page, row, so->key_len, so->key_len2);
                char *k2p = smol12_row_k2_ptr(page, row, so->key_len, so->key_len2);
                /* Enforce leading-key bound per row for correctness */
                if (so->have_bound)
                {
                    int c = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, k1p, so->key_len, so->key_byval, so->bound_datum);
                    if (so->bound_strict ? (c <= 0) : (c < 0))
                    { so->leaf_i++; continue; }
                    if (so->have_k1_eq && c > 0)
                    { /* passed equality run: force advance to next leaf */ so->leaf_i = so->leaf_n; continue; }
                }
                /* Optional equality on second key (int2/int4/int8 only) */
                if (so->have_k2_eq)
                {
                    int64 v;
                    if (so->key_len2 == 2) { int16 t; memcpy(&t, k2p, 2); v = (int64) t; }
                    else if (so->key_len2 == 4) { int32 t; memcpy(&t, k2p, 4); v = (int64) t; }
                    else /* 8 */ { int64 t; memcpy(&t, k2p, 8); v = t; }
                    if (v != so->k2_eq)
                    { so->leaf_i++; continue; }
                }
                /* Optimized copies for common fixed lengths; fallback for others */
                if (so->key_len == 2) so->copy1_fn(so->itup_data, k1p);
                else if (so->key_len == 4) so->copy1_fn(so->itup_data, k1p);
                else if (so->key_len == 8) so->copy1_fn(so->itup_data, k1p);
                else if (so->key_len == 16) smol_copy16(so->itup_data, k1p);
                else smol_copy_small(so->itup_data, k1p, so->key_len);

                if (so->key_len2 == 2) so->copy2_fn(so->itup_data + so->itup_off2, k2p);
                else if (so->key_len2 == 4) so->copy2_fn(so->itup_data + so->itup_off2, k2p);
                else if (so->key_len2 == 8) so->copy2_fn(so->itup_data + so->itup_off2, k2p);
                else if (so->key_len2 == 16) smol_copy16(so->itup_data + so->itup_off2, k2p);
                else smol_copy_small(so->itup_data + so->itup_off2, k2p, so->key_len2);
                scan->xs_itup = so->itup; ItemPointerSet(&(scan->xs_heaptid), 0, 1);
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
                    if (so->have_k1_eq)
                    {
                            int c = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, keyp, so->key_len, so->key_byval, so->bound_datum);
                        if (c < 0)
                        {
                            /* Past the equality run when scanning backward: terminate overall */
                            so->cur_blk = InvalidBlockNumber;
                            break;
                        }
                        else if (c > 0)
                        {
                            /* Should not happen; skip defensively */
                            so->cur_off--;
                            continue;
                        }
                        /* c == 0: emit normally */
                    }
                    if (so->key_len == 2) so->copy1_fn(so->itup_data, keyp);
                    else if (so->key_len == 4) so->copy1_fn(so->itup_data, keyp);
                    else if (so->key_len == 8) so->copy1_fn(so->itup_data, keyp);
                    else if (so->key_len == 16) smol_copy16(so->itup_data, keyp);
                    else smol_copy_small(so->itup_data, keyp, so->key_len);
                    /* copy INCLUDE attrs */
                    if (so->ninclude > 0)
                    {
                        uint16 n2 = smol_leaf_nitems(page);
                        uint32 row = (uint32) (so->cur_off - 1);
        for (uint16 ii=0; ii<so->ninclude; ii++)
        {
            char *ip = smol1_inc_ptr(page, so->key_len, n2, so->inc_len, so->ninclude, ii, row);
            if (so->inc_len[ii] == 2) so->inc_copy[ii](so->itup_data + so->inc_offs[ii], ip);
            else if (so->inc_len[ii] == 4) so->inc_copy[ii](so->itup_data + so->inc_offs[ii], ip);
            else if (so->inc_len[ii] == 8) so->inc_copy[ii](so->itup_data + so->inc_offs[ii], ip);
            else if (so->inc_len[ii] == 16) smol_copy16(so->itup_data + so->inc_offs[ii], ip);
            else smol_copy_small(so->itup_data + so->inc_offs[ii], ip, so->inc_len[ii]);
        }
                    }
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
                    if (so->have_k1_eq)
                    {
                            int c = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, keyp, so->key_len, so->key_byval, so->bound_datum);
                        if (c > 0)
                        {
                            /* We've moved past the equality run on this page; advance to next leaf */
                            break;
                        }
                        else if (c < 0)
                        {
                            /* Should not happen (we start at >= bound); skip forward */
                            so->cur_off++;
                            continue;
                        }
                        /* c == 0: emit normally */
                    }
                    if (so->key_len == 2) so->copy1_fn(so->itup_data, keyp);
                    else if (so->key_len == 4) so->copy1_fn(so->itup_data, keyp);
                    else if (so->key_len == 8) so->copy1_fn(so->itup_data, keyp);
                    else if (so->key_len == 16) smol_copy16(so->itup_data, keyp);
                    else smol_copy_small(so->itup_data, keyp, so->key_len);
                    if (so->ninclude > 0)
                    {
                        uint16 n2 = n;
                        uint32 row = (uint32) (so->cur_off - 1);
        for (uint16 ii=0; ii<so->ninclude; ii++)
        {
            char *ip = smol1_inc_ptr(page, so->key_len, n2, so->inc_len, so->ninclude, ii, row);
            if (so->inc_len[ii] == 2) so->inc_copy[ii](so->itup_data + so->inc_offs[ii], ip);
            else if (so->inc_len[ii] == 4) so->inc_copy[ii](so->itup_data + so->inc_offs[ii], ip);
            else if (so->inc_len[ii] == 8) so->inc_copy[ii](so->itup_data + so->inc_offs[ii], ip);
            else if (so->inc_len[ii] == 16) smol_copy16(so->itup_data + so->inc_offs[ii], ip);
            else smol_copy_small(so->itup_data + so->inc_offs[ii], ip, so->inc_len[ii]);
        }
                    }
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
                    int64 lb;
                    if (so->have_bound)
                    {
                        if (so->atttypid == INT2OID) lb = (int64) DatumGetInt16(so->bound_datum);
                        else if (so->atttypid == INT4OID) lb = (int64) DatumGetInt32(so->bound_datum);
                        else if (so->atttypid == INT8OID) lb = DatumGetInt64(so->bound_datum);
                        else lb = PG_INT64_MIN;
                    }
                    else lb = PG_INT64_MIN;
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
        /* Prefetch the next leaf (forward scans) to overlap I/O */
        if (dir != BackwardScanDirection && BlockNumberIsValid(next))
            PrefetchBuffer(idx, MAIN_FORKNUM, next);
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
                so->leaf_n = smol12_leaf_nrows(np);
                so->leaf_i = 0;
                if (so->have_bound)
                {
                    uint16 lo = FirstOffsetNumber, hi = so->leaf_n, ans = InvalidOffsetNumber;
                    while (lo <= hi)
                    {
                        uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                        char *k1p = smol12_row_k1_ptr(np, mid, so->key_len, so->key_len2);
                        int c = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, k1p, so->key_len, so->key_byval, so->bound_datum);
                        if (so->prof_enabled) so->prof_bsteps++;
                        if (c >= 0) { ans = mid; if (mid == 0) break; hi = (uint16) (mid - 1); }
                        else lo = (uint16) (mid + 1);
                    }
                    so->leaf_i = (ans != InvalidOffsetNumber) ? (uint32) (ans - 1) : (uint32) so->leaf_n;
                }
            }
            else
            {
                /* single-col: if we have a bound, re-seek within new leaf */
                if (so->have_bound && dir != BackwardScanDirection)
                {
                    uint16 n2 = smol_leaf_nitems(np);
                    uint16 lo2 = FirstOffsetNumber, hi2 = n2, ans2 = InvalidOffsetNumber;
                    while (lo2 <= hi2)
                    {
                        uint16 mid2 = (uint16) (lo2 + ((hi2 - lo2) >> 1));
                        char *kp2 = smol_leaf_keyptr(np, mid2, so->key_len);
                        int cc = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, kp2, so->key_len, so->key_byval, so->bound_datum);
                        if (so->prof_enabled) so->prof_bsteps++;
                        if ((so->bound_strict ? (cc > 0) : (cc >= 0))) { ans2 = mid2; if (mid2 == 0) break; hi2 = (uint16) (mid2 - 1); }
                        else lo2 = (uint16) (mid2 + 1);
                    }
                    so->cur_off = (ans2 != InvalidOffsetNumber) ? ans2 : (uint16) (n2 + 1);
                }
                else if (so->have_bound && dir == BackwardScanDirection)
                {
                    /* backward: position to last <= bound in this leaf */
                    uint16 n2 = smol_leaf_nitems(np);
                    /* Find first > bound, then step one back */
                    uint16 lo2 = FirstOffsetNumber, hi2 = n2, ans2 = InvalidOffsetNumber;
                    while (lo2 <= hi2)
                    {
                        uint16 mid2 = (uint16) (lo2 + ((hi2 - lo2) >> 1));
                        char *kp2 = smol_leaf_keyptr(np, mid2, so->key_len);
                        int cc = smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, kp2, so->key_len, so->key_byval, so->bound_datum);
                        if (so->prof_enabled) so->prof_bsteps++;
                        if (cc > 0) { ans2 = mid2; if (mid2 == 0) break; hi2 = (uint16) (mid2 - 1); }
                        else lo2 = (uint16) (mid2 + 1);
                    }
                    if (ans2 == InvalidOffsetNumber)
                        so->cur_off = n2;
                    else
                        so->cur_off = (ans2 > FirstOffsetNumber) ? (uint16) (ans2 - 1) : InvalidOffsetNumber;
                }
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
    (void) loop_count;
    IndexOptInfo *idx = path->indexinfo;
    double pages = (idx->pages > 0 ? idx->pages : 1);
    double tuples = (idx->tuples > 0 ? idx->tuples : 1);
    double qual_selec = 0.5;     /* default */
    double cpu_per_tuple = smol_cost_tup;
    double page_cost = smol_cost_page;
    double nscan;

    /* Heuristic selectivity: favor equality/range on leading key */
    if (path->indexclauses != NIL)
    {
        bool have_leading = false;
        bool likely_eq = false;
        ListCell *lc;
        foreach(lc, path->indexclauses)
        {
            IndexClause *ic = (IndexClause *) lfirst(lc);
            if (ic && ic->indexcol == 0)
            {
                have_leading = true;
                /* If exactly one clause on leading key, assume equality-selective */
                likely_eq = true;
                break;
            }
        }
        if (have_leading)
            qual_selec = likely_eq ? smol_selec_eq : smol_selec_range;
        else
            qual_selec = 0.5; /* scan without leading key is undesirable */
    }

    nscan = tuples * qual_selec;
    *indexStartupCost = 0.0;
    *indexPages = pages;
    *indexCorrelation = 0.5;
    *indexSelectivity = (Selectivity) qual_selec;
    *indexTotalCost = (pages * page_cost) + (nscan * cpu_per_tuple);

    /* Discourage scans with no leading-key quals strongly */
    if (qual_selec >= 0.5)
        *indexTotalCost += 1e6;
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
    /* removed unused rbuf/rpage locals */
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
        /* removed unused pbuf/p/pop locals */
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

/* removed unused two-column builder */
#if 0
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
    /* removed unused rbuf/rpage locals */
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
    /* unused */

    return;
}
#endif /* unused 2-col builder */

/* Build single-column tree with INCLUDE attrs from sorted arrays */
static void
smol_build_tree1_inc_from_sorted(Relation idx, const int64 *keys, const char * const *incs,
                                 Size nkeys, uint16 key_len, int inc_count, const uint16 *inc_lens)
{
    Buffer mbuf;
    Page mpage;
    SmolMeta *meta;
    SMOL_LOGF("leaf-write(1col+INCLUDE) start nkeys=%zu inc=%d", (size_t) nkeys, inc_count);
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
        meta->inc_count = (uint16) inc_count;
        for (int i=0;i<inc_count;i++) meta->inc_len[i] = inc_lens[i];
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
    }
    if (nkeys == 0)
        return;

    Size i = 0; BlockNumber prev = InvalidBlockNumber; char *scratch = (char *) palloc(BLCKSZ);
    while (i < nkeys)
    {
        Buffer buf = smol_extend(idx);
        Page page = BufferGetPage(buf);
        smol_init_page(buf, true, InvalidBlockNumber);
        Size fs = PageGetFreeSpace(page);
        Size avail = (fs > sizeof(ItemIdData)) ? (fs - sizeof(ItemIdData)) : 0;
        Size header = sizeof(uint16);
        Size perrow = (Size) key_len; for (int c=0;c<inc_count;c++) perrow += inc_lens[c];
        Size max_n = (avail > header) ? ((avail - header) / perrow) : 0;
        Size remaining = nkeys - i;
        Size n_this = (remaining < max_n) ? remaining : max_n;
        if (n_this == 0)
            ereport(ERROR, (errmsg("smol: cannot fit tuple with INCLUDE on a leaf (perrow=%zu avail=%zu)", (size_t) perrow, (size_t) avail)));
        memcpy(scratch, &n_this, sizeof(uint16));
        char *p = scratch + sizeof(uint16);
        /* keys */
        if (key_len == 8)
        { memcpy(p, (const char *) (keys + i), n_this * 8); p += n_this * 8; }
        else if (key_len == 4)
        { for (Size j=0;j<n_this;j++){ int32 v=(int32) keys[i+j]; memcpy(p + j*4, &v, 4);} p += n_this*4; }
        else
        { for (Size j=0;j<n_this;j++){ int16 v=(int16) keys[i+j]; memcpy(p + j*2, &v, 2);} p += n_this*2; }
        /* includes: contiguous blocks per include column */
        for (int c=0;c<inc_count;c++)
        {
            uint16 len = inc_lens[c];
            Size bytes = (Size) n_this * (Size) len;
            memcpy(p, incs[c] + ((size_t) i * len), bytes);
            p += bytes;
        }
        Size sz = (Size) (p - scratch);
        if (PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false) == InvalidOffsetNumber)
            ereport(ERROR, (errmsg("smol: failed to add leaf payload (INCLUDE)")));
        MarkBufferDirty(buf);
        BlockNumber cur = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);
        if (BlockNumberIsValid(prev))
        {
            Buffer pbuf = ReadBuffer(idx, prev);
            LockBuffer(pbuf, BUFFER_LOCK_EXCLUSIVE);
            Page pp = BufferGetPage(pbuf);
            smol_page_opaque(pp)->rightlink = cur;
            MarkBufferDirty(pbuf);
            UnlockReleaseBuffer(pbuf);
        }
        prev = cur;
        i += n_this;
    }
    /* set root on meta */
    mbuf = ReadBuffer(idx, 0);
    LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
    mpage = BufferGetPage(mbuf);
    meta = smol_meta_ptr(mpage);
    meta->root_blkno = 1; /* leftmost leaf */
    meta->height = 1;
    MarkBufferDirty(mbuf);
    UnlockReleaseBuffer(mbuf);
    pfree(scratch);
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

/* removed unused smol_read_key_as_datum */


static inline int
smol_cmp_keyptr_bound_generic(FmgrInfo *cmp, Oid collation, const char *keyp, uint16 key_len, bool key_byval, Datum bound)
{
    Datum kd;
    if (key_byval)
    {
        if (key_len == 1)
        { char v; memcpy(&v, keyp, 1); kd = CharGetDatum(v); }
        else if (key_len == 2)
        { int16 v; memcpy(&v, keyp, 2); kd = Int16GetDatum(v); }
        else if (key_len == 4)
        { int32 v; memcpy(&v, keyp, 4); kd = Int32GetDatum(v); }
        else if (key_len == 8)
        { int64 v; memcpy(&v, keyp, 8); kd = Int64GetDatum(v); }
        else
            ereport(ERROR, (errmsg("unexpected byval key_len=%u", (unsigned) key_len)));
    }
    else
    {
        kd = PointerGetDatum((void *) keyp);
    }
    int32 c = DatumGetInt32(FunctionCall2Coll(cmp, collation, kd, bound));
    return (c > 0) - (c < 0);
}

/* Legacy integer comparator used by two-column/internal paths */
static inline int
smol_cmp_keyptr_bound(const char *keyp, uint16 key_len, Oid atttypid, int64 bound)
{
    if (key_len == 2)
    { int16 v; memcpy(&v, keyp, 2); return (v > bound) - (v < bound); }
    else if (key_len == 4)
    { int32 v; memcpy(&v, keyp, 4); return ((int64)v > bound) - ((int64)v < bound); }
    else
    { int64 v; memcpy(&v, keyp, 8); return (v > bound) - (v < bound); }
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

/* Legacy two-column helpers (group-directory layout)
 * Note: kept only for reference during the restart; not used by build/scan.
 * The active on-disk format for 2-key leaves is row-major (see smol12_*).
 */
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

/* removed unused two-column per-leaf cache builder */

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
ts_build_cb_any(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsBuildCtxAny *c = (TsBuildCtxAny *) state;
    (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    tuplesort_putdatum(c->ts, values[0], false);
    (*c->pnkeys)++;
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

/* removed unused per-type build callbacks and comparators */

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
        Size bytes1 = (Size) newcap * (Size) c->len1;
        Size bytes2 = (Size) newcap * (Size) c->len2;
        char *n1 = (*c->pcap == 0) ? (char *) palloc(bytes1) : (char *) repalloc(*c->pk1, bytes1);
        char *n2 = (*c->pcap == 0) ? (char *) palloc(bytes2) : (char *) repalloc(*c->pk2, bytes2);
        *c->pk1 = n1; *c->pk2 = n2; *c->pcap = newcap;
    }
    char *dst1 = (*c->pk1) + ((size_t) (*c->pcount) * (size_t) c->len1);
    char *dst2 = (*c->pk2) + ((size_t) (*c->pcount) * (size_t) c->len2);
    if (c->byval1)
    {
        switch (c->len1)
        { case 1: { char v = DatumGetChar(values[0]); memcpy(dst1,&v,1); break; }
          case 2: { int16 v = DatumGetInt16(values[0]); memcpy(dst1,&v,2); break; }
          case 4: { int32 v = DatumGetInt32(values[0]); memcpy(dst1,&v,4); break; }
          case 8: { int64 v = DatumGetInt64(values[0]); memcpy(dst1,&v,8); break; }
          default: ereport(ERROR,(errmsg("unexpected byval len1=%u", (unsigned) c->len1))); }
    }
    else memcpy(dst1, DatumGetPointer(values[0]), c->len1);
    if (c->byval2)
    {
        switch (c->len2)
        { case 1: { char v = DatumGetChar(values[1]); memcpy(dst2,&v,1); break; }
          case 2: { int16 v = DatumGetInt16(values[1]); memcpy(dst2,&v,2); break; }
          case 4: { int32 v = DatumGetInt32(values[1]); memcpy(dst2,&v,4); break; }
          case 8: { int64 v = DatumGetInt64(values[1]); memcpy(dst2,&v,8); break; }
          default: ereport(ERROR,(errmsg("unexpected byval len2=%u", (unsigned) c->len2))); }
    }
    else memcpy(dst2, DatumGetPointer(values[1]), c->len2);
    (*c->pcount)++;
    if (smol_debug_log && smol_progress_log_every > 0 && (*c->pcount % (Size) smol_progress_log_every) == 0)
        SMOL_LOGF("collect pair: tuples=%zu", *c->pcount);
}

/* removed old tuplesort pair collector */

/* ---- Radix sort for fixed-width signed integers ------------------------- */
static inline uint64 smol_norm64(int64 v) { return (uint64) v ^ UINT64_C(0x8000000000000000); }
static inline uint32 smol_norm32(int32 v) { return (uint32) v ^ UINT32_C(0x80000000); }
static inline uint16 smol_norm16(int16 v) { return (uint16) v ^ UINT16_C(0x8000); }

static void smol_radix_sort_idx_u64(const uint64 *key, uint32 *idx, uint32 *tmp, Size n)
{
    if (n < 2) return;
    uint32 count[256];
    for (int pass = 0; pass < 8; pass++)
    {
        memset(count, 0, sizeof(count));
        for (Size i = 0; i < n; i++)
        {
            uint8_t byte = (uint8_t) (key[idx[i]] >> (pass * 8));
            count[byte]++;
        }
        uint32 sum = 0;
        for (int b = 0; b < 256; b++) { uint32 c = count[b]; count[b] = sum; sum += c; }
        for (Size i = 0; i < n; i++)
        {
            uint8_t byte = (uint8_t) (key[idx[i]] >> (pass * 8));
            tmp[count[byte]++] = idx[i];
        }
        memcpy(idx, tmp, n * sizeof(uint32));
    }
}

/* removed unused smol_sort_int64 */

/* removed unused smol_sort_int32 */

/* removed unused smol_sort_int16 */

/* Stable radix sort pairs (k1,k2) by ascending k1 then k2 */
/* Helper: sort idx by 64-bit unsigned key via 8 byte-wise stable passes */
/* removed unused smol_radix_sort_idx_u64_16 */

/* removed unused pair sort variant using index permutations */

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
/* removed unused packed 16-bit pair sort */

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
/* fixed-size copy helpers */
/*
 * Hot-path copy helpers: favor unrolled fixed-size copies and aligned wide
 * loads/stores to reduce memcpy/branch overhead. Fall back to memcpy for
 * potentially-unaligned cases. These are small and declared inline to help
 * the compiler optimize call sites.
 */
static inline void
smol_copy2(char *dst, const char *src)
{
    if ((((uintptr_t) dst | (uintptr_t) src) & (uintptr_t) 1) == 0)
        *(uint16_t *) dst = *(const uint16_t *) src;
    else
        memcpy(dst, src, 2);
}

static inline void
smol_copy4(char *dst, const char *src)
{
    if ((((uintptr_t) dst | (uintptr_t) src) & (uintptr_t) 3) == 0)
        *(uint32_t *) dst = *(const uint32_t *) src;
    else
        memcpy(dst, src, 4);
}

static inline void
smol_copy8(char *dst, const char *src)
{
    if ((((uintptr_t) dst | (uintptr_t) src) & (uintptr_t) 7) == 0)
        *(uint64_t *) dst = *(const uint64_t *) src;
    else
        memcpy(dst, src, 8);
}

/* Try a single 16-byte wide copy when both pointers are 16B-aligned. */
static inline void
smol_copy16(char *dst, const char *src)
{
    if ((((uintptr_t) dst | (uintptr_t) src) & (uintptr_t) 15) == 0)
    {
        struct vec16 { uint64_t a, b; };
        *(struct vec16 *) dst = *(const struct vec16 *) src;
    }
    else
    {
        /* Two 8-byte copies are usually competitive; they handle alignment. */
        smol_copy8(dst, src);
        smol_copy8(dst + 8, src + 8);
    }
}

/* Generic small copy for uncommon fixed lengths (<= 32). */
static inline void
smol_copy_small(char *dst, const char *src, uint16 len)
{
    switch (len)
    {
        case 1: *dst = *src; break;
        case 2: smol_copy2(dst, src); break;
        case 3: smol_copy2(dst, src); dst[2] = src[2]; break;
        case 4: smol_copy4(dst, src); break;
        case 5: smol_copy4(dst, src); dst[4] = src[4]; break;
        case 6: smol_copy4(dst, src); smol_copy2(dst+4, src+4); break;
        case 7: smol_copy4(dst, src); smol_copy2(dst+4, src+4); dst[6] = src[6]; break;
        case 8: smol_copy8(dst, src); break;
        case 9: smol_copy8(dst, src); dst[8] = src[8]; break;
        case 10: smol_copy8(dst, src); smol_copy2(dst+8, src+8); break;
        case 11: smol_copy8(dst, src); smol_copy2(dst+8, src+8); dst[10] = src[10]; break;
        case 12: smol_copy8(dst, src); smol_copy4(dst+8, src+8); break;
        case 13: smol_copy8(dst, src); smol_copy4(dst+8, src+8); dst[12] = src[12]; break;
        case 14: smol_copy8(dst, src); smol_copy4(dst+8, src+8); smol_copy2(dst+12, src+12); break;
        case 15: smol_copy8(dst, src); smol_copy4(dst+8, src+8); smol_copy2(dst+12, src+12); dst[14] = src[14]; break;
        case 16: smol_copy16(dst, src); break;
        default:
            /* For larger but still small sizes, copy in 16B then tail. */
            while (len >= 16) { smol_copy16(dst, src); dst += 16; src += 16; len -= 16; }
            if (len >= 8) { smol_copy8(dst, src); dst += 8; src += 8; len -= 8; }
            if (len >= 4) { smol_copy4(dst, src); dst += 4; src += 4; len -= 4; }
            if (len >= 2) { smol_copy2(dst, src); dst += 2; src += 2; len -= 2; }
            if (len) *dst = *src;
            break;
    }
}
/* Callback to collect single key + INCLUDE ints */
static void
smol_build_cb_inc(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    CollectIncCtx *c = (CollectIncCtx *) state; (void) rel; (void) tid; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    for (int i=0;i<c->incn;i++) if (isnull[1+i]) ereport(ERROR, (errmsg("smol INCLUDE does not support NULL values")));
    if (*c->pcount == *c->pcap)
    {
        Size newcap = (*c->pcap == 0 ? 1024 : *c->pcap * 2);
        int64 *newk = (*c->pcap == 0) ? (int64 *) palloc(newcap * sizeof(int64)) : (int64 *) repalloc(*c->pk, newcap * sizeof(int64));
        *c->pk = newk;
        for (int i=0;i<c->incn;i++)
        {
            Size bytes = (Size) newcap * (Size) c->ilen[i];
            char *old = *c->pi[i];
            char *ni = (*c->pcap == 0) ? (char *) palloc(bytes) : (char *) repalloc(old, bytes);
            *c->pi[i] = ni;
        }
        *c->pcap = newcap;
    }
    (*c->pk)[*c->pcount] = DatumGetInt64(values[0]);
    for (int i=0;i<c->incn;i++)
    {
        char *dst = (*c->pi[i]) + ((size_t)(*c->pcount) * (size_t) c->ilen[i]);
        if (c->ibyval[i])
        {
            switch (c->ilen[i])
            {
                case 1: { char v = DatumGetChar(values[1+i]); memcpy(dst, &v, 1); break; }
                case 2: { int16 v = DatumGetInt16(values[1+i]); memcpy(dst, &v, 2); break; }
                case 4: { int32 v = DatumGetInt32(values[1+i]); memcpy(dst, &v, 4); break; }
                case 8: { int64 v = DatumGetInt64(values[1+i]); memcpy(dst, &v, 8); break; }
                default: ereport(ERROR, (errmsg("unexpected include byval len=%u", (unsigned) c->ilen[i])));
            }
        }
        else
        {
            memcpy(dst, DatumGetPointer(values[1+i]), c->ilen[i]);
        }
    }
    (*c->pcount)++;
}
