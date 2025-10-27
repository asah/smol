#ifndef SMOL_H
#define SMOL_H

/*
 * smol.h
 * 
 * Shared header for SMOL PostgreSQL index access method
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/htup_details.h"
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
#include "funcapi.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "port/atomics.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "access/amvalidate.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_opclass.h"
#include "utils/syscache.h"
#include "utils/regproc.h"
#include "catalog/pg_collation_d.h"
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
#include "pgstat.h"
#include "utils/selfuncs.h"
#include "optimizer/cost.h"
#include "access/parallel.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "storage/ipc.h"
#include "storage/condition_variable.h"
#include "storage/spin.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"
#include "utils/queryenvironment.h"
#include "utils/wait_event.h"


/* ---- Constants and Enums ---- */

/* Adaptive storage format GUCs */
typedef enum
{
    KEY_RLE_V1 = 1,      /* Use V1 format (0x8001u) without continues_byte */
    KEY_RLE_V2 = 2,      /* Use V2 format (0x8002u) with continues_byte */
    KEY_RLE_AUTO = 3     /* Auto-select based on build path (default: V1 for text, V2 for sorted) */
} KeyRLEVersion;

/* Internal constants */
#define SMOL_PROGRESS_LOG_EVERY 250000
#define SMOL_WAIT_LOG_MS 500
#define SMOL_LOG_HEX_LIMIT 16
#define SMOL_LOG_SAMPLE_N 8

/* Page layout tags */
#define SMOL_TAG_KEY_RLE     0x8001u
#define SMOL_TAG_KEY_RLE_V2  0x8002u
#define SMOL_TAG_INC_RLE     0x8003u

/* Metapage constants */
#define SMOL_META_MAGIC   0x534D4F4CUL /* 'SMOL' */
#define SMOL_META_VERSION 1

/* Parallel build shared memory keys */
#define PARALLEL_KEY_SMOL_SHARED  1
#define PARALLEL_KEY_TUPLESORT    2
#define PARALLEL_KEY_QUERY_TEXT   3

/* ---- GUC Variables (extern) ---- */
extern bool smol_debug_log;
extern bool smol_profile_log;
extern double smol_cost_page;
extern double smol_cost_tup;
extern int smol_parallel_claim_batch;
extern int smol_prefetch_depth;
extern double smol_rle_uniqueness_threshold;
extern int smol_key_rle_version;
extern bool smol_use_position_scan;

#ifdef SMOL_TEST_COVERAGE
extern int smol_test_keylen_inflate;
extern int smol_simulate_atomic_race;
extern int smol_atomic_race_counter;
extern int smol_cas_fail_counter;
extern int smol_cas_fail_every;
extern int smol_growth_threshold_test;
extern int smol_force_loop_guard_test;
extern int smol_loop_guard_iteration;
extern int smol_test_force_realloc_at;
extern bool smol_test_force_page_bounds_check;
extern int smol_test_force_parallel_workers;
/*
 * Test-only GUCs for forcing tall tree structures (naming: smol_test_*)
 * In production builds, these are constant 0, causing compiler to eliminate dead code.
 * In coverage builds, they're real GUCs that force tall tree structures for testing.
 */
extern int smol_test_max_internal_fanout;
extern int smol_test_max_tuples_per_page;
/*
 * Test-only GUC for coverage: force smol_find_first_leaf() to return a leaf
 * N blocks earlier than optimal. Used to test binary search in subsequent leaves.
 */
extern int smol_test_leaf_offset;
#else
/* Production builds: test GUCs are constant 0 (dead code elimination) */
#define smol_test_max_internal_fanout 0
#define smol_test_max_tuples_per_page 0
#define smol_test_leaf_offset 0
#endif

/* ---- Sorting Globals (for build functions) ---- */
extern char *smol_sort_k1_buffer, *smol_sort_k2_buffer;
extern uint16 smol_sort_key_len1, smol_sort_key_len2;
extern bool smol_sort_byval1, smol_sort_byval2;
extern FmgrInfo smol_sort_cmp1, smol_sort_cmp2;
extern Oid smol_sort_coll1, smol_sort_coll2;
extern Oid smol_sort_typoid1, smol_sort_typoid2;

/* ---- Logging Macros ---- */
#define SMOL_LOG(msg) \
    do { if (smol_debug_log) elog(LOG, "[smol] %s:%d: %s", __func__, __LINE__, (msg)); } while (0)
#define SMOL_LOGF(fmt, ...) \
    do { if (smol_debug_log) elog(LOG, "[smol] %s:%d: " fmt, __func__, __LINE__, __VA_ARGS__); } while (0)

/* ---- Defensive Checks ---- */
#ifdef SMOL_TEST_COVERAGE
#define SMOL_DEFENSIVE_CHECK(cond, level, args) \
    ({ \
        bool _check_result = (cond); \
        if (!_check_result) { \
            ereport(level, args); \
        } \
        _check_result; \
    })
#define SMOL_ASSERT_BYVAL_LEN(len) \
    SMOL_DEFENSIVE_CHECK((len) == 1 || (len) == 2 || (len) == 4 || (len) == 8, ERROR, \
        (errmsg("smol: invalid byval length %u, expected 1/2/4/8", (unsigned)(len))))
#define SMOL_KEYLEN_ADJUST(len) ((uint16)((len) + smol_test_keylen_inflate))
#define SMOL_ATOMIC_READ_U32(ptr) \
    (smol_simulate_atomic_race == 1 && smol_atomic_race_counter++ < 2 ? 0u : pg_atomic_read_u32(ptr))
#define SMOL_ATOMIC_CAS_U32(ptr, expected, newval) \
    (smol_cas_fail_every > 0 && (++smol_cas_fail_counter % smol_cas_fail_every) == 0 ? false : pg_atomic_compare_exchange_u32(ptr, expected, newval))
#else
#define SMOL_DEFENSIVE_CHECK(cond, level, args) \
    ({ \
        bool _check_result = (cond); \
        if (!_check_result) { \
            ereport(level, args); \
        } \
        _check_result; \
    })
#define SMOL_ASSERT_BYVAL_LEN(len) \
    SMOL_DEFENSIVE_CHECK((len) == 1 || (len) == 2 || (len) == 4 || (len) == 8, ERROR, \
        (errmsg("smol: invalid byval length %u, expected 1/2/4/8", (unsigned)(len))))
#define SMOL_KEYLEN_ADJUST(len) (len)
#define SMOL_ATOMIC_READ_U32(ptr) pg_atomic_read_u32(ptr)
#define SMOL_ATOMIC_CAS_U32(ptr, expected, newval) pg_atomic_compare_exchange_u32(ptr, expected, newval)
#endif


/* ---- Structure Definitions ---- */

/* Page opaque flags */
#define SMOL_F_LEAF     0x0001
#define SMOL_F_INTERNAL 0x0002

/* Metapage structure */
typedef struct SmolMeta
{
    uint32      magic;
    uint16      version;
    uint16      nkeyatts;
    uint16      key_len1;
    uint16      key_len2;
    BlockNumber root_blkno;
    uint16      height;
    uint16      inc_count;
    uint16      inc_len[16];
} SmolMeta;

/* Page opaque data */
typedef struct SmolPageOpaqueData
{
    uint16      flags;
    BlockNumber rightlink;
    BlockNumber leftlink;
} SmolPageOpaqueData;

typedef SmolPageOpaqueData *SmolOpaque;

/* Internal node item */
typedef struct SmolInternalItem
{
    int32       highkey;
    BlockNumber child;
} SmolInternalItem;

/* Leaf reference during build */
typedef struct SmolLeafRef
{
    BlockNumber blk;
} SmolLeafRef;

/*
 * SmolIncludeMetadata - Dynamically allocated INCLUDE column metadata
 *
 * This structure holds all INCLUDE-related arrays that were previously
 * embedded directly in SmolScanOpaqueData. By allocating this dynamically
 * based on the actual number of INCLUDE columns (ninclude), we save
 * significant memory for the common case of 0-3 INCLUDE columns vs the
 * fixed allocation for 16.
 *
 * Allocated in smol_beginscan() when ninclude > 0, freed in smol_endscan().
 */
typedef struct SmolIncludeMetadata
{
    uint16      *inc_len;           /* length of each INCLUDE column */
    uint32      *inc_cumul_offs;    /* cumulative offsets for INCLUDE columns */
    char        *inc_align;         /* alignment for each INCLUDE column */
    uint16      *inc_offs;          /* offsets in tuple data area */
    void      (**inc_copy)(char *dst, const char *src);  /* copy functions */
    bool        *inc_is_text;       /* true if INCLUDE column is text32 */
    bool        *inc_const;         /* true if INCLUDE value is constant within run */
    int16       *run_inc_len;       /* cached varlena lengths for run */
    char       **plain_inc_base;    /* base pointers for plain pages */
    char       **rle_run_inc_ptr;   /* INCLUDE pointers for RLE runs */
    bool        *run_inc_built;     /* true if run varlena blob is built */
    int16       *run_inc_vl_len;    /* varlena blob lengths */
    char      (*run_inc_vl)[VARHDRSZ + 32];  /* varlena blob storage */
} SmolIncludeMetadata;

typedef struct SmolScanOpaqueData
{
    bool        initialized;    /* positioned to first tuple/group? */
    ScanDirection last_dir;     /* last scan direction (to detect direction changes) */
    BlockNumber cur_blk;        /* current leaf blkno */
    OffsetNumber cur_off;       /* 1-based item index for single-col */

    /* pinned buffer for current leaf (no locking needed: index is read-only) */
    Buffer      cur_buf;
    bool        have_pin;

    /* optional lower bound on leading key (>=, >, =) */
    bool        have_bound;
    bool        bound_strict;   /* true when '>' bound (not >=) */
    Datum       bound_datum;    /* bound as Datum for comparator */
    /* optional upper bound on leading key (<=, <) */
    bool        have_upper_bound;
    bool        upper_bound_strict;   /* true when '<' bound (not <=) */
    Datum       upper_bound_datum;    /* upper bound as Datum for comparator */
    FmgrInfo    cmp_fmgr;       /* comparator proc 1 */
    Oid         collation;      /* leading key collation */
    bool        key_byval;      /* byval property of leading key */
    int16       key_typlen;     /* typlen of leading key (1,2,4,8) */
    bool        have_k1_eq;     /* true when leading key equality present */
    /* optional equality filter on second key (attno=2) */
    bool        have_k2_eq;
    int64       k2_eq;

    /* Scankeys for runtime filtering (all predicates) */
    ScanKey     runtime_keys;
    int         n_runtime_keys;
    bool        need_runtime_key_test; /* cached: true if runtime key testing needed (opt #4) */

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
    uint16      itup_data_off;  /* data offset from tuple start (for varwidth sizing) */
    char        align1;         /* typalign for attr1 */
    char        align2;         /* typalign for attr2 (if any) */
    /* fixed-size copy helpers to avoid per-row branches */
    void      (*copy1_fn)(char *dst, const char *src);
    void      (*copy2_fn)(char *dst, const char *src);
    /* INCLUDE metadata (single-col path) - dynamically allocated */
    uint16      ninclude;
    SmolIncludeMetadata *inc_meta;  /* NULL if ninclude == 0, else dynamically allocated */
    bool        run_inc_evaluated;  /* inc_const[] computed for current run */

    /* lightweight profiling (enabled by smol.profile) */
    bool        prof_enabled;
    uint64      prof_calls;     /* amgettuple calls */
    uint64      prof_rows;      /* rows returned */
    uint64      prof_pages;     /* leaf pages visited */
    uint64      prof_bytes;     /* bytes copied into tuples */
    uint64      prof_touched;   /* bytes of key/include touched/emitted (independent of copying) */
    uint64      prof_bsteps;    /* binary-search steps */

    /* two-col per-leaf cache to simplify correct emission */
    int64      *leaf_k1;
    int64      *leaf_k2;
    uint32      leaf_n;
    uint32      leaf_i;
    uint32      leaf_cap;       /* capacity of leaf_k1/leaf_k2 arrays */
    uint32      chunk_left;     /* parallel: leaves remaining in locally claimed chunk */
    BufferAccessStrategy bstrategy; /* bulk-read strategy for index leafs */

    /* Equal-key run optimization (single-key indices): */
    bool        run_active;
    uint16      run_start_off;  /* inclusive (for backward scans) */
    uint16      run_end_off;    /* inclusive (for forward scans) */
    uint16      run_key_len;    /* bytes of key stored in run_key */
    char        run_key[16];    /* store up to 16 bytes of fixed-length key */
    int16       run_text_klen;  /* cached varlena key length for current run (text32) */
    bool        page_is_plain;  /* true when current page is plain (not RLE) - set once per page */
    /* RLE run caching to avoid O(m) scans in smol_leaf_keyptr_ex */
    uint16      rle_cached_run_idx;      /* current cached RLE run index (0-based) */
    uint32      rle_cached_run_acc;      /* accumulated offset before current run */
    uint32      rle_cached_run_end;      /* last offset in current run (1-based) */
    char       *rle_cached_run_keyptr;   /* pointer to current run's key */
    char       *rle_cached_run_ptr;      /* pointer to start of current run data (for fast forward scan) */
    BlockNumber rle_cached_page_blk;     /* which page the cache is valid for */
    uint64      rle_cache_hits;          /* cache hit counter for debugging */
    uint64      rle_cache_misses;        /* cache miss counter for debugging */
    /* Cached page metadata (opt #5): nitems and format cached once per page */
    uint16      cur_page_nitems;    /* cached nitems for current page */
    uint8       cur_page_format;    /* cached format: 0=plain, 2=key_rle, 3=inc_rle */
    bool        plain_inc_cached;   /* true when plain_inc_base[] is valid for current page */
    bool        rle_run_inc_cached;  /* true when rle_run_inc_ptr[] is valid for current run */
    /* Prebuilt varlena blobs reused within run (text) */
    bool        run_key_built;
    int16       run_key_vl_len; /* bytes in run_key_vl (VARHDR+payload) */
    char        run_key_vl[VARHDRSZ + 32];

    /* Adaptive prefetching for bounded scans (slow-start to avoid over-prefetching) */
    uint16      pages_scanned;     /* total pages successfully scanned (not skipped) */
    uint16      adaptive_prefetch_depth; /* current prefetch depth (grows with scan progress) */
    /* V2 RLE continuation support: tracks if last run on previous page continues to current page */
    bool        prev_page_last_run_active;     /* true if previous page's last run continues */
    char        prev_page_last_run_key[16];    /* key from previous page's last run */
    int16       prev_page_last_run_text_klen;  /* text length from previous page's last run */
    /* Position-based scan optimization (two-search approach) */
    bool        use_position_scan;      /* true when position-based scan is active */
    BlockNumber end_blk;                /* end position block number */
    OffsetNumber end_off;               /* end position offset (exclusive) */
    /* key type flags */
    bool        key_is_text32;  /* true when key type is text/varchar packed to 32B */
    bool        has_varwidth;   /* any varlena field present in tuple (key or includes) */
} SmolScanOpaqueData;
typedef SmolScanOpaqueData *SmolScanOpaque;
typedef struct SmolParallelScan
{
    pg_atomic_uint32 curr;    /* next leaf to claim (0 = init, InvalidBlockNumber = done) */
} SmolParallelScan;
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

/* Build callback context and helpers */
typedef struct SmolBuildContextInt16
{
    int16     **pkeys;
    Size       *pnalloc;
    Size       *pnkeys;
} SmolBuildContextInt16;

typedef struct SmolBuildContextInt32
{
    int32     **pkeys;
    Size       *pnalloc;
    Size       *pnkeys;
} SmolBuildContextInt32;

typedef struct SmolBuildContextInt64
{
    int64     **pkeys;
    Size       *pnalloc;
    Size       *pnkeys;
} SmolBuildContextInt64;

/* Generic tuplesort collector for arbitrary fixed-length types */
typedef struct SmolTuplesortContext
{
    Tuplesortstate *ts;
    Size           *pnkeys;
} SmolTuplesortContext;


typedef struct SmolTextBuildContext
{
    Tuplesortstate *ts;
    Size           *pnkeys;
    int            *pmax;
} SmolTextBuildContext;


/* Two-column generic builders */
typedef struct SmolPairContext
{
    char     **pk1;
    char     **pk2;
    Size      *pcap;
    Size      *pcount;
    uint16     len1;
    uint16     len2;
    bool       byval1;
    bool       byval2;
} SmolPairContext;
/* Single-key + INCLUDE collection (generic fixed-length include attrs) */
typedef struct SmolIncludeContext {
    /* key collection: either int64 (fixed-width ints) or fixed-size bytes for text32 */
    int64 **pk;           /* when !key_is_text32 && nkeyatts==1 */
    char  **pkbytes;      /* when key_is_text32 && nkeyatts==1 */
    uint16 key_len;       /* 8/16/32 for text */
    bool   key_is_text32; /* key is TEXTOID packed to key_len */
    /* two-key collection */
    int nkeyatts;         /* 1 or 2 */
    char **pk1buf;        /* first key buffer for 2-key */
    char **pk2buf;        /* second key buffer for 2-key */
    uint16 key_len2;      /* length of second key */
    bool byval1, byval2;  /* pass-by-value flags for 2-key */
    /* include buffers (pointers-to-pointers so we can grow/assign) */
    char **pi[16];
    uint16 ilen[16]; bool ibyval[16]; bool itext[16];
    Size *pcap; Size *pcount; int incn;
} SmolIncludeContext;
typedef struct SMOLShared
{
    /* Immutable state shared across all workers */
    Oid heaprelid;
    Oid indexrelid;
    bool isconcurrent;
    int scantuplesortstates;  /* Number of participants (leader + workers) */

    /* Synchronization */
    ConditionVariable workersdonecv;
    slock_t mutex;

    /* Mutable state updated by workers */
    int nparticipantsdone;
    double reltuples;
    int maxlen;  /* Maximum text length seen across all workers (for text types) */

    /*
     * ParallelTableScanDescData follows. Can't directly embed here, as
     * implementations of the parallel table scan desc interface might need
     * stronger alignment.
     */
} SMOLShared;

#define ParallelTableScanFromSMOLShared(shared) \
    (ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(SMOLShared)))

/* Leader state for coordinating parallel build */
typedef struct SMOLLeader
{
    ParallelContext *pcxt;
    SMOLShared *smolshared;
    Sharedsort *sharedsort;
    Snapshot snapshot;
    int nparticipanttuplesorts;
} SMOLLeader;

/* Build state that gets passed around */
typedef struct SMOLBuildState
{
    Relation heap;
    Relation index;
    IndexInfo *indexInfo;
    SMOLLeader *smolleader;
} SMOLBuildState;

/* ---- Inline Helper Functions ---- */

/* Get metapage pointer */
static inline SmolMeta *
smol_meta_ptr(Page page)
{
    return (SmolMeta *) PageGetContents(page);
}

/* Get page opaque data */
static inline SmolPageOpaqueData *
smol_page_opaque(Page page)
{
    return (SmolPageOpaqueData *) PageGetSpecialPointer(page);
}

/*
 * Fast copy helpers and related functions
 *
 * In coverage builds, these are defined as regular functions in smol_h_coverage.c
 * to allow gcov to measure their coverage. In production builds, they remain
 * static inline for optimal performance.
 */
#ifdef SMOL_TEST_COVERAGE
/* Coverage build: extern declarations (implemented in smol_h_coverage.c) */
extern void smol_copy1(char *dst, const char *src);
extern void smol_copy2(char *dst, const char *src);
extern void smol_copy4(char *dst, const char *src);
extern void smol_copy8(char *dst, const char *src);
extern void smol_copy16(char *dst, const char *src);
extern void smol_copy_small(char *dst, const char *src, uint16 len);
extern uint64 smol_norm64(int64 v);
extern int smol_cmp_keyptr_to_bound(SmolScanOpaque so, const char *keyp);
extern int smol_cmp_keyptr_to_upper_bound(SmolScanOpaque so, const char *keyp);
#else
/* Production build: static inline for performance */
static inline void smol_copy1(char *dst, const char *src)
{
    *dst = *src;
}

static inline void smol_copy2(char *dst, const char *src)
{
    __builtin_memcpy(dst, src, 2);
}

static inline void smol_copy4(char *dst, const char *src)
{
    __builtin_memcpy(dst, src, 4);
}

static inline void smol_copy8(char *dst, const char *src)
{
    __builtin_memcpy(dst, src, 8);
}

static inline void smol_copy16(char *dst, const char *src)
{
    __builtin_memcpy(dst, src, 16);
}

/* Generic small copy for uncommon fixed lengths (<= 32) */
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
            /* For larger but still small sizes, copy in 16B then tail */
            while (len >= 16) { smol_copy16(dst, src); dst += 16; src += 16; len -= 16; }
            if (len >= 8) { smol_copy8(dst, src); dst += 8; src += 8; len -= 8; }
            if (len >= 4) { smol_copy4(dst, src); dst += 4; src += 4; len -= 4; }
            if (len >= 2) { smol_copy2(dst, src); dst += 2; src += 2; len -= 2; }
            if (len) *dst = *src;
            break;
    }
}

/* Radix sort helper: normalize signed int64 to unsigned for bitwise sorting */
static inline uint64
smol_norm64(int64 v)
{
    return (uint64) v ^ UINT64_C(0x8000000000000000);
}
#endif /* SMOL_TEST_COVERAGE */

/* Two-column row pointer helpers */
static inline char *smol12_row_ptr(Page page, uint16 row, uint16 key_len1, uint16 key_len2, uint32 inc_total_len)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *base = (char *) PageGetItem(page, iid);
    size_t row_size = (size_t) key_len1 + (size_t) key_len2 + (size_t) inc_total_len;
    size_t off = sizeof(uint16) + (size_t)(row - 1) * row_size;
    return base + off;
}

static inline char *smol12_row_k1_ptr(Page page, uint16 row, uint16 key_len1, uint16 key_len2, uint32 inc_total_len)
{
    return smol12_row_ptr(page, row, key_len1, key_len2, inc_total_len) + 0;
}

static inline char *smol12_row_k2_ptr(Page page, uint16 row, uint16 key_len1, uint16 key_len2, uint32 inc_total_len)
{
    return smol12_row_ptr(page, row, key_len1, key_len2, inc_total_len) + key_len1;
}

/* Single-column + INCLUDE helpers */
static inline char *smol1_payload(Page page)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    return (char *) PageGetItem(page, iid);
}

/* Get number of rows in two-column leaf page */
static inline uint16 smol12_leaf_nrows(Page page)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 n; memcpy(&n, p, sizeof(uint16)); return n;
}

/* Reset scan run state */
static inline void
smol_run_reset(SmolScanOpaque so)
{
    so->run_active = false;
    so->run_start_off = InvalidOffsetNumber;
    so->run_end_off = InvalidOffsetNumber;
    so->run_key_len = 0;
    so->run_inc_evaluated = false;
    so->rle_run_inc_cached = false;
    so->run_key_built = false;
    if (so->inc_meta)
    {
        for (int i = 0; i < so->ninclude; i++)
            so->inc_meta->run_inc_built[i] = false;
    }
}


/* ---- External Function Declarations ---- */

/* AM handler (smol.c) */
extern Datum smol_handler(PG_FUNCTION_ARGS);

/* AM callback functions (in smol.c or split files) */
extern IndexBuildResult *smol_build(Relation heap, Relation index, struct IndexInfo *indexInfo);
extern void smol_buildempty(Relation index);
extern bool smol_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                        Relation heapRel, IndexUniqueCheck checkUnique, bool indexUnchanged,
                        struct IndexInfo *indexInfo);
extern bytea *smol_options(Datum reloptions, bool validate);
extern bool smol_validate(Oid opclassoid);
extern void smol_costestimate(struct PlannerInfo *root, struct IndexPath *path, double loop_count,
                              Cost *indexStartupCost, Cost *indexTotalCost,
                              Selectivity *indexSelectivity, double *indexCorrelation,
                              double *indexPages);
extern struct IndexBulkDeleteResult *smol_vacuumcleanup(struct IndexVacuumInfo *info,
                                                         struct IndexBulkDeleteResult *stats);
extern CompareType smol_translatestrategy(StrategyNumber strat, Oid opfamily);
extern StrategyNumber smol_translatecmptype(CompareType cmptype, Oid opfamily);
extern IndexScanDesc smol_beginscan(Relation index, int nkeys, int norderbys);
extern void smol_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
extern bool smol_gettuple(IndexScanDesc scan, ScanDirection dir);
extern void smol_endscan(IndexScanDesc scan);
extern bool smol_canreturn(Relation index, int attno);
extern Size smol_estimateparallelscan(Relation index, int nkeys, int norderbys);
extern void smol_initparallelscan(void *target);
extern void smol_parallelrescan(IndexScanDesc scan);

/* Utility functions (smol_utils.c) */
extern void smol_meta_read(Relation idx, SmolMeta *out);
extern void smol_mark_heap0_allvisible(Relation heapRel);
extern Buffer smol_extend(Relation idx);
extern void smol_init_page(Buffer buf, bool leaf, BlockNumber rightlink);
extern void smol_link_siblings(Relation idx, BlockNumber prev, BlockNumber cur);
extern BlockNumber smol_find_first_leaf(Relation idx, int64 lower_bound, Oid atttypid, uint16 key_len);
extern BlockNumber smol_find_first_leaf_generic(Relation idx, SmolScanOpaque so);
extern BlockNumber smol_find_leaf_for_upper_bound(Relation idx, SmolScanOpaque so);
extern void smol_find_end_position(Relation idx, SmolScanOpaque so, BlockNumber *end_blk_out, OffsetNumber *end_off_out);
extern int smol_cmp_keyptr_bound_generic(FmgrInfo *cmp, Oid collation, const char *keyp, uint16 key_len, bool key_byval, Datum bound);
extern int smol_cmp_keyptr_bound(const char *keyp, uint16 key_len, Oid atttypid, int64 bound);
extern uint16 smol_leaf_nitems(Page page);
extern char *smol_leaf_keyptr_ex(Page page, uint16 idx, uint16 key_len, const uint16 *inc_lens, uint16 ninc, const uint32 *inc_cumul_offs);
extern bool smol_key_eq_len(const char *a, const char *b, uint16 len);
extern BlockNumber smol_rightmost_leaf(Relation idx);

/* Parallel build worker entry (must be extern for dynamic loading) */
extern PGDLLEXPORT void smol_parallel_build_main(dsm_segment *seg, shm_toc *toc);


/* Bound comparison helpers (inline for performance in production, extern in coverage builds) */
#ifndef SMOL_TEST_COVERAGE
static inline int
smol_cmp_keyptr_to_bound(SmolScanOpaque so, const char *keyp)
{
    if (so->have_bound && (so->atttypid == INT2OID || so->atttypid == INT4OID || so->atttypid == INT8OID))
        return smol_cmp_keyptr_bound(keyp, so->key_len, so->atttypid, (int64)
                                     (so->atttypid == INT2OID ? (int64) DatumGetInt16(so->bound_datum)
                                                              : so->atttypid == INT4OID ? (int64) DatumGetInt32(so->bound_datum)
                                                                                        : DatumGetInt64(so->bound_datum)));
    if (so->have_bound && (so->atttypid == TEXTOID /* || so->atttypid == VARCHAROID */))
    {
        /* Compare 32-byte padded keyp with detoasted bound text under C collation (binary) */
        text *bt = DatumGetTextPP(so->bound_datum);
        int blen = VARSIZE_ANY_EXHDR(bt);
        const char *b = VARDATA_ANY(bt);
        /* Compute key length up to first zero using memchr */
        const char *kend = (const char *) memchr(keyp, '\0', 32);
        int klen = kend ? (int)(kend - keyp) : 32;
        int minl = (klen < blen) ? klen : blen;
        int cmp = minl ? memcmp(keyp, b, minl) : 0;
        if (cmp != 0) return (cmp > 0) - (cmp < 0);
        /* If common prefix equal, shorter is smaller */
        return (klen > blen) - (klen < blen);
    }
    return smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, keyp, so->key_len, so->key_byval, so->bound_datum);
}

static inline int
smol_cmp_keyptr_to_upper_bound(SmolScanOpaque so, const char *keyp)
{
    /* Precondition: This function should only be called when we have an upper bound.
     * All callers check so->have_upper_bound before calling this function. */
    Assert(so->have_upper_bound);

    if (so->atttypid == INT2OID || so->atttypid == INT4OID || so->atttypid == INT8OID)
        return smol_cmp_keyptr_bound(keyp, so->key_len, so->atttypid, (int64)
                                     (so->atttypid == INT2OID ? (int64) DatumGetInt16(so->upper_bound_datum)
                                                              : so->atttypid == INT4OID ? (int64) DatumGetInt32(so->upper_bound_datum)
                                                                                        : DatumGetInt64(so->upper_bound_datum)));
    if (so->atttypid == TEXTOID)
    {
        text *bt = DatumGetTextPP(so->upper_bound_datum);
        int blen = VARSIZE_ANY_EXHDR(bt);
        const char *b = VARDATA_ANY(bt);
        const char *kend = (const char *) memchr(keyp, '\0', 32);
        int klen = kend ? (int)(kend - keyp) : 32;
        int minl = (klen < blen) ? klen : blen;
        int cmp = minl ? memcmp(keyp, b, minl) : 0;
        if (cmp != 0) return (cmp > 0) - (cmp < 0);
        return (klen > blen) - (klen < blen);
    }
    /* Generic comparator for other types (e.g., UUID, TIMESTAMP, FLOAT8) */
    return smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, keyp, so->key_len, so->key_byval, so->upper_bound_datum);
}
#endif /* !SMOL_TEST_COVERAGE */

#endif /* SMOL_H */
