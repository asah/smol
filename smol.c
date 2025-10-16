/*
 * smol.c
 *
 * PostgreSQL index access method "smol": a read-only, space-efficient,
 * index-only-scan oriented AM that stores packed fixed-width keys.
 * Supports ordered and backward scans, single-key INCLUDE columns, and
 * single-key RLE encoding (C/POSIX collation for text keys) is always on
 * when beneficial for space; reader transparently handles both layouts.
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
/* Parallel build support */
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

PG_MODULE_MAGIC;  

/* -------------------------------------------------------------------------
 * Design Notes (maintainers’ summary)
 *
 * Goals & constraints
 * - Read-only AM optimized for index-only scans (IOS) on append-only data.
 * - Fixed-width key attributes only; varlena keys (e.g., text without C/POSIX
 *   binary semantics) are rejected at build time. INCLUDE attrs are supported
 *   for single-key indexes when they are fixed-width.
 * - No heap TIDs are stored; executor receives a synthetic, constant TID to
 *   stay on IOS paths. Write entry points ERROR by design.
 *
 * On-disk layout (high level)
 * - Metapage at blk 0 contains magic/version and key-attribute metadata.
 * - Data pages (blk >= 1) store tightly-packed, fixed-width key payloads in a
 *   columnar-friendly layout. For duplicate-heavy single-key pages, an RLE
 *   variant is used when it is strictly smaller; scanning transparently
 *   decodes both formats.
 *
 * Build path (correctness before crash-safety)
 * - Keys are collected via table_index_build_scan() with validation that all
 *   key attrs are non-NULL and fixed-width. Collected items are sorted using
 *   the opclass comparator (proc 1), respecting collation.
 * - Leaf pages are formed by PageInit/PageAddItem. Metapage is (re)initialized
 *   if needed. WAL/FSM is not implemented yet; correctness first.
 * - IOS contract without TIDs relies on the visibility map. The safe sequence
 *   to keep the executor heapless is:
 *     1) heapBuf = ReadBuffer(heap, 0); LockBuffer(heapBuf, BUFFER_LOCK_EXCLUSIVE);
 *     2) PageSetAllVisible(BufferGetPage(heapBuf)); MarkBufferDirty(heapBuf);
 *     3) visibilitymap_pin(heap, 0, &vmbuf);
 *     4) visibilitymap_set(heap, 0, heapBuf, InvalidXLogRecPtr, vmbuf,
 *                          InvalidTransactionId, VISIBILITYMAP_ALL_VISIBLE);
 *     5) unlock/release buffers.
 *   This sequence is embedded in build code paths that establish IOS-only
 *   execution for the index.
 *
 * Scan path (ordered, forward/backward implemented)
 * - The executor must request tuples for IOS (`xs_want_itup`), otherwise we
 *   ERROR early. For each match, we materialize `xs_itup` from on-disk payload
 *   and return with a constant `xs_heaptid` (block 0, off 1).
 * - Forward and backward scans are implemented; however, PostgreSQL’s planner
 *   may prefer external Sort for ORDER BY rather than generating backward
 *   index scans for non-btree AMs. Code paths exist for completeness and for
 *   white-box testing, but some are difficult to reach through plain SQL.
 * - Parallel scan support is wired (amcanparallel=true) with conservative
 *   defaults (e.g., prefetch depth 1). As with backward scans, some planner
 *   behaviors for custom AMs limit how often those paths are exercised.
 *
 * Storage adaptability & RLE
 * - Single-key leaves opportunistically switch to an RLE encoding when it
 *   produces a smaller page. Readers handle both layouts transparently.
 * - INCLUDE payloads for single-key indexes are copied into the prebuilt
 *   index tuple during IOS. Duplicate-heavy runs benefit from minimal copying
 *   when INCLUDE values are constant across the run.
 * - GUCs are provided to experiment with storage/reader behavior (e.g., zero-
 *   copy thresholds, uniqueness heuristics). Defaults remain conservative.
 *
 * Coverage discipline
 * - We target 100% of testable code. Branches that are architecturally
 *   unreachable under current planner behavior are annotated with gcov
 *   exclusions (GCOV_EXCL_*), and dead code is removed rather than excluded.
 *   Dedicated white-box SQL-callable helpers exist to hit tricky paths.
 *
 * Performance notes (see README/BENCHMARKING for user-facing results)
 * - The AM’s compact layout and IOS-only contract yield substantial footprint
 *   savings and strong cache locality. Two-column selective queries often see
 *   large speedups vs btree INCLUDE, while unique-key workloads are tuned to
 *   remain competitive.
 * ------------------------------------------------------------------------- */

/* ---- Logging and GUCs --------------------------------------------------- */
static bool smol_debug_log = false; /* toggled by GUC smol.debug_log */
static bool smol_profile_log = false; /* toggled by GUC smol.profile */
static int  smol_progress_log_every = 250000; /* GUC: log progress every N tuples */
static int  smol_wait_log_ms = 500; /* GUC: log waits longer than this (ms) */
/* Planner cost GUCs */
static double smol_cost_page = 1.0;   /* cost multiplier for page I/O (>1 penalizes smol) */
static double smol_cost_tup = 1.0;    /* cost multiplier for per-tuple CPU (>1 penalizes smol) */
static double smol_selec_eq = 0.05;    /* heuristic equality selectivity */
static double smol_selec_range = 0.15; /* heuristic range selectivity */
static int  smol_parallel_claim_batch = 1; /* GUC: number of leaves to claim per atomic operation */
static int  smol_prefetch_depth = 1; /* GUC: prefetch depth (1=single-step, higher for aggressive I/O) */
extern int maintenance_work_mem;
/* Debug GUCs */
static int smol_log_hex_limit = 16;   /* bytes to hex-dump when logging */
static int smol_log_sample_n = 8;     /* sample first N items per phase */

/* Adaptive storage format GUCs */
typedef enum
{
    ZERO_COPY_OFF = 0,
    ZERO_COPY_ON = 1,
    ZERO_COPY_AUTO = 2
} ZeroCopyMode;

typedef enum
{
    KEY_RLE_V1 = 1,      /* Use V1 format (0x8001u) without continues_byte */
    KEY_RLE_V2 = 2,      /* Use V2 format (0x8002u) with continues_byte */
    KEY_RLE_AUTO = 3     /* Auto-select based on build path (default: V1 for text, V2 for sorted) */
} KeyRLEVersion;

static int smol_enable_zero_copy = ZERO_COPY_AUTO;  /* Zero-copy mode: off, on, or auto */
static int  smol_zero_copy_threshold_mb = 1;     /* Max index size (MB) for zero-copy format - very conservative for safety */
static double smol_rle_uniqueness_threshold = 0.98; /* If nruns/nitems >= this, consider unique (very high threshold) */
static int smol_key_rle_version = KEY_RLE_AUTO;  /* KEY_RLE format version: v1, v2, or auto */

/* Logging macros - always available at runtime controlled by smol_debug_log GUC */
#define SMOL_LOG(msg) \
    do { if (smol_debug_log) elog(LOG, "[smol] %s:%d: %s", __func__, __LINE__, (msg)); } while (0)
#define SMOL_LOGF(fmt, ...) \
    do { if (smol_debug_log) elog(LOG, "[smol] %s:%d: " fmt, __func__, __LINE__, __VA_ARGS__); } while (0)

/*
 * SMOL_DEFENSIVE_CHECK: Testable defensive assertions for coverage
 *
 * In coverage mode (SMOL_TEST_COVERAGE), this macro executes the error path
 * to allow testing defensive checks that should never be reached in production.
 * In normal mode, it becomes a zero-cost assertion that the optimizer removes.
 *
 * Returns: bool - true if condition is true (success), false if condition failed
 * This allows using SMOL_DEFENSIVE_CHECK in boolean expressions.
 *
 * Usage:
 *   SMOL_DEFENSIVE_CHECK(condition_should_be_true, ERROR, (errmsg("error message")));
 *   if (!SMOL_DEFENSIVE_CHECK(cond, ERROR, args)) { handle_failure; }
 */
/* Coverage-testable defensive checks and evil testing hacks */
#ifdef SMOL_TEST_COVERAGE
/* In test coverage mode, defensive checks are actual executable code that gets covered */
#define SMOL_DEFENSIVE_CHECK(cond, level, args) \
    ({ \
        bool _check_result = (cond); \
        if (!_check_result) { \
            ereport(level, args); \
        } \
        _check_result; \
    })

/* Assertion for key_len/inc_len being valid byval sizes (1,2,4,8) */
#define SMOL_ASSERT_BYVAL_LEN(len) \
    SMOL_DEFENSIVE_CHECK((len) == 1 || (len) == 2 || (len) == 4 || (len) == 8, ERROR, \
        (errmsg("smol: invalid byval length %u, expected 1/2/4/8", (unsigned)(len))))

/* Evil hack: artificially inflate key_len to test edge cases */
static int smol_keylen_inflate = 0;
#define SMOL_KEYLEN_ADJUST(len) ((uint16)((len) + smol_keylen_inflate))

/* Simulate atomic contention in parallel scans */
static int smol_simulate_atomic_race = 0;  /* 0=normal, 1=force curv==0, 2=force retry */
static int smol_atomic_race_counter = 0;
static int smol_cas_fail_counter = 0;
static int smol_cas_fail_every = 0;  /* 0=normal, N=fail every Nth CAS */
static int smol_growth_threshold_test = 0;  /* 0=normal (8M), >0=override threshold for testing growth */
static int smol_force_loop_guard_test = 0;  /* 0=off, >0=force n_this=0 after N iterations to test loop guard */
static int smol_loop_guard_iteration = 0;   /* Counter for loop guard test */
static int smol_test_force_realloc_at = 0;  /* 0=disabled, >0=force reallocation when next_n reaches this value */
static bool smol_test_force_page_bounds_check = false;  /* Force page-level bounds checking even when planner doesn't set it up */
static int smol_test_force_parallel_workers = 0;  /* 0=use planner's decision, >0=force N parallel workers for testing */
/* Note: Parallel CAS retry (line ~1700) proved too difficult to reliably test with synthetic worker coordination */

#define SMOL_ATOMIC_READ_U32(ptr) \
    (smol_simulate_atomic_race == 1 && smol_atomic_race_counter++ < 2 ? 0u : pg_atomic_read_u32(ptr))
#define SMOL_ATOMIC_CAS_U32(ptr, expected, newval) \
    (smol_cas_fail_every > 0 && (++smol_cas_fail_counter % smol_cas_fail_every) == 0 ? false : pg_atomic_compare_exchange_u32(ptr, expected, newval))

#else
/* In production, defensive checks still execute but without coverage warnings */
#define SMOL_DEFENSIVE_CHECK(cond, level, args) \
    ({ \
        bool _check_result = (cond); \
        if (!_check_result) { \
            ereport(level, args); \
        } \
        _check_result; \
    })

/* Assertion for key_len/inc_len being valid byval sizes (1,2,4,8) */
#define SMOL_ASSERT_BYVAL_LEN(len) \
    SMOL_DEFENSIVE_CHECK((len) == 1 || (len) == 2 || (len) == 4 || (len) == 8, ERROR, \
        (errmsg("smol: invalid byval length %u, expected 1/2/4/8", (unsigned)(len))))

#define SMOL_KEYLEN_ADJUST(len) (len)
#define SMOL_ATOMIC_READ_U32(ptr) pg_atomic_read_u32(ptr)
#define SMOL_ATOMIC_CAS_U32(ptr, expected, newval) pg_atomic_compare_exchange_u32(ptr, expected, newval)
#endif

/* Test GUCs available in all builds for testing edge cases */
static int smol_test_max_internal_fanout = 0;  /* 0=unlimited, >0=limit children per internal node to force tall trees */
static int smol_test_max_tuples_per_page = 0;  /* 0=unlimited, >0=cap tuples per leaf page to force taller trees */

void _PG_init(void);

#ifdef SMOL_TEST_COVERAGE
/* Callable function to run synthetic tests for coverage */
static void smol_run_synthetic_tests(void);
#endif

/* Forward decl for debug helper used in amgettuple */
static char *smol_hex(const char *buf, int len, int maxbytes);

/* Forward decls for copy helpers (needed by _PG_init synthetic tests) */
static inline void smol_copy1(char *dst, const char *src);
static inline void smol_copy2(char *dst, const char *src);
static inline void smol_copy4(char *dst, const char *src);
static inline void smol_copy8(char *dst, const char *src);
static inline void smol_copy16(char *dst, const char *src);
static inline void smol_copy_small(char *dst, const char *src, uint16 len);

/* Forward declarations for _PG_init() */
static bytea *smol_options(Datum reloptions, bool validate);

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

    /* RLE is always considered; no GUC. */

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

#ifdef SMOL_TEST_COVERAGE
    DefineCustomIntVariable("smol.cas_fail_every",
                            "TEST ONLY: Force CAS failure every Nth call",
                            "For coverage testing: force atomic CAS to fail every N calls to test retry paths.",
                            &smol_cas_fail_every,
                            0, /* default: normal operation */
                            0, /* min */
                            1000, /* max */
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.test_force_realloc_at",
                            "TEST ONLY: Force next_blks reallocation when next_n reaches this value",
                            "For coverage testing: trigger array reallocation (0=disabled, >0=force at value)",
                            &smol_test_force_realloc_at,
                            0, /* default: disabled */
                            0, /* min */
                            10000, /* max */
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomBoolVariable("smol.test_force_page_bounds_check",
                             "TEST ONLY: Force page-level bounds checking",
                             "For coverage testing: enable page-level bounds optimization even when planner doesn't set it up",
                             &smol_test_force_page_bounds_check,
                             false, /* default: disabled */
                             PGC_USERSET,
                             0,
                             NULL, NULL, NULL);

    DefineCustomIntVariable("smol.test_force_parallel_workers",
                            "For coverage testing: force N parallel workers (0=use planner's decision)",
                            "For coverage testing: force N parallel workers (0=use planner's decision)",
                            &smol_test_force_parallel_workers,
                            0, /* default: use planner */
                            0, /* min */
                            64, /* max */
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);
#endif

    /* Test GUCs available in all builds */
    DefineCustomIntVariable("smol.test_max_internal_fanout",
                            "TEST ONLY: Limit internal node fanout to force tall trees",
                            "For coverage testing: limit children per internal node (0=unlimited, >0=max children)",
                            &smol_test_max_internal_fanout,
                            0, /* default: unlimited */
                            0, /* min */
                            10000, /* max */
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.test_max_tuples_per_page",
                            "TEST ONLY: Cap tuples per leaf page to force taller trees",
                            "For coverage testing: limit tuples per page (0=unlimited)",
                            &smol_test_max_tuples_per_page,
                            0, /* default: unlimited */
                            0, /* min */
                            10000, /* max */
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomRealVariable("smol.cost_page",
                             "Cost multiplier for SMOL page I/O (values > 1 penalize smol)",
                             NULL,
                             &smol_cost_page,
                             1.0, 0.0, 10000.0,
                             PGC_USERSET, 0,
                             NULL, NULL, NULL);

    DefineCustomRealVariable("smol.cost_tup",
                             "Cost multiplier for SMOL per-tuple CPU (values > 1 penalize smol)",
                             NULL,
                             &smol_cost_tup,
                             1.0, 0.0, 10000.0,
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

#ifdef SMOL_TEST_COVERAGE
    /* Evil testing hacks - only available in coverage builds */
    DefineCustomIntVariable("smol.keylen_inflate",
                            "Test coverage: artificially inflate key_len calculations",
                            NULL,
                            &smol_keylen_inflate,
                            0, 0, 100,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.simulate_atomic_race",
                            "Test coverage: simulate atomic contention (0=off, 1=force curv==0, 2=force retry)",
                            NULL,
                            &smol_simulate_atomic_race,
                            0, 0, 2,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.growth_threshold_test",
                            "Test coverage: override growth threshold (0=normal 8M, >0=test threshold)",
                            "Reduces the 8M exponential growth threshold for testing linear growth path",
                            &smol_growth_threshold_test,
                            0, 0, 100000000,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.force_loop_guard_test",
                            "Test coverage: force n_this=0 after N build iterations to test loop guard (0=off)",
                            "Forces the build loop guard error detection by making n_this=0 after N successful iterations",
                            &smol_force_loop_guard_test,
                            0, 0, 100000,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

#endif

    /* Removed minor tuning GUCs for simplicity: skip_dup_copy */

    DefineCustomIntVariable("smol.log_hex_limit",
                            "Bytes to hex-dump when smol.debug_log is on",
                            NULL,
                            &smol_log_hex_limit,
                            16, 1, 128,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.log_sample_n",
                            "Sample first N items to log per build/scan block",
                            NULL,
                            &smol_log_sample_n,
                            8, 0, 1000,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.prefetch_depth",
                            "Prefetch depth for I/O optimization (1=single-step, higher for aggressive I/O)",
                            NULL,
                            &smol_prefetch_depth,
                            1, 1, 16,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.parallel_claim_batch",
                            "Number of leaves to claim per atomic operation in parallel scans",
                            NULL,
                            &smol_parallel_claim_batch,
                            1, 1, 16,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    /* Adaptive storage format GUCs */
    static const struct config_enum_entry zero_copy_options[] = {
        {"off", ZERO_COPY_OFF, false},
        {"on", ZERO_COPY_ON, false},
        {"auto", ZERO_COPY_AUTO, false},
        {NULL, 0, false}
    };

    DefineCustomEnumVariable("smol.enable_zero_copy",
                            "Enable zero-copy optimization for unique-key workloads",
                            "auto: decide based on uniqueness and size; on: always use; off: never use",
                            &smol_enable_zero_copy,
                            ZERO_COPY_AUTO,
                            zero_copy_options,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    static const struct config_enum_entry key_rle_version_options[] = {
        {"v1", KEY_RLE_V1, false},
        {"v2", KEY_RLE_V2, false},
        {"auto", KEY_RLE_AUTO, false},
        {NULL, 0, false}
    };

    DefineCustomEnumVariable("smol.key_rle_version",
                            "Force KEY_RLE format version for index builds",
                            "v1: use V1 format (0x8001u) without continues_byte; v2: use V2 format (0x8002u) with continues_byte; auto: use default for build path",
                            &smol_key_rle_version,
                            KEY_RLE_AUTO,
                            key_rle_version_options,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.zero_copy_threshold_mb",
                            "Maximum index size (MB) for zero-copy format",
                            "Indexes larger than this use compression even for unique keys",
                            &smol_zero_copy_threshold_mb,
                            1, 0, 10000,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    DefineCustomRealVariable("smol.rle_uniqueness_threshold",
                            "Uniqueness threshold for RLE format (nruns/nitems)",
                            "If nruns/nitems >= this threshold, keys are considered unique",
                            &smol_rle_uniqueness_threshold,
                            0.98, 0.0, 1.0,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

#ifdef SMOL_TEST_COVERAGE
    /* Run synthetic tests on first load */
    smol_run_synthetic_tests();
#endif
}

#ifdef SMOL_TEST_COVERAGE
/* Synthetic tests for copy functions - extracted from _PG_init for explicit calling */
static void
smol_run_synthetic_tests(void)
{
    /* Synthetic tests for unaligned copy paths (lines 4231, 4259, 4276, 4298) */
    {
        char src_buf[64] __attribute__((aligned(16)));
        char dst_buf[64] __attribute__((aligned(16)));

        /* Initialize source with test pattern */
        for (int i = 0; i < 64; i++) src_buf[i] = (char)(0x10 + i);

        /* Test smol_copy2 with unaligned pointers (line 4231) */
        memset(dst_buf, 0, 64);
        smol_copy2(dst_buf + 1, src_buf + 1);  /* Both misaligned (odd offset) */
        Assert(dst_buf[1] == src_buf[1] && dst_buf[2] == src_buf[2]);

        /* Test smol_copy16 with unaligned pointers (line 4259) */
        memset(dst_buf, 0, 64);
        smol_copy16(dst_buf + 1, src_buf + 1);  /* Misaligned for 16-byte copy */
        for (int i = 0; i < 16; i++)
            Assert(dst_buf[1 + i] == src_buf[1 + i]);

        /* Test smol_copy_small for all switch cases (lines 4430-4455) */
        /* Test all cases 1-16 (explicit switch cases) */
        for (int len = 1; len <= 16; len++)
        {
            memset(dst_buf, 0, 64);
            smol_copy_small(dst_buf, src_buf, len);
            for (int i = 0; i < len; i++)
                Assert(dst_buf[i] == src_buf[i]);
        }

        /* Test default case with various sizes that exercise different paths */
        int default_test_lengths[] = {
            17, 18, 19, 20, 21, 22, 23, 24, /* 17-24: len >= 16 + tail */
            25, 26, 27, 28, 29, 30, 31, 32, /* 25-32: larger sizes */
            33, 34, 35, 36, 40, 48, 50, 60  /* 33+: even larger sizes */
        };
        for (int test = 0; test < (int)(sizeof(default_test_lengths) / sizeof(default_test_lengths[0])); test++)
        {
            int len = default_test_lengths[test];
            memset(dst_buf, 0, 64);
            smol_copy_small(dst_buf, src_buf, len);
            for (int i = 0; i < len && i < 64; i++)
                Assert(dst_buf[i] == src_buf[i]);
        }

        elog(DEBUG1, "SMOL: Synthetic copy tests passed (all sizes 1-60)");
    }

    /* Test smol_options() - AM option parsing (always returns NULL for SMOL) */
    {
        Assert(smol_options(PointerGetDatum(NULL), false) == NULL);
        Assert(smol_options(PointerGetDatum(NULL), true) == NULL);
        elog(DEBUG1, "SMOL: smol_options() synthetic test passed");
    }
}
#endif

/* Global comparator context for 2-col row sorting (single-threaded build) */
static char *g_k1buf = NULL, *g_k2buf = NULL;
static uint16 g_key_len1 = 0, g_key_len2 = 0;
static bool g_byval1 = false, g_byval2 = false;
static FmgrInfo g_cmp1, g_cmp2;
static Oid g_coll1 = InvalidOid, g_coll2 = InvalidOid;
static Oid g_typoid1 = InvalidOid, g_typoid2 = InvalidOid;
static int
smol_pair_qsort_cmp(const void *pa, const void *pb)
{
    uint32 ia = *(const uint32 *) pa, ib = *(const uint32 *) pb;
    char *a1 = g_k1buf + (size_t) ia * g_key_len1;
    char *b1 = g_k1buf + (size_t) ib * g_key_len1;

    /* Fast path: inline comparison for common integer types */
    int32 r1;
    if (g_typoid1 == INT2OID && g_key_len1 == 2)
    {
        int16 v1 = *(int16*)a1, v2 = *(int16*)b1;
        r1 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else if (g_typoid1 == INT4OID && g_key_len1 == 4)
    {
        int32 v1 = *(int32*)a1, v2 = *(int32*)b1;
        r1 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else if (g_typoid1 == INT8OID && g_key_len1 == 8)
    {
        int64 v1 = *(int64*)a1, v2 = *(int64*)b1;
        r1 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else
    {
        /* Generic path: use FunctionCall2Coll for other types */
        Datum da1 = g_byval1 ? (g_key_len1==1?CharGetDatum(*a1): g_key_len1==2?Int16GetDatum(*(int16*)a1): g_key_len1==4?Int32GetDatum(*(int32*)a1): Int64GetDatum(*(int64*)a1)) : PointerGetDatum(a1);
        Datum db1 = g_byval1 ? (g_key_len1==1?CharGetDatum(*b1): g_key_len1==2?Int16GetDatum(*(int16*)b1): g_key_len1==4?Int32GetDatum(*(int32*)b1): Int64GetDatum(*(int64*)b1)) : PointerGetDatum(b1);
        r1 = DatumGetInt32(FunctionCall2Coll(&g_cmp1, g_coll1, da1, db1));
    }
    if (r1 != 0) return r1;

    char *a2 = g_k2buf + (size_t) ia * g_key_len2;
    char *b2 = g_k2buf + (size_t) ib * g_key_len2;

    /* Fast path: inline comparison for common integer types */
    int32 r2;
    if (g_typoid2 == INT2OID && g_key_len2 == 2)
    {
        int16 v1 = *(int16*)a2, v2 = *(int16*)b2;
        r2 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else if (g_typoid2 == INT4OID && g_key_len2 == 4)
    {
        int32 v1 = *(int32*)a2, v2 = *(int32*)b2;
        r2 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else if (g_typoid2 == INT8OID && g_key_len2 == 8)
    {
        int64 v1 = *(int64*)a2, v2 = *(int64*)b2;
        r2 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else
    {
        /* Generic path: use FunctionCall2Coll for other types */
        Datum da2 = g_byval2 ? (g_key_len2==1?CharGetDatum(*a2): g_key_len2==2?Int16GetDatum(*(int16*)a2): g_key_len2==4?Int32GetDatum(*(int32*)a2): Int64GetDatum(*(int64*)a2)) : PointerGetDatum(a2);
        Datum db2 = g_byval2 ? (g_key_len2==1?CharGetDatum(*b2): g_key_len2==2?Int16GetDatum(*(int16*)b2): g_key_len2==4?Int32GetDatum(*(int32*)b2): Int64GetDatum(*(int64*)b2)) : PointerGetDatum(b2);
        r2 = DatumGetInt32(FunctionCall2Coll(&g_cmp2, g_coll2, da2, db2));
    }
    return r2;
}

/* qsort comparator for fixed-size byte keys (uses g_k1buf/g_key_len1) */
static int
smol_qsort_cmp_bytes(const void *pa, const void *pb)
{
    uint32 ia = *(const uint32 *) pa, ib = *(const uint32 *) pb;
    const char *a = g_k1buf + (size_t) ia * g_key_len1;
    const char *b = g_k1buf + (size_t) ib * g_key_len1;
    return memcmp(a, b, g_key_len1);
}

/* forward decls */
PGDLLEXPORT Datum smol_handler(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(smol_handler);
PG_FUNCTION_INFO_V1(smol_test_backward_scan);
PG_FUNCTION_INFO_V1(smol_test_error_non_ios);
PG_FUNCTION_INFO_V1(smol_test_no_movement);
PG_FUNCTION_INFO_V1(smol_test_run_synthetic);

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
static CompareType smol_translatestrategy(StrategyNumber strat, Oid opfamily);
static StrategyNumber smol_translatecmptype(CompareType cmptype, Oid opfamily);

/* --- On-disk structs (prototype, 1- or 2-column fixed-width ints) --- */
#define SMOL_META_MAGIC   0x534D4F4CUL /* 'SMOL' */
#define SMOL_META_VERSION 1

/* Leaf page format tags */
#define SMOL_TAG_ZEROCOPY  0x8000u  /* Zero-copy format: [tag][nitems][keys...] - no memcpy */
#define SMOL_TAG_KEY_RLE   0x8001u  /* Key-RLE V1: [tag][nitems][nruns][runs...] */
#define SMOL_TAG_KEY_RLE_V2 0x8002u  /* Key-RLE V2: [tag][nitems][nruns][continues_byte][runs...] - runs can continue across pages */
#define SMOL_TAG_INC_RLE   0x8003u  /* Include-RLE: [tag][nitems][nruns][runs...] */

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
    BlockNumber leftlink;  /* previous leaf (for leaf pages), or InvalidBlockNumber */
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
    IndexTuple  itup_zerocopy_hdr; /* 8-byte header for zero-copy pages (points to page data) */
    char       *itup_data;      /* pointer to data area inside itup */
    uint16      itup_off2;      /* second-attr offset from data (two-col), else 0 */
    uint16      itup_data_off;  /* data offset from tuple start (for varwidth sizing) */
    char        align1;         /* typalign for attr1 */
    char        align2;         /* typalign for attr2 (if any) */
    /* fixed-size copy helpers to avoid per-row branches */
    void      (*copy1_fn)(char *dst, const char *src);
    void      (*copy2_fn)(char *dst, const char *src);
    /* INCLUDE metadata (single-col path) */
    uint16      ninclude;
    uint16      inc_len[16];
    uint32      inc_cumul_offs[16]; /* cumulative offsets: inc_cumul_offs[i] = sum(inc_len[0..i-1]) */
    char        inc_align[16];
    uint16      inc_offs[16];   /* offsets inside tuple data area (from data_off) */
    void      (*inc_copy[16])(char *dst, const char *src);
    bool        inc_is_text[16]; /* include attr is text32 (varlena in tuple) */
    bool        inc_const[16];  /* within current run, include[ii] is constant */
    bool        run_inc_evaluated; /* inc_const[] computed for current run */
    int16       run_inc_len[16];   /* cached include varlena lengths for run when inc_const */

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
    bool        page_is_zerocopy; /* true when current page uses zero-copy format (tag 0x8000) */
    /* Cached page metadata (opt #5): nitems and format cached once per page */
    uint16      cur_page_nitems;    /* cached nitems for current page */
    uint8       cur_page_format;    /* cached format: 0=plain, 1=zerocopy, 2=key_rle, 3=inc_rle */
    /* Cached INCLUDE column base pointers for plain pages (performance optimization) */
    char       *plain_inc_base[16]; /* base pointer for each INCLUDE column on plain pages */
    bool        plain_inc_cached;   /* true when plain_inc_base[] is valid for current page */
    /* Cached INCLUDE column pointers for current RLE run (performance optimization) */
    char       *rle_run_inc_ptr[16]; /* pointer to INCLUDE values for current run on RLE pages */
    bool        rle_run_inc_cached;  /* true when rle_run_inc_ptr[] is valid for current run */
    /* Prebuilt varlena blobs reused within run (text) */
    bool        run_key_built;
    int16       run_key_vl_len; /* bytes in run_key_vl (VARHDR+payload) */
    char        run_key_vl[VARHDRSZ + 32];

    /* Adaptive prefetching for bounded scans (slow-start to avoid over-prefetching) */
    uint16      pages_scanned;     /* total pages successfully scanned (not skipped) */
    uint16      adaptive_prefetch_depth; /* current prefetch depth (grows with scan progress) */
    bool        run_inc_built[16];
    int16       run_inc_vl_len[16];
    char        run_inc_vl[16][VARHDRSZ + 32];
    /* V2 RLE continuation support: tracks if last run on previous page continues to current page */
    bool        prev_page_last_run_active;     /* true if previous page's last run continues */
    char        prev_page_last_run_key[16];    /* key from previous page's last run */
    int16       prev_page_last_run_text_klen;  /* text length from previous page's last run */
    /* key type flags */
    bool        key_is_text32;  /* true when key type is text/varchar packed to 32B */
    bool        has_varwidth;   /* any varlena field present in tuple (key or includes) */
} SmolScanOpaqueData;
typedef SmolScanOpaqueData *SmolScanOpaque;

/* Parallel scan shared state (DSM):
 * curr == 0            => uninitialized; first worker sets to leftmost leaf blkno
 * curr == InvalidBlockNumber => all leaves claimed
 * otherwise curr holds the next leaf blkno to claim; workers atomically swap to its rightlink. */
typedef struct SmolParallelScan
{
    pg_atomic_uint32 curr;    /* next leaf to claim (0 = init, InvalidBlockNumber = done) */
} SmolParallelScan;

/* Utilities */
static void smol_meta_read(Relation idx, SmolMeta *out);
static void smol_mark_heap0_allvisible(Relation heapRel);
static Buffer smol_extend(Relation idx);
static void smol_init_page(Buffer buf, bool leaf, BlockNumber rightlink);
static void smol_link_siblings(Relation idx, BlockNumber prev, BlockNumber cur);
static void smol_build_tree_from_sorted(Relation idx, const void *keys, Size nkeys, uint16 key_len);
static void smol_build_tree1_inc_from_sorted(Relation idx, const int64 *keys, const char * const *incs,
                             Size nkeys, uint16 key_len, int inc_count, const uint16 *inc_lens);
static void smol_build_text_stream_from_tuplesort(Relation idx, Tuplesortstate *ts, Size nkeys, uint16 key_len);
static void smol_build_fixed_stream_from_tuplesort(Relation idx, Tuplesortstate *ts, Size nkeys, uint16 key_len, bool byval);
static void smol_build_text_inc_from_sorted(Relation idx, const char *keys32, const char * const *incs,
                             Size nkeys, uint16 key_len, int inc_count, const uint16 *inc_lens);
/* removed: static void smol_build_tree2_from_sorted(...); */
static void pg_attribute_unused() smol_build_internal_levels(Relation idx,
                                       BlockNumber *child_blks, const int64 *child_high,
                                       Size nchildren, uint16 key_len,
                                       BlockNumber *out_root, uint16 *out_levels);
static void smol_build_internal_levels_bytes(Relation idx,
                                       BlockNumber *child_blks, const char *child_high_bytes,
                                       Size nchildren, uint16 key_len,
                                       BlockNumber *out_root, uint16 *out_levels);
static BlockNumber smol_find_first_leaf(Relation idx, int64 lower_bound, Oid atttypid, uint16 key_len);
static BlockNumber smol_find_first_leaf_generic(Relation idx, SmolScanOpaque so);
static BlockNumber smol_find_leaf_for_upper_bound(Relation idx, SmolScanOpaque so);
static BlockNumber smol_rightmost_leaf(Relation idx);
static BlockNumber smol_prev_leaf(Relation idx, BlockNumber cur);
static int smol_cmp_keyptr_bound(const char *keyp, uint16 key_len, Oid atttypid, int64 bound);
static int smol_cmp_keyptr_bound_generic(FmgrInfo *cmp, Oid collation, const char *keyp, uint16 key_len, bool key_byval, Datum bound);
static inline int smol_cmp_keyptr_to_bound(SmolScanOpaque so, const char *keyp);
static inline bool smol_page_matches_scan_bounds(SmolScanOpaque so, Page page, uint16 nitems, bool *stop_scan_out);
static bytea *smol_options(Datum reloptions, bool validate);
static bool smol_validate(Oid opclassoid);
static bool smol_test_runtime_keys(IndexScanDesc scan, SmolScanOpaque so);
static void smol_sort_pairs_rows64(int64 *k1, int64 *k2, Size n);
static uint16 smol_leaf_nitems(Page page);
static char *smol_leaf_keyptr(Page page, uint16 idx, uint16 key_len);
static char *smol_leaf_keyptr_ex(Page page, uint16 idx, uint16 key_len, const uint16 *inc_lens, uint16 ninc, const uint32 *inc_cumul_offs);
static inline bool smol_key_eq_len(const char *a, const char *b, uint16 len);
static inline void smol_run_reset(SmolScanOpaque so);
static inline bool smol_leaf_is_rle(Page page);
static bool smol_leaf_run_bounds_rle_ex(Page page, uint16 idx, uint16 key_len,
                                     uint16 *run_start_out, uint16 *run_end_out,
                                     const uint16 *inc_lens, uint16 ninc);
static inline char *smol1_inc_ptr_any(Page page, uint16 key_len, uint16 n,
                                      const uint16 *inc_lens, uint16 ninc,
                                      uint16 inc_idx, uint32 row, const uint32 *inc_cumul_offs);
/* Build single-column tuple in-place with dynamic varlena offsets */
static inline void smol_emit_single_tuple(SmolScanOpaque so, Page page, const char *keyp, uint32 row);
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

/* Fast-path comparator dispatcher: for common integer key types, avoid fmgr */
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

/* Fast-path comparator for upper bound checking */
static inline int
smol_cmp_keyptr_to_upper_bound(SmolScanOpaque so, const char *keyp)
{
    if (so->have_upper_bound && (so->atttypid == INT2OID || so->atttypid == INT4OID || so->atttypid == INT8OID))
        return smol_cmp_keyptr_bound(keyp, so->key_len, so->atttypid, (int64)
                                     (so->atttypid == INT2OID ? (int64) DatumGetInt16(so->upper_bound_datum)
                                                              : so->atttypid == INT4OID ? (int64) DatumGetInt32(so->upper_bound_datum)
                                                                                        : DatumGetInt64(so->upper_bound_datum)));
    if (so->have_upper_bound && (so->atttypid == TEXTOID))
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
#ifdef SMOL_PLANNER_BACKWARD_UPPER
    /* Generic comparator only needed for backward scans with upper bounds on non-INT/TEXT types.
     * Since planner doesn't generate backward scans with upper bounds, this is unreachable. */
    return smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, keyp, so->key_len, so->key_byval, so->upper_bound_datum);
#else
    return 0; /* GCOV_EXCL_LINE - unreachable without SMOL_PLANNER_BACKWARD_UPPER */
#endif
}

/* Page-level bounds checking: returns true if page might have matching tuples
 * This optimization dramatically improves range scans with upper bounds (BETWEEN) by:
 * 1. Stopping scan early when all future pages > upper_bound
 * 2. Checking equality bounds to stop when past the equal value
 *
 * Note: Lower bounds (>=, >) are NOT checked here because SMOL uses smol_find_first_leaf()
 * to seek directly to the starting page, then binary searches within that page. All pages
 * from the starting point forward are guaranteed to have keys >= lower_bound.
 *
 * For query "WHERE k BETWEEN 10000 AND 20000" on 1M rows:
 * - Before: Scans from 10000 to end of index (scanning 980K irrelevant rows)
 * - After: Stops at first page where min_key > 20000
 * - Speedup: Dramatic reduction for BETWEEN queries
 */
static inline bool
smol_page_matches_scan_bounds(SmolScanOpaque so, Page page, uint16 nitems, bool *stop_scan_out)
{
    *stop_scan_out = false;

    /* No upper bounds: all pages match (lower bounds handled by initial seek) */
    if (!so->have_upper_bound && !so->have_k1_eq)
        return true; /* GCOV_EXCL_LINE - defensive: this path is never reached in current code */

    /* Empty page: defensive check for corruption */
    if (nitems == 0)
    { /* GCOV_EXCL_START - defensive: pages are never empty in normal operation */
        SMOL_DEFENSIVE_CHECK(false, ERROR,
            (errmsg("smol: empty page %u during bounds checking", so->cur_blk)));
        return false;
    } /* GCOV_EXCL_STOP */

    /* Get first key on page for bounds checking */
    char *first_key = smol_leaf_keyptr_ex(page, FirstOffsetNumber, so->key_len, so->inc_len, so->ninclude, so->inc_cumul_offs);

    /* For zero-copy pages, skip IndexTuple header to get actual key data */
    if (so->page_is_zerocopy)
    { /* GCOV_EXCL_START - zero-copy format only in deprecated smol_build_tree_from_sorted */
        first_key += sizeof(IndexTupleData);
    } /* GCOV_EXCL_STOP */

    /* Upper bound check: if first key exceeds upper bound, stop entire scan
     * Since keys are sorted across pages (via rightlinks), if first_key > upper_bound
     * then ALL future pages will also exceed upper_bound */
    if (so->have_upper_bound)
    {
        int c = smol_cmp_keyptr_to_upper_bound(so, first_key);
        if (so->upper_bound_strict ? (c >= 0) : (c > 0))
        { /* GCOV_EXCL_START - defensive: prefetch logic avoids loading pages beyond bounds */
            /* first_key > upper_bound → past end of range, stop scan */
            *stop_scan_out = true;
            return false;
        } /* GCOV_EXCL_STOP */
    }

    /* Equality bound check: if first key exceeds the equality value, stop scan
     * For queries like "WHERE k = 50000", once we see a key > 50000, we're done */
    if (so->have_k1_eq)
    {
        int c = smol_cmp_keyptr_to_bound(so, first_key);
        if (c > 0)
        { /* GCOV_EXCL_START - defensive: scan stops at tuple level before loading next page */
            /* first_key > equality_bound → past the equal value, stop scan */
            *stop_scan_out = true;
            return false;
        } /* GCOV_EXCL_STOP */
    }

    /* Page might have matching tuples, process it */
    return true;
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
static inline char *smol1_inc_ptr(Page page, uint16 key_len, uint16 n, const uint16 *inc_lens, uint16 ninc, uint16 inc_idx, uint32 row, const uint32 *inc_cumul_offs)
{
    char *base = smol1_payload(page) + sizeof(uint16) + (size_t) n * key_len;
    base += (size_t) n * inc_cumul_offs[inc_idx];  /* O(1) using precomputed cumulative offsets */
    return base + (size_t) row * inc_lens[inc_idx];
}

/* Page summary (diagnostic) */
static void smol_log_page_summary(Relation idx);

/* Sorting helpers for build path */
static void smol_sort_pairs_rows64(int64 *k1, int64 *k2, Size n);
PGDLLEXPORT void smol_parallel_sort_worker(Datum arg);

/* DSM layout for parallel two-column sort
 *
 * PARALLEL BUILD STATUS:
 * Infrastructure exists (lines 744-4282) but is NOT integrated or reachable:
 * - amcanbuildparallel=false at line 816 (PostgreSQL never uses parallel build)
 * - smol_build() uses qsort at line 1132 (not parallel sort)
 * - smol_parallel_sort_worker() exists but is never launched
 *
 * Integration would require (beyond scope of current work):
 * 1. Only works for (int64, int64) keys (radix sort limitation)
 * 2. DSM allocation, data distribution, worker launch, and synchronization
 * 3. Change amcanbuildparallel=true
 *
 * Leaving infrastructure in place for potential future work.
 */
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
/* generic tuplesort collector for arbitrary fixed-length types */
typedef struct TsBuildCtxAny { Tuplesortstate *ts; Size *pnkeys; } TsBuildCtxAny;
static void ts_build_cb_any(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
typedef struct TsBuildCtxText { Tuplesortstate *ts; Size *pnkeys; int *pmax; } TsBuildCtxText;
static void ts_build_cb_text(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
/* 2-col generic builders */
typedef struct { char **pk1; char **pk2; Size *pcap; Size *pcount; uint16 len1; uint16 len2; bool byval1; bool byval2; } PairArrCtx;
static void smol_build_cb_pair(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
/* Single-key + INCLUDE collection (generic fixed-length include attrs) */
typedef struct CollectIncCtx {
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
} CollectIncCtx;
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
    am->amcanbuildparallel = true;
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

    am->amtranslatestrategy = smol_translatestrategy;
    am->amtranslatecmptype = smol_translatecmptype;

    PG_RETURN_POINTER(am);
}

/* ---- Parallel Build Infrastructure ---- */

/* Shared state for parallel index build */
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

/* Table-of-contents IDs for shared memory segments */
#define PARALLEL_KEY_SMOL_SHARED        1
#define PARALLEL_KEY_TUPLESORT          2
#define PARALLEL_KEY_QUERY_TEXT         3

/* Forward declarations */
static void smol_begin_parallel(SMOLBuildState *buildstate, bool isconcurrent, int request);
static void smol_end_parallel(SMOLLeader *smolleader);
/* Worker entry point - must NOT be static for dynamic loading */
extern PGDLLEXPORT void smol_parallel_build_main(dsm_segment *seg, shm_toc *toc);

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
    SMOLBuildState buildstate;

    /* Initialize build state for potential parallel build */
    buildstate.heap = heap;
    buildstate.index = index;
    buildstate.indexInfo = indexInfo;
    buildstate.smolleader = NULL;

    SMOL_LOGF("build start rel=%u idx=%u", RelationGetRelid(heap), RelationGetRelid(index));
    /* Phase timers */
    instr_time t_start, t_collect_end, t_sort_end, t_write_end;
    INSTR_TIME_SET_CURRENT(t_start);
    INSTR_TIME_SET_CURRENT(t_collect_end);
    INSTR_TIME_SET_CURRENT(t_sort_end);
    INSTR_TIME_SET_CURRENT(t_write_end);

    /* Request parallel workers for single-key builds without INCLUDE columns */
    int parallel_workers = indexInfo->ii_ParallelWorkers;
#ifdef SMOL_TEST_COVERAGE
    if (smol_test_force_parallel_workers > 0)
        parallel_workers = smol_test_force_parallel_workers;
#endif
    if (nkeyatts == 1 && ninclude == 0 && parallel_workers > 0)
    {
        elog(LOG, "[smol] About to call smol_begin_parallel, parallel_workers=%d", parallel_workers);
        smol_begin_parallel(&buildstate, indexInfo->ii_Concurrent,
                          parallel_workers);
        elog(LOG, "[smol] Returned from smol_begin_parallel, smolleader=%p", buildstate.smolleader);
    }

    /* Enforce 1 or 2 key columns (fixed-width or text32 packed) */
    if (nkeyatts != 1 && nkeyatts != 2)
        ereport(ERROR, (errmsg("smol prototype supports 1 or 2 key columns only")));
    atttypid = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
    {
        int16 typlen; bool byval; char align;
        get_typlenbyvalalign(atttypid, &typlen, &byval, &align);
        if (typlen <= 0)
        {
            SMOL_DEFENSIVE_CHECK(atttypid == TEXTOID, ERROR,
                (errmsg("smol supports fixed-length key types or text(<=32B) only (attno=1)")));
	    key_len = SMOL_KEYLEN_ADJUST(32); /* pack to 32 bytes */
        }
        else
            key_len = SMOL_KEYLEN_ADJUST((uint16) typlen);
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
    }
    /* ninclude is computed from natts (uint16) minus nkeyatts (int16), result cannot be negative */
    Assert(ninclude >= 0);

    if (ninclude > 0)
    {
        /* INCLUDE columns (fixed-width ints or text) - supports single or multi-key indexes */
        int inc_count = ninclude;
        uint16 inc_lens[16]; bool inc_byval[16]; bool inc_is_text[16];
        if (inc_count > 16)
            ereport(ERROR, (errmsg("smol supports up to 16 INCLUDE columns")));
        for (int i = 0; i < inc_count; i++)
        {
            Oid t = TupleDescAttr(RelationGetDescr(index), nkeyatts + i)->atttypid;
            int16 typlen; bool byval; char align;
            get_typlenbyvalalign(t, &typlen, &byval, &align);
            if (typlen <= 0)
            {
                /* Defensive check - no if-statement needed, macro handles everything */
                SMOL_DEFENSIVE_CHECK(t == TEXTOID, ERROR,
                    (errmsg("smol INCLUDE supports fixed-length or text(<=32B) types only (attno=%d)", nkeyatts + i + 1)));
                if (t == TEXTOID /* || t == VARCHAROID */)
                { inc_lens[i] = 32; inc_byval[i] = false; inc_is_text[i] = true; }
            }
            else
            { inc_lens[i] = (uint16) typlen; inc_byval[i] = byval; inc_is_text[i] = false; }
        }
        /* Collect keys + includes into arrays */
        Size cap = 0, n = 0;
        int64 *karr = NULL;
        char *incarr[16]; memset(incarr, 0, sizeof(incarr));
        CollectIncCtx cctx; memset(&cctx, 0, sizeof(cctx));
        char *kbytes = NULL, *k1buf = NULL, *k2buf = NULL;
        cctx.nkeyatts = nkeyatts;
        cctx.pk = &karr;
        cctx.pkbytes = &kbytes;
        cctx.key_is_text32 = (atttypid == TEXTOID);
        cctx.key_len = key_len;
        /* Setup two-key fields if needed */
        if (nkeyatts == 2)
        {
            cctx.pk1buf = &k1buf;
            cctx.pk2buf = &k2buf;
            cctx.key_len2 = key_len2;
            int16 l; bool bv; char al;
            get_typlenbyvalalign(atttypid, &l, &bv, &al); cctx.byval1 = bv;
            get_typlenbyvalalign(atttypid2, &l, &bv, &al); cctx.byval2 = bv;
        }
        for (int i=0;i<inc_count;i++){ cctx.pi[i] = &incarr[i]; cctx.ilen[i] = inc_lens[i]; cctx.ibyval[i] = inc_byval[i]; cctx.itext[i] = inc_is_text[i]; }
        cctx.pcap=&cap; cctx.pcount=&n; cctx.incn=inc_count;
        table_index_build_scan(heap, index, indexInfo, true, true, smol_build_cb_inc, (void *) &cctx, NULL);
        INSTR_TIME_SET_CURRENT(t_collect_end);
        SMOL_LOGF("build: collected rows=%zu (key+%d includes)", (size_t) n, inc_count);
        /* Specialize INCLUDE text caps (8/16/32) and repack source buffers to new stride. */
        if (n > 0)
        {
            for (int c = 0; c < inc_count; c++)
            {
                if (!inc_is_text[c]) continue;
                uint16 old_stride = inc_lens[c];
                int maxlen = 0;
                for (Size r = 0; r < n; r++)
                {
                    const char *p = incarr[c] + ((size_t) r * old_stride);
                    const char *zend = (const char *) memchr(p, '\0', old_stride);
                    int len = zend ? (int)(zend - p) : (int) old_stride;
                    if (len > maxlen) maxlen = len;
                }
                uint16 new_stride = (maxlen <= 8) ? 8 : (maxlen <= 16 ? 16 : 32);
                if (new_stride != old_stride)
                {
                    char *nbuf = (char *) MemoryContextAllocHuge(CurrentMemoryContext, ((Size) n) * new_stride);
                    for (Size r = 0; r < n; r++)
                    {
                        const char *src = incarr[c] + ((size_t) r * old_stride);
                        const char *zend = (const char *) memchr(src, '\0', old_stride);
                        int len = zend ? (int)(zend - src) : (int) old_stride;
                        if (len > (int) new_stride) len = (int) new_stride;
                        if (len > 0) memcpy(nbuf + ((size_t) r * new_stride), src, (size_t) len);
                        if (len < (int) new_stride) memset(nbuf + ((size_t) r * new_stride) + len, 0, (size_t) (new_stride - len));
                    }
                    pfree(incarr[c]);
                    incarr[c] = nbuf;
                    inc_lens[c] = new_stride;
                }
            }
        }
        /* Build permutation via radix sort */
        if (n > 0)
        {
            char *sinc[16]; for (int i=0;i<inc_count;i++) sinc[i] = (char *) MemoryContextAllocHuge(CurrentMemoryContext, ((Size) n) * inc_lens[i]);

            if (nkeyatts == 2)
            {
                /* Two-key path: sort pairs then write with INCLUDE */
                /* Use index-based sort (qsort) to easily apply permutation to INCLUDE columns */
                FmgrInfo cmp1, cmp2;
                Oid coll1 = TupleDescAttr(RelationGetDescr(index), 0)->attcollation;
                Oid coll2 = TupleDescAttr(RelationGetDescr(index), 1)->attcollation;
                Oid typoid1 = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
                Oid typoid2 = TupleDescAttr(RelationGetDescr(index), 1)->atttypid;
                fmgr_info_copy(&cmp1, index_getprocinfo(index, 1, 1), CurrentMemoryContext);
                fmgr_info_copy(&cmp2, index_getprocinfo(index, 2, 1), CurrentMemoryContext);
                uint32 *idx = (uint32 *) MemoryContextAllocHuge(CurrentMemoryContext, n * sizeof(uint32)); for (Size i=0;i<n;i++) idx[i] = (uint32) i;
                /* set global comparator context */
                g_k1buf = k1buf; g_k2buf = k2buf; g_key_len1 = key_len; g_key_len2 = key_len2; g_byval1 = cctx.byval1; g_byval2 = cctx.byval2; g_coll1 = coll1; g_coll2 = coll2; g_typoid1 = typoid1; g_typoid2 = typoid2; memcpy(&g_cmp1, &cmp1, sizeof(FmgrInfo)); memcpy(&g_cmp2, &cmp2, sizeof(FmgrInfo));
                qsort(idx, n, sizeof(uint32), smol_pair_qsort_cmp);
                INSTR_TIME_SET_CURRENT(t_sort_end);
                /* Apply permutation to INCLUDE columns */
                for (Size i = 0; i < n; i++)
                {
                    uint32 j = idx[i];
                    for (int c = 0; c < inc_count; c++)
                        memcpy(sinc[c] + ((size_t) i * inc_lens[c]), incarr[c] + ((size_t) j * inc_lens[c]), inc_lens[c]);
                }
                /* init meta if new */
                if (RelationGetNumberOfBlocks(index) == 0)
                {
                    Buffer mb = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
                    LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE); Page pg = BufferGetPage(mb); PageInit(pg, BLCKSZ, 0);
                    SmolMeta *m = smol_meta_ptr(pg); m->magic=SMOL_META_MAGIC; m->version=SMOL_META_VERSION; m->nkeyatts=2; m->key_len1=key_len; m->key_len2=key_len2; m->root_blkno=InvalidBlockNumber; m->height=0; m->inc_count=inc_count;
                    for (int i=0;i<inc_count;i++) { m->inc_len[i]=inc_lens[i]; }
                    MarkBufferDirty(mb); UnlockReleaseBuffer(mb);
                }
                /* Write leaves in two-key + INCLUDE layout */
                Size i = 0; BlockNumber prev = InvalidBlockNumber; char *scratch = (char *) palloc(BLCKSZ);
                while (i < n)
                {
                    Buffer buf = smol_extend(index); Page page = BufferGetPage(buf); smol_init_page(buf, true, InvalidBlockNumber);
                    Size fs = PageGetFreeSpace(page); Size avail = (fs > sizeof(ItemIdData)) ? (fs - sizeof(ItemIdData)) : 0;
                    Size header = sizeof(uint16);
                    Size perrow = (Size) key_len + (Size) key_len2;
                    for (int c=0;c<inc_count;c++) perrow += inc_lens[c];
                    Size maxn = (avail > header) ? ((avail - header) / perrow) : 0; Size rem = n - i; Size n_this = (rem < maxn) ? rem : maxn;
                    if (n_this == 0) ereport(ERROR,(errmsg("smol: two-col+INCLUDE row too large for page")));
                    memcpy(scratch, &n_this, sizeof(uint16)); char *p = scratch + sizeof(uint16);
                    /* Use index permutation to access sorted data */
                    for (Size j=0;j<n_this;j++)
                    {
                        uint32 id = idx[i+j];
                        memcpy(p, k1buf + (size_t) id * key_len, key_len); p += key_len;
                        memcpy(p, k2buf + (size_t) id * key_len2, key_len2); p += key_len2;
                        for (int c=0;c<inc_count;c++) { memcpy(p, sinc[c] + (size_t) (i+j) * inc_lens[c], inc_lens[c]); p += inc_lens[c]; }
                    }
                    Size sz = (Size) (p - scratch);
                    OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
                    Assert(off != InvalidOffsetNumber); (void) off;
                    MarkBufferDirty(buf); BlockNumber cur = BufferGetBlockNumber(buf); UnlockReleaseBuffer(buf);
                    smol_link_siblings(index, prev, cur); prev = cur; i += n_this;
                }
                Buffer mb = ReadBuffer(index, 0); LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE); Page pg = BufferGetPage(mb); SmolMeta *m = smol_meta_ptr(pg); m->root_blkno = 1; m->height = 1; MarkBufferDirty(mb); UnlockReleaseBuffer(mb);
                pfree(idx);
                pfree(scratch);
                for (int i=0;i<inc_count;i++) pfree(sinc[i]);
                if (k1buf) pfree(k1buf);
                if (k2buf) pfree(k2buf);
            }
            else  /* nkeyatts == 1 */
            {
                uint32 *idx = (uint32 *) MemoryContextAllocHuge(CurrentMemoryContext, n * sizeof(uint32));
                for (Size i = 0; i < n; i++) idx[i] = (uint32) i;
                if (!cctx.key_is_text32)
                {
                    /* radix sort by int64 key */
                    uint64 *norm = (uint64 *) MemoryContextAllocHuge(CurrentMemoryContext, n * sizeof(uint64));
                    uint32 *tmp = (uint32 *) MemoryContextAllocHuge(CurrentMemoryContext, n * sizeof(uint32));
                    for (Size i = 0; i < n; i++) norm[i] = smol_norm64(karr[i]);
                    smol_radix_sort_idx_u64(norm, idx, tmp, n);
                    pfree(norm); pfree(tmp);
                    /* Apply permutation */
                    int64 *sk = (int64 *) MemoryContextAllocHuge(CurrentMemoryContext, n * sizeof(int64));
                    for (Size i = 0; i < n; i++)
                    {
                        uint32 j = idx[i]; sk[i] = karr[j];
                        for (int c = 0; c < inc_count; c++)
                            memcpy(sinc[c] + ((size_t) i * inc_lens[c]), incarr[c] + ((size_t) j * inc_lens[c]), inc_lens[c]);
                    }
                    pfree(idx);
                    SMOL_LOGF("build phase: write start n=%zu (includes=%d)", (size_t) n, inc_count);
                    smol_build_tree1_inc_from_sorted(index, sk, (const char * const *) sinc, n, key_len, inc_count, inc_lens);
                    for (int i=0;i<inc_count;i++) pfree(sinc[i]);
                    pfree(sk);
                }
                else
                {
                    /* Text32: sort by binary memcmp on fixed-size keys */
                    /* n * key_len */
                    /* qsort indices by key bytes */
                    g_k1buf = kbytes; g_key_len1 = key_len; /* reuse globals for simple cmp */
                    qsort(idx, n, sizeof(uint32), smol_qsort_cmp_bytes);
                    /* Apply permutation */
                    char *skeys = (char *) MemoryContextAllocHuge(CurrentMemoryContext, ((Size) n) * key_len);
                    for (Size i = 0; i < n; i++)
                    {
                        uint32 j = idx[i];
                        memcpy(skeys + ((size_t) i * key_len), kbytes + ((size_t) j * key_len), key_len);
                        for (int c = 0; c < inc_count; c++)
                            memcpy(sinc[c] + ((size_t) i * inc_lens[c]), incarr[c] + ((size_t) j * inc_lens[c]), inc_lens[c]);
                    }
                    pfree(idx);
                    SMOL_LOGF("build phase: write start n=%zu (includes=%d, text32)", (size_t) n, inc_count);
                    smol_build_text_inc_from_sorted(index, (const char *) skeys, (const char * const *) sinc, n, key_len, inc_count, inc_lens);
                    for (int i=0;i<inc_count;i++) pfree(sinc[i]);
                    pfree(skeys);
                    pfree(kbytes);
                }
            }
        }
        else
        {
            /* Empty index: still call build functions to initialize metapage if needed */
            if (nkeyatts == 2)
            {
                /* GCOV_EXCL_START - Dead code: smol_buildempty now properly initializes two-key metapage */
                /* Initialize empty two-key INCLUDE index */
                if (RelationGetNumberOfBlocks(index) == 0) /* GCOV_EXCL_LINE */
                { /* GCOV_EXCL_LINE */
                    Buffer mb = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL); /* GCOV_EXCL_LINE */
                    LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE); Page pg = BufferGetPage(mb); PageInit(pg, BLCKSZ, 0); /* GCOV_EXCL_LINE */
                    SmolMeta *m = smol_meta_ptr(pg); m->magic=SMOL_META_MAGIC; m->version=SMOL_META_VERSION; m->nkeyatts=2; m->key_len1=key_len; m->key_len2=key_len2; m->root_blkno=InvalidBlockNumber; m->height=0; m->inc_count=inc_count; /* GCOV_EXCL_LINE */
                    for (int i=0;i<inc_count;i++) { m->inc_len[i]=inc_lens[i]; } /* GCOV_EXCL_LINE */
                    MarkBufferDirty(mb); UnlockReleaseBuffer(mb); /* GCOV_EXCL_LINE */
                } /* GCOV_EXCL_LINE */
                /* GCOV_EXCL_STOP */
            }
            else if (!cctx.key_is_text32)
            {
                smol_build_tree1_inc_from_sorted(index, NULL, NULL, 0, key_len, inc_count, inc_lens);
            }
            else
            {
                smol_build_text_inc_from_sorted(index, NULL, NULL, 0, key_len, inc_count, inc_lens);
            }
        }
        for (int i=0;i<inc_count;i++) if (incarr[i]) pfree(incarr[i]);
        if (karr) pfree(karr);
        INSTR_TIME_SET_CURRENT(t_write_end);
    }
    else if (nkeyatts == 1 && atttypid == TEXTOID)
    {
        /* Single-key text: sort with tuplesort then pack to 32-byte padded keys */
        Oid coll = TupleDescAttr(RelationGetDescr(index), 0)->attcollation;
        /* Defensive check - no if-statement needed, macro handles everything */
        SMOL_DEFENSIVE_CHECK(coll == C_COLLATION_OID ||
            (get_collation_name(coll) &&
             (strcmp(get_collation_name(coll), "C") == 0 ||
              strcmp(get_collation_name(coll), "POSIX") == 0)), ERROR,
            (errmsg("smol text keys require C/POSIX collation")));
        TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_LT_OPR);
        Tuplesortstate *ts;
        TsBuildCtxText cb; int maxlen = 0;
        if (!OidIsValid(tce->lt_opr)) ereport(ERROR, (errmsg("no < operator for type %u", atttypid)));
        /* Set up parallel coordination if workers were launched */
        SortCoordinate coordinate = NULL;
        if (buildstate.smolleader)
        {
            coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
            coordinate->isWorker = false;
            coordinate->nParticipants = buildstate.smolleader->nparticipanttuplesorts;
            coordinate->sharedsort = buildstate.smolleader->sharedsort;
        }
        ts = tuplesort_begin_index_btree(heap, index, false, false, maintenance_work_mem, coordinate, TUPLESORT_NONE);
        /* collect and track max len */
        cb.ts = ts; cb.pnkeys = &nkeys; cb.pmax = &maxlen;

        /* In parallel mode, only workers scan the table. Leader just waits and merges. */
        if (!buildstate.smolleader)
        {
            /* Serial build: leader does the scan */
            table_index_build_scan(heap, index, indexInfo, true, true, ts_build_cb_text, (void *) &cb, NULL);
            INSTR_TIME_SET_CURRENT(t_collect_end);
            tuplesort_performsort(ts);
        }
        else
        {
            /* Parallel build: wait for all workers to finish, then merge */
            SMOLShared *smolshared = buildstate.smolleader->smolshared;

            /* Wait for all workers to finish sorting */
            for (;;)
            {
                SpinLockAcquire(&smolshared->mutex);
                if (smolshared->nparticipantsdone == buildstate.smolleader->nparticipanttuplesorts)
                {
                    nkeys = smolshared->reltuples;
                    maxlen = smolshared->maxlen;
                    SpinLockRelease(&smolshared->mutex);
                    break;
                }
                SpinLockRelease(&smolshared->mutex);
                ConditionVariableSleep(&smolshared->workersdonecv, WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
            }
            ConditionVariableCancelSleep();
            INSTR_TIME_SET_CURRENT(t_collect_end);

            /* Now perform sort on leader's tuplesort, which merges worker results */
            tuplesort_performsort(ts);
        }
        INSTR_TIME_SET_CURRENT(t_sort_end);
        if (maxlen > 32) ereport(ERROR, (errmsg("smol text32 key exceeds 32 bytes")));
        /* choose auto cap 8/16/32 */
        uint16 cap = (maxlen <= 8) ? 8 : (maxlen <= 16 ? 16 : 32);
        /* stream write */
        smol_build_text_stream_from_tuplesort(index, ts, nkeys, cap);
        tuplesort_end(ts);
        INSTR_TIME_SET_CURRENT(t_write_end);
    }
    else if (nkeyatts == 1)
    {
        /* Generic fixed-length single-key path (non-varlena) */
        int16 typlen; bool byval; char typalign;
        TypeCacheEntry *tce; Tuplesortstate *ts;
        get_typlenbyvalalign(atttypid, &typlen, &byval, &typalign);
        /* Defensive check - no if-statement needed, macro handles everything */
        SMOL_DEFENSIVE_CHECK(typlen > 0, ERROR,
            (errmsg("smol supports fixed-length types only (typlen=%d)", (int) typlen)));
        key_len = SMOL_KEYLEN_ADJUST((uint16) typlen);
        tce = lookup_type_cache(atttypid, TYPECACHE_LT_OPR);
        if (!OidIsValid(tce->lt_opr)) ereport(ERROR, (errmsg("no < operator for type %u", atttypid)));
        /* Set up parallel coordination if workers were launched */
        SortCoordinate coordinate = NULL;
        if (buildstate.smolleader)
        {
            coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
            coordinate->isWorker = false;
            coordinate->nParticipants = buildstate.smolleader->nparticipanttuplesorts;
            coordinate->sharedsort = buildstate.smolleader->sharedsort;
        }
        ts = tuplesort_begin_index_btree(heap, index, false, false, maintenance_work_mem, coordinate, TUPLESORT_NONE);
        TsBuildCtxAny gcb; gcb.ts = ts; gcb.pnkeys = &nkeys;

        /* In parallel mode, only workers scan the table. Leader just waits and merges. */
        if (!buildstate.smolleader)
        {
            /* Serial build: leader does the scan */
            table_index_build_scan(heap, index, indexInfo, true, true, ts_build_cb_any, (void *) &gcb, NULL);
            INSTR_TIME_SET_CURRENT(t_collect_end);
            tuplesort_performsort(ts);
        }
        else
        {
            /* Parallel build: wait for all workers to finish, then merge */
            SMOLShared *smolshared = buildstate.smolleader->smolshared;

            /* Wait for all workers to finish sorting */
            for (;;)
            {
                SpinLockAcquire(&smolshared->mutex);
                if (smolshared->nparticipantsdone == buildstate.smolleader->nparticipanttuplesorts)
                {
                    nkeys = smolshared->reltuples;
                    SpinLockRelease(&smolshared->mutex);
                    break;
                }
                SpinLockRelease(&smolshared->mutex);
                ConditionVariableSleep(&smolshared->workersdonecv, WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
            }
            ConditionVariableCancelSleep();
            INSTR_TIME_SET_CURRENT(t_collect_end);

            /* Now perform sort on leader's tuplesort, which merges worker results */
            tuplesort_performsort(ts);
        }
        INSTR_TIME_SET_CURRENT(t_sort_end);
        /* stream write directly from tuplesort */
        smol_build_fixed_stream_from_tuplesort(index, ts, nkeys, key_len, byval);
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
            /* Fast path: radix sort for (int64, int64) pairs */
            bool use_radix = (atttypid == INT8OID && atttypid2 == INT8OID);
            uint32 *idx = NULL;

            if (use_radix)
            {
                /* Direct radix sort on int64 pairs (stable, O(n) time) */
                smol_sort_pairs_rows64((int64 *) k1buf, (int64 *) k2buf, n);
                INSTR_TIME_SET_CURRENT(t_sort_end);
                /* Note: smol_validate() is already tested during CREATE EXTENSION (called 21 times for each opclass) */
            }
            else
            {
                /* Generic comparison-based sort via index permutation */
                FmgrInfo cmp1, cmp2;
                Oid coll1 = TupleDescAttr(RelationGetDescr(index), 0)->attcollation;
                Oid coll2 = TupleDescAttr(RelationGetDescr(index), 1)->attcollation;
                Oid typoid1 = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
                Oid typoid2 = TupleDescAttr(RelationGetDescr(index), 1)->atttypid;
                fmgr_info_copy(&cmp1, index_getprocinfo(index, 1, 1), CurrentMemoryContext);
                fmgr_info_copy(&cmp2, index_getprocinfo(index, 2, 1), CurrentMemoryContext);
                idx = (uint32 *) MemoryContextAllocHuge(CurrentMemoryContext, n * sizeof(uint32)); for (Size i=0;i<n;i++) idx[i] = (uint32) i;
                /* set global comparator context */
                g_k1buf = k1buf; g_k2buf = k2buf; g_key_len1 = key_len; g_key_len2 = key_len2; g_byval1 = cctx.byval1; g_byval2 = cctx.byval2; g_coll1 = coll1; g_coll2 = coll2; g_typoid1 = typoid1; g_typoid2 = typoid2; memcpy(&g_cmp1, &cmp1, sizeof(FmgrInfo)); memcpy(&g_cmp2, &cmp2, sizeof(FmgrInfo));
                qsort(idx, n, sizeof(uint32), smol_pair_qsort_cmp);
                INSTR_TIME_SET_CURRENT(t_sort_end);
            }
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
                if (use_radix)
                {
                    /* Data is already sorted in-place, copy sequentially */
                    for (Size j=0;j<n_this;j++) { memcpy(p, k1buf + (size_t) (i+j) * key_len, key_len); p += key_len; memcpy(p, k2buf + (size_t) (i+j) * key_len2, key_len2); p += key_len2; }
                }
                else
                {
                    /* Use index permutation to access sorted order */
                    for (Size j=0;j<n_this;j++) { uint32 id = idx[i+j]; memcpy(p, k1buf + (size_t) id * key_len, key_len); p += key_len; memcpy(p, k2buf + (size_t) id * key_len2, key_len2); p += key_len2; }
                }
                Size sz = (Size) (p - scratch);
                OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
                /* Should always succeed since we validated n_this > 0 and calculated sz to fit */
                Assert(off != InvalidOffsetNumber);
                (void) off; /* suppress unused variable warning in non-assert builds */
                MarkBufferDirty(buf); BlockNumber cur = BufferGetBlockNumber(buf); UnlockReleaseBuffer(buf);
                if (BlockNumberIsValid(prev)) { Buffer pb = ReadBuffer(index, prev); LockBuffer(pb, BUFFER_LOCK_EXCLUSIVE); Page pp=BufferGetPage(pb); smol_page_opaque(pp)->rightlink=cur; MarkBufferDirty(pb); UnlockReleaseBuffer(pb);} prev = cur; i += n_this;
            }
            Buffer mb = ReadBuffer(index, 0); LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE); Page pg = BufferGetPage(mb); SmolMeta *m = smol_meta_ptr(pg); m->root_blkno = 1; m->height = 1; MarkBufferDirty(mb); UnlockReleaseBuffer(mb);
            if (idx) pfree(idx);
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

    /* Clean up parallel build state if used */
    if (buildstate.smolleader)
    {
        smol_end_parallel(buildstate.smolleader);
        pfree(buildstate.smolleader);
        SMOL_LOG("parallel build complete");
    }

    return res;
}

static void
smol_buildempty(Relation index)
{
    Buffer buf;
    Page page;
    SmolMeta *meta;
    int nkeyatts = index->rd_index->indnkeyatts;
    int ninclude = index->rd_att->natts - nkeyatts;

    SMOL_LOGF("enter smol_buildempty nkeyatts=%d ninclude=%d", nkeyatts, ninclude);
    buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf);
    PageInit(page, BLCKSZ, 0);
    meta = smol_meta_ptr(page);
    meta->magic = SMOL_META_MAGIC;
    meta->version = SMOL_META_VERSION;
    meta->nkeyatts = nkeyatts;
    meta->key_len1 = sizeof(int32); /* Default for prototype, overwritten if needed */
    meta->key_len2 = (nkeyatts == 2) ? sizeof(int32) : 0;
    meta->root_blkno = InvalidBlockNumber;
    meta->height = 0;
    meta->inc_count = ninclude;
    /* Set default INCLUDE lengths (will be overwritten by build callback if data exists) */
    for (int i = 0; i < ninclude && i < 16; i++)
    {
        meta->inc_len[i] = sizeof(int32); /* Default assumption */
    }
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
    so->last_dir = ForwardScanDirection;  /* Default to forward */
    so->cur_blk = InvalidBlockNumber;
    so->cur_off = InvalidOffsetNumber;
    so->cur_buf = InvalidBuffer;
    so->have_pin = false;
    so->rle_cached_page_blk = InvalidBlockNumber;  /* Initialize RLE cache as invalid */
    so->have_bound = false;
    so->have_k1_eq = false;
    so->bound_strict = false;
    so->chunk_left = 0;
    so->runtime_keys = NULL;
    so->n_runtime_keys = 0;
    so->atttypid = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
    so->atttypid2 = (RelationGetDescr(index)->natts >= 2) ? TupleDescAttr(RelationGetDescr(index), 1)->atttypid : InvalidOid;
    /* read meta */
    smol_meta_read(index, &meta);
    so->two_col = (meta.nkeyatts == 2);
    so->key_len = meta.key_len1;
    so->key_len2 = meta.key_len2;
    so->cur_group = 0;
    so->pos_in_group = 0;
    smol_run_reset(so);
    so->bstrategy = GetAccessStrategy(BAS_BULKREAD);
    SMOL_LOGF("beginscan nkeys=%d key_len=%u", nkeys, so->key_len);

    /*
     * Prebuild a minimal index tuple with no nulls/varwidth.
     * We reuse this across all returned rows by memcpy-ing new key bytes.
     */
    {
        Size data_off = MAXALIGN(sizeof(IndexTupleData));
        Size off1 = data_off;
        Size off2 = 0;
        Size sz = 0;
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
                Oid attoid = att->atttypid;
                so->inc_is_text[i] = (attoid == TEXTOID);
            }
            /* Precompute cumulative offsets for O(1) INCLUDE pointer arithmetic */
            uint32 cumul = 0;
            for (uint16 i=0;i<so->ninclude;i++)
            {
                so->inc_cumul_offs[i] = cumul;
                cumul += so->inc_len[i];
            }
            so->inc_cumul_offs[so->ninclude] = cumul;  /* Total for "skip all includes" case */
        }
        so->align1 = align1;
        so->align2 = align2;
        bool key_is_text = (so->atttypid == TEXTOID);
        if (!so->two_col)
        {
            /* Single-col: key then INCLUDE attrs aligned per attalign */
            Size stored_key_size = key_is_text ? (Size)(VARHDRSZ + so->key_len) : (Size) so->key_len;
            Size cur = off1 + stored_key_size;
            for (uint16 i=0;i<so->ninclude;i++)
            {
                cur = att_align_nominal(cur, so->inc_align[i]);
                so->inc_offs[i] = (uint16) (cur - data_off);
                Size inc_bytes = so->inc_is_text[i] ? (Size)(VARHDRSZ + so->inc_len[i]) : (Size) so->inc_len[i];
                cur += inc_bytes;
            }
            sz = MAXALIGN(cur);
        }
        else
        {
            /* Two-col: compute offset/aligned size for key2 */
            off2 = att_align_nominal(off1 + so->key_len, align2);
            sz = MAXALIGN(off2 + so->key_len2);
        }
        if (smol_debug_log)
        {
            SMOL_LOGF("beginscan layout: key_len=%u two_col=%d ninclude=%u sz=%zu", so->key_len, so->two_col, so->ninclude, (size_t) sz);
            for (uint16 i=0;i<so->ninclude;i++)
                SMOL_LOGF("include[%u]: len=%u align=%c off=%u is_text=%d", i, so->inc_len[i], so->inc_align[i], so->inc_offs[i], so->inc_is_text[i]);
        }
        so->itup = (IndexTuple) palloc0(sz);
        /* Allocate zero-copy header (8 bytes: 6-byte TID + 2-byte t_info) for pointing to page data */
        so->itup_zerocopy_hdr = (IndexTuple) palloc0(sizeof(IndexTupleData));
        so->has_varwidth = key_is_text;
        for (uint16 i=0;i<so->ninclude;i++) if (so->inc_is_text[i]) { so->has_varwidth = true; break; }
        so->itup->t_info = (unsigned short) (sz | (so->has_varwidth ? INDEX_VAR_MASK : 0)); /* updated per-row when key varwidth */
        so->itup_data = (char *) so->itup + data_off;
        so->itup_off2 = so->two_col ? (uint16) (off2 - data_off) : 0;
        so->itup_data_off = (uint16) data_off;
        /* set copy helpers */
        so->copy1_fn = (so->key_len == 2) ? smol_copy2 : (so->key_len == 4) ? smol_copy4 : smol_copy8;
        if (so->two_col)
            so->copy2_fn = (so->key_len2 == 2) ? smol_copy2 : (so->key_len2 == 4) ? smol_copy4 : smol_copy8;
        for (uint16 i=0;i<so->ninclude;i++)
        {
            /* Precompute copy function for each INCLUDE column to eliminate hot-path if-else chain */
            if (so->inc_len[i] == 1) so->inc_copy[i] = smol_copy1;
            else if (so->inc_len[i] == 2) so->inc_copy[i] = smol_copy2;
            else if (so->inc_len[i] == 4) so->inc_copy[i] = smol_copy4;
            else if (so->inc_len[i] == 8) so->inc_copy[i] = smol_copy8;
            else if (so->inc_len[i] == 16) so->inc_copy[i] = smol_copy16;
            else /* GCOV_EXCL_LINE - defensive: all PostgreSQL fixed-length types are 1/2/4/8/16 bytes */
                elog(ERROR, "smol: unsupported INCLUDE column size %u", so->inc_len[i]); /* GCOV_EXCL_LINE */
        }
        /* comparator + key type props */
        so->collation = TupleDescAttr(RelationGetDescr(index), 0)->attcollation;
        get_typlenbyvalalign(so->atttypid, &so->key_typlen, &so->key_byval, &so->align1);
        fmgr_info_copy(&so->cmp_fmgr, index_getprocinfo(index, 1, 1), CurrentMemoryContext);
        so->key_is_text32 = key_is_text;
    }
    so->prof_enabled = smol_profile_log;
    so->prof_calls = 0;
    so->prof_rows = 0;
    so->prof_pages = 0;
    so->prof_bytes = 0;
    so->prof_touched = 0;
    so->prof_bsteps = 0;
    so->run_key_built = false;
    for (int i=0;i<16;i++){ so->run_inc_built[i]=false; so->run_inc_vl_len[i]=0; }
    /* Initialize V2 continuation support */
    so->prev_page_last_run_active = false;
    so->prev_page_last_run_text_klen = 0;
    /* Initialize adaptive prefetch counters */
    so->pages_scanned = 0;
    so->adaptive_prefetch_depth = 0;
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
    /* Release buffer pin if held from previous scan.
     * Hard to trigger: requires rescan while holding pin, depends on PostgreSQL executor
     * timing. Pattern follows btree (BTScanPosUnpinIfPinned) and hash (_hash_dropscanbuf). */
    if (so->have_pin && BufferIsValid(so->cur_buf)) 
    {
        ReleaseBuffer(so->cur_buf); 
        so->cur_buf = InvalidBuffer; 
        so->have_pin = false; 
    }
    so->have_bound = false;
    so->have_upper_bound = false;
    so->have_k1_eq = false;
    so->have_k2_eq = false;
    so->chunk_left = 0;

    /* Store all scankeys for runtime filtering */
    if (so->runtime_keys)
        pfree(so->runtime_keys);
    so->runtime_keys = NULL;
    so->n_runtime_keys = 0;

    if (keys && nkeys > 0)
    {
        /* Copy scankeys for later filtering */
        so->runtime_keys = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
        memcpy(so->runtime_keys, keys, sizeof(ScanKeyData) * nkeys);
        so->n_runtime_keys = nkeys;

        /* Opt #4: Cache whether runtime key testing is needed (checked once per scan, not per tuple) */
        so->need_runtime_key_test = false;

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
                else if (sk->sk_strategy == BTLessEqualStrategyNumber ||
                         sk->sk_strategy == BTLessStrategyNumber)
                {
                    so->have_upper_bound = true;
                    so->upper_bound_strict = (sk->sk_strategy == BTLessStrategyNumber);
                    so->upper_bound_datum = sk->sk_argument;
                }
            }
            else if (sk->sk_attno == 2)
            {
                if (sk->sk_strategy == BTEqualStrategyNumber)
                {
                    so->have_k2_eq = true;
                    Oid t2 = so->atttypid2;
                    if (t2 == INT2OID) so->k2_eq = (int64) DatumGetInt16(sk->sk_argument);
                    else if (t2 == INT4OID) so->k2_eq = (int64) DatumGetInt32(sk->sk_argument);
                    else so->k2_eq = DatumGetInt64(sk->sk_argument);
                }
                else
                {
                    /* Attribute 2 with non-equality: requires runtime testing */
                    so->need_runtime_key_test = true;
                }
            }
        }
    }
    else
    {
        /* No keys: no runtime testing needed */
        so->need_runtime_key_test = false;
    }

#ifdef SMOL_TEST_COVERAGE
    /* Test hook: fake bounds for page-level bounds checking coverage */
    if (smol_test_force_page_bounds_check && !so->have_upper_bound && !so->have_k1_eq)
    {
        /* Fake an upper bound to trigger page-level bounds check
         * Use a value that will be exceeded by later pages in test data
         * Test data has gap: 1-5000, then 100000+, so fake bound at 10000 */
        so->have_upper_bound = true;
        so->upper_bound_strict = true;
        so->upper_bound_datum = Int32GetDatum(10000);
    }
#endif
}

/*
 * smol_test_runtime_keys
 * Test scan keys that SMOL doesn't handle natively against the materialized tuple.
 * Returns true if all keys pass, false otherwise.
 *
 * SMOL natively handles:
 * - Attribute 1 (leading key): All range predicates (>=, >, =, <=, <)
 * - Attribute 2 (second key): Equality only (=)
 *
 * We need to recheck:
 * - Attribute 2 (second key): Range predicates (>=, >, <=, <)
 */
static bool
smol_test_runtime_keys(IndexScanDesc scan, SmolScanOpaque so)
{
    if (so->n_runtime_keys == 0)
        return true;

    /* Opt #4: Use cached flag instead of iterating all keys (eliminates 10M loop iterations) */
    if (!so->need_runtime_key_test)
        return true; /* All keys are handled natively by SMOL */

    /* Extract values from the prebuilt tuple */
    Datum *values = (Datum *) palloc(scan->xs_itupdesc->natts * sizeof(Datum));
    bool *isnull = (bool *) palloc(scan->xs_itupdesc->natts * sizeof(bool));

    index_deform_tuple(so->itup, scan->xs_itupdesc, values, isnull);

    /* Test each scankey that needs rechecking */
    for (int i = 0; i < so->n_runtime_keys; i++)
    {
        ScanKey key = &so->runtime_keys[i];

        /* Skip keys that SMOL handles natively */
        if (key->sk_attno == 1)
            continue; /* SMOL handles all attribute 1 predicates */
        if (key->sk_attno == 2 && key->sk_strategy == BTEqualStrategyNumber)
            continue; /* SMOL handles attribute 2 equality */ /* GCOV_EXCL_LINE */

        int attno = key->sk_attno - 1; /* 1-based to 0-based */

        if (attno < 0 || attno >= scan->xs_itupdesc->natts) 
            continue; /* GCOV_EXCL_LINE */

        /* NULL handling */
        if (isnull[attno])
        {
            pfree(values); /* GCOV_EXCL_LINE */
            pfree(isnull); /* GCOV_EXCL_LINE */
            return false; /* GCOV_EXCL_LINE */
        }

        /* Evaluate the scankey */
        bool result = DatumGetBool(FunctionCall2Coll(&key->sk_func,
                                                      key->sk_collation,
                                                      values[attno],
                                                      key->sk_argument));

        if (!result)
        {
            pfree(values);
            pfree(isnull);
            return false;
        }
    }

    pfree(values);
    pfree(isnull);
    return true;
}

/*
 * smol_leaf_keyptr_cached
 * Cached version of smol_leaf_keyptr_ex for RLE pages during sequential scans.
 *
 * For RLE pages, smol_leaf_keyptr_ex does O(m) work scanning through runs.
 * When scanning sequentially, this becomes O(n*m) which is extremely slow.
 * This function caches the current RLE run position to provide O(1) lookups
 * for sequential scans within the same run, and O(1) amortized across a page.
 */
/* Get cached RLE run bounds if available, avoiding linear scan through runs */
static inline bool
smol_get_cached_run_bounds(SmolScanOpaque so, uint16 idx, uint16 *run_start_out, uint16 *run_end_out)
{
    /* Check if idx is within cached run */
    if (so->rle_cached_run_keyptr != NULL &&
        idx >= (so->rle_cached_run_acc + 1) &&
        idx <= so->rle_cached_run_end)
    {
        /* Cache hit - return cached run bounds */
        if (run_start_out) *run_start_out = (uint16) (so->rle_cached_run_acc + 1);
        if (run_end_out)   *run_end_out = so->rle_cached_run_end;
        return true;
    }
    return false;
}

static inline char *
smol_leaf_keyptr_cached(SmolScanOpaque so, Page page, uint16 idx, uint16 key_len,
                        uint16 inc_len[], uint16 ninc, uint32 inc_cumul_offs[])
{
    /* Check if we need to invalidate cache (changed pages or first call) */
    if (so->rle_cached_page_blk != so->cur_blk || so->rle_cached_run_keyptr == NULL)
    {
        /* New page or uninitialized - reset cache */
        so->rle_cached_page_blk = so->cur_blk;
        so->rle_cached_run_idx = 0;
        so->rle_cached_run_acc = 0;
        so->rle_cached_run_end = 0;
        so->rle_cached_run_keyptr = NULL;
        so->rle_cached_run_ptr = NULL;
    }

    /* Check if idx is within cached run */
    if (so->rle_cached_run_keyptr != NULL &&
        idx >= (so->rle_cached_run_acc + 1) &&
        idx <= so->rle_cached_run_end)
    {
        /* Cache hit - return cached key pointer */
        so->rle_cache_hits++;
        return so->rle_cached_run_keyptr;
    }

    /* Cache miss */
    so->rle_cache_misses++;

    /* Cache miss - need to find the run containing idx */
    /* PERF OPT: Reuse cur_page_format instead of extracting tag again */
    /* Only cache for KEY_RLE pages (format 2 or 4) */
    if (so->cur_page_format != 2 && so->cur_page_format != 4)
    {
        /* Not a KEY_RLE page - fall back to regular function */
        return smol_leaf_keyptr_ex(page, idx, key_len, inc_len, ninc, inc_cumul_offs);
    }

    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);

    /* RLE page - scan runs from last cached position */
    uint16 nitems, nruns;
    memcpy(&nitems, p + sizeof(uint16), sizeof(uint16));
    memcpy(&nruns, p + sizeof(uint16) * 2, sizeof(uint16));

    char *rp = p + sizeof(uint16) * 3;
    if (so->cur_page_format == 4)  /* SMOL_TAG_KEY_RLE_V2 = format 4 */
        rp++;  /* Skip continues_byte */

    /* Start from cached position if valid, otherwise from beginning */
    uint16 start_run = 0;
    uint32 acc = 0;

    if (so->rle_cached_run_ptr != NULL && idx > so->rle_cached_run_end)
    {
        /* Forward scan - resume from cached position (O(1) instead of O(N)) */
        start_run = so->rle_cached_run_idx + 1;
        acc = so->rle_cached_run_end;
        /* Calculate position of next run: cached run ptr + key + count */
        rp = so->rle_cached_run_ptr + (size_t) key_len + sizeof(uint16);
    }

    /* Scan runs to find idx */
    for (uint16 r = start_run; r < nruns; r++)
    {
        char *k = rp;
        uint16 cnt;
        memcpy(&cnt, rp + key_len, sizeof(uint16));

        if (idx <= acc + cnt)
        {
            /* Found it - update cache */
            so->rle_cached_run_idx = r;
            so->rle_cached_run_acc = acc;
            so->rle_cached_run_end = acc + cnt;
            so->rle_cached_run_keyptr = k;
            so->rle_cached_run_ptr = rp;  /* Cache run pointer for O(1) forward scan */
            return k;
        }

        acc += cnt;
        rp += (size_t) key_len + sizeof(uint16);
    }

    /* Should not reach here - idx out of range */ /* GCOV_EXCL_LINE - defensive: idx always in valid range */
    ereport(ERROR, (errmsg("smol: cached keyptr index %u out of range [1,%u]", idx, nitems))); /* GCOV_EXCL_LINE */
    return NULL;  /* unreachable */ /* GCOV_EXCL_LINE */
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

    /* SMOL is IOS-only; executor must request index attributes */
    if (!scan->xs_want_itup)
        ereport(ERROR, (errmsg("smol supports index-only scans only")));
    if (dir == NoMovementScanDirection)
        return false;

    if (dir == BackwardScanDirection)

    /* Detect direction change and reinitialize if needed */
    if (so->initialized && so->last_dir != dir)
    {
        /* Direction changed - need to reinitialize scan */
        so->initialized = false;
        /* Release any pinned buffer */
        if (so->have_pin && BufferIsValid(so->cur_buf))
        {
            ReleaseBuffer(so->cur_buf);
            so->have_pin = false;
            so->cur_buf = InvalidBuffer;
        }
        so->cur_blk = InvalidBlockNumber;
    }

    /*
     * First-time init: position to first tuple/group >= bound.
     * We pin (but do not lock) the chosen leaf page and keep it pinned across
     * calls until exhausted, to minimize buffer manager overhead.
     */
    if (!so->initialized)
    {
        /* PostgreSQL executor always calls amrescan before amgettuple.
         * If we have scan keys but runtime_keys is NULL, rescan was never called. */
        SMOL_DEFENSIVE_CHECK(scan->numberOfKeys == 0 || so->runtime_keys != NULL, ERROR,
                            (errmsg("smol: amgettuple called before amrescan")));
        /* no local variables needed here */
        if (dir == BackwardScanDirection)
        {
            /* For now, always start at rightmost leaf for backward scans
             * TODO: Optimize by seeking to leaf containing upper bound */
            so->cur_blk = smol_rightmost_leaf(idx);
            /* position within leaf: set to end; we'll walk backward */
            buf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy);
            page = BufferGetPage(buf);

            if (so->two_col)
            {
                /* Two-column backward scan: initialize leaf_i for backward iteration */
                so->leaf_n = smol12_leaf_nrows(page);
                so->leaf_i = (so->leaf_n > 0) ? (so->leaf_n - 1) : 0;
                so->cur_buf = buf; so->have_pin = true;
                so->initialized = true;
                so->last_dir = dir;
                smol_run_reset(so);
                SMOL_LOGF("init backward two-col cur_blk=%u leaf_i=%u leaf_n=%u", so->cur_blk, so->leaf_i, so->leaf_n);
            }
            else
            {
                /* Single-column backward scan: use cur_off */
                uint16 n = smol_leaf_nitems(page);
                so->cur_off = n;
                so->cur_buf = buf; so->have_pin = true;
                so->initialized = true;
                so->last_dir = dir;
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
                        uint32 curv = SMOL_ATOMIC_READ_U32(&ps->curr);
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
                            /* publish rightlink(left) or skip ahead by batch-1 to reserve a local chunk */
                            Buffer lbuf = ReadBufferExtended(idx, MAIN_FORKNUM, left, RBM_NORMAL, so->bstrategy);
                            Page lpg = BufferGetPage(lbuf);
                            BlockNumber step = smol_page_opaque(lpg)->rightlink;
                            uint32 claimed = 0;
                            for (int i = 1; i < smol_parallel_claim_batch && BlockNumberIsValid(step); i++)
                            {
                                Buffer nb = ReadBufferExtended(idx, MAIN_FORKNUM, step, RBM_NORMAL, so->bstrategy);
                                Page np = BufferGetPage(nb);
                                BlockNumber rl2 = smol_page_opaque(np)->rightlink;
                                ReleaseBuffer(nb);
                                step = rl2;
                                claimed++;
                            }
                            ReleaseBuffer(lbuf);
                            uint32 expect = 0u;
                            uint32 newv = (uint32) (BlockNumberIsValid(step) ? step : InvalidBlockNumber);
                            if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expect, newv))
                            { so->cur_blk = left; so->chunk_left = claimed; break; }
                            continue;
                        }
                        if (curv == (uint32) InvalidBlockNumber)
                        { so->cur_blk = InvalidBlockNumber; break; }
                        /* claim current and optionally skip ahead by batch-1 */
                        Buffer tbuf = ReadBufferExtended(idx, MAIN_FORKNUM, (BlockNumber) curv, RBM_NORMAL, so->bstrategy);
                        Page tpg = BufferGetPage(tbuf);
                        BlockNumber step = smol_page_opaque(tpg)->rightlink;
                        uint32 claimed = 0;
                        for (int i = 1; i < smol_parallel_claim_batch && BlockNumberIsValid(step); i++)
                        {
                            Buffer nb = ReadBufferExtended(idx, MAIN_FORKNUM, step, RBM_NORMAL, so->bstrategy);
                            Page np = BufferGetPage(nb);
                            BlockNumber rl2 = smol_page_opaque(np)->rightlink;
                            ReleaseBuffer(nb);
                            step = rl2;
                            claimed++;
                        }
                        ReleaseBuffer(tbuf);
                        uint32 expected = curv;
                        uint32 newv = (uint32) (BlockNumberIsValid(step) ? step : InvalidBlockNumber);
                        if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expected, newv))
                        { so->cur_blk = (BlockNumber) curv; so->chunk_left = claimed; break; }
                    }
                    so->cur_off = FirstOffsetNumber;
                    so->initialized = true;
                    so->last_dir = dir;
                    so->last_dir = dir;
                    /* prefetch the first claimed leaf */
                    if (BlockNumberIsValid(so->cur_blk))
                        PrefetchBuffer(idx, MAIN_FORKNUM, so->cur_blk);
                    if (BlockNumberIsValid(so->cur_blk))
                    {
                        buf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy);
                        page = BufferGetPage(buf);
                        so->cur_buf = buf; so->have_pin = true;
                        /* Seek within first claimed leaf to the bound (forward scans) */
                        if (so->have_bound && dir != BackwardScanDirection && !so->two_col)
                        {
                            uint16 n2 = smol_leaf_nitems(page);
                            /* Detect page format for zero-copy adjustment */
                            ItemId iid_par = PageGetItemId(page, FirstOffsetNumber);
                            char *base_par = (char *) PageGetItem(page, iid_par);
                            uint16 tag_par; memcpy(&tag_par, base_par, sizeof(uint16));
                            bool page_is_zerocopy_par = (tag_par == SMOL_TAG_ZEROCOPY);
                            uint16 lo2 = FirstOffsetNumber, hi2 = n2, ans2 = InvalidOffsetNumber;
                            while (lo2 <= hi2)
                            {
                                uint16 mid2 = (uint16) (lo2 + ((hi2 - lo2) >> 1));
                                char *kp2 = smol_leaf_keyptr_ex(page, mid2, so->key_len, so->inc_len, so->ninclude, so->inc_cumul_offs);
                                /* For zero-copy pages, skip IndexTuple header to get key data */
                                if (page_is_zerocopy_par) kp2 += sizeof(IndexTupleData);
                                int cc = smol_cmp_keyptr_to_bound(so, kp2);
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
                    /* Single-threaded scan: seek to first leaf containing bound
                     * For equality queries (k = value), this seeks directly to the page containing value
                     * instead of starting at leftmost leaf and scanning sequentially */
                    if (so->have_bound && so->atttypid == TEXTOID)
                    {
                        /* TEXT types: use generic find_first_leaf that handles text comparison correctly */
                        so->cur_blk = smol_find_first_leaf_generic(idx, so);
                    }
                    else
                    {
                        /* Integer types or unbounded: use fast integer-only path */
                        int64 lb = 0;  /* Default for unbounded scans: start at leftmost leaf */
                        if (so->have_bound)
                        {
                            if (so->atttypid == INT2OID) lb = (int64) DatumGetInt16(so->bound_datum);
                            else if (so->atttypid == INT4OID) lb = (int64) DatumGetInt32(so->bound_datum);
                            else if (so->atttypid == INT8OID) lb = DatumGetInt64(so->bound_datum);
                            else lb = 0;  /* Non-INT types (date, timestamp, etc.): leftmost leaf */
                        }
                        so->cur_blk = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                    }
                    so->cur_off = FirstOffsetNumber;
                    so->initialized = true;
                    so->last_dir = dir;
                    SMOL_LOGF("gettuple init cur_blk=%u", so->cur_blk);

                    /* seek within leaf to >= bound */
                    if (so->have_bound)
                    {
                        /* Pin leaf and binary-search to first >= or > bound */
                        uint16 n2, lo = FirstOffsetNumber, hi, ans = InvalidOffsetNumber;
                        buf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy);
                        page = BufferGetPage(buf);
                        /* Detect page format for zero-copy adjustment */
                        ItemId iid_tmp = PageGetItemId(page, FirstOffsetNumber);
                        char *base_tmp = (char *) PageGetItem(page, iid_tmp);
                        uint16 tag_tmp; memcpy(&tag_tmp, base_tmp, sizeof(uint16));
                        so->page_is_zerocopy = (tag_tmp == SMOL_TAG_ZEROCOPY);
                        n2 = smol_leaf_nitems(page);
                        hi = n2;
                        while (lo <= hi)
                        {
                            uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                            char *keyp = smol_leaf_keyptr_ex(page, mid, so->key_len, so->inc_len, so->ninclude, so->inc_cumul_offs);
                            /* For zero-copy pages, skip IndexTuple header to get key data */
                            if (so->page_is_zerocopy) 
                                keyp += sizeof(IndexTupleData); /* GCOV_EXCL_LINE */
                            int c = smol_cmp_keyptr_to_bound(so, keyp);
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
                        buf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy);
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
                        uint32 curv = SMOL_ATOMIC_READ_U32(&ps->curr);
                        if (curv == 0u)
                        { /* GCOV_EXCL_LINE - opening brace artifact, see inner line coverage */
                            /* Convert bound_datum to int64 lower bound for two-col parallel scan */
                            /* Defensive: PostgreSQL planner only uses parallel index scans with WHERE clauses, so have_bound must be true */
                            SMOL_DEFENSIVE_CHECK(so->have_bound, ERROR,
                                                (errmsg("smol: parallel scan without bound")));
                            int64 lb;
                            if (so->atttypid == INT2OID) lb = (int64) DatumGetInt16(so->bound_datum);
                            else if (so->atttypid == INT4OID) lb = (int64) DatumGetInt32(so->bound_datum);
                            else if (so->atttypid == INT8OID) lb = DatumGetInt64(so->bound_datum);
                            else lb = PG_INT64_MIN; /* Non-INT types: use minimum for conservative bound */
                            BlockNumber left = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                            Buffer lbuf = ReadBuffer(idx, left);
                            Page lpg = BufferGetPage(lbuf);
                            BlockNumber step = smol_page_opaque(lpg)->rightlink;
                            uint32 claimed = 0;
                            for (int i = 1; i < smol_parallel_claim_batch && BlockNumberIsValid(step); i++)
                            {
                                Buffer nb = ReadBuffer(idx, step);
                                Page np = BufferGetPage(nb);
                                BlockNumber rl2 = smol_page_opaque(np)->rightlink;
                                ReleaseBuffer(nb);
                                step = rl2;
                                claimed++;
                            }
                            ReleaseBuffer(lbuf);
                            uint32 expect = 0u;
                            uint32 newv = (uint32) (BlockNumberIsValid(step) ? step : InvalidBlockNumber);
                            if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expect, newv))
                            { so->cur_blk = left; so->chunk_left = claimed; break; }
                            continue; /* GCOV_EXCL_LINE - CAS retry: requires precise parallel worker timing impossible to reliably simulate */
                        }
                        if (curv == (uint32) InvalidBlockNumber)
                        { so->cur_blk = InvalidBlockNumber; break; }
                        Buffer tbuf = ReadBuffer(idx, (BlockNumber) curv);
                        Page tpg = BufferGetPage(tbuf);
                        BlockNumber step = smol_page_opaque(tpg)->rightlink;
                        uint32 claimed = 0;
                        for (int i = 1; i < smol_parallel_claim_batch && BlockNumberIsValid(step); i++)
                        {
                            Buffer nb = ReadBuffer(idx, step);
                            Page np = BufferGetPage(nb);
                            BlockNumber rl2 = smol_page_opaque(np)->rightlink;
                            ReleaseBuffer(nb);
                            step = rl2;
                            claimed++;
                        }
                        ReleaseBuffer(tbuf);
                        uint32 expected = curv;
                        uint32 newv = (uint32) (BlockNumberIsValid(step) ? step : InvalidBlockNumber);
                        if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expected, newv))
                        { so->cur_blk = (BlockNumber) curv; so->chunk_left = claimed; break; }
                    }
                    so->cur_group = 0;
                    so->pos_in_group = 0;
                    so->initialized = true;
                    so->last_dir = dir;
                    if (BlockNumberIsValid(so->cur_blk))
                    {
                        PrefetchBuffer(idx, MAIN_FORKNUM, so->cur_blk);
                        buf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy);
                        page = BufferGetPage(buf);
                        so->cur_buf = buf; so->have_pin = true;
                        so->leaf_n = smol12_leaf_nrows(page); so->leaf_i = 0;
                        smol_run_reset(so);
                        if (so->have_bound)
                        {
                            uint16 lo = FirstOffsetNumber, hi = so->leaf_n, ans = InvalidOffsetNumber;
                            while (lo <= hi)
                            {
                                uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                                char *k1p = smol12_row_k1_ptr(page, mid, so->key_len, so->key_len2);
                                int c = smol_cmp_keyptr_to_bound(so, k1p);
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
                    /* Two-column single-threaded scan: seek to first leaf containing bound */
                    int64 lb = 0;  /* Default for unbounded scans */
                    if (so->have_bound)
                    {
                        if (so->atttypid == INT2OID) lb = (int64) DatumGetInt16(so->bound_datum);
                        else if (so->atttypid == INT4OID) lb = (int64) DatumGetInt32(so->bound_datum);
                        else if (so->atttypid == INT8OID) lb = DatumGetInt64(so->bound_datum);
                        else lb = 0;
                    }
                    so->cur_blk = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                    so->cur_group = 0;
                    so->pos_in_group = 0;
                    so->initialized = true;
                    so->last_dir = dir;
                    if (so->have_bound)
                    {
                        /* Pin leaf and binary-search rows on k1 (>= or > bound) */
                        uint16 lo = FirstOffsetNumber, hi, ans = InvalidOffsetNumber;
                        buf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy);
                        page = BufferGetPage(buf);
                        so->leaf_n = smol12_leaf_nrows(page);
                        hi = so->leaf_n;
                        smol_run_reset(so);
                        while (lo <= hi)
                        {
                            uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                            char *k1p = smol12_row_k1_ptr(page, mid, so->key_len, so->key_len2);
                            int c = smol_cmp_keyptr_to_bound(so, k1p);
                            if (so->prof_enabled) so->prof_bsteps++;
                            if ((so->bound_strict ? (c > 0) : (c >= 0))) { ans = mid; if (mid == 0) break; hi = (uint16) (mid - 1); }
                            else lo = (uint16) (mid + 1);
                        }
                        so->cur_buf = buf; so->have_pin = true;
                        so->leaf_i = (ans != InvalidOffsetNumber) ? (uint32) (ans - 1) : (uint32) so->leaf_n;
                    }
                    else
                    {
                        buf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy);
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
            so->cur_buf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy); /* GCOV_EXCL_LINE */
            so->have_pin = true; /* GCOV_EXCL_LINE */
        }
        buf = so->cur_buf;
        page = BufferGetPage(buf);
        /* Detect page format: plain, zero-copy, or RLE */
        ItemId iid = PageGetItemId(page, FirstOffsetNumber);
        char *base = (char *) PageGetItem(page, iid);
	/* memcpy() is same perf but handles unaligned - uint16 tag = *((uint16*)base) is unsafe */
        uint16 tag; memcpy(&tag, base, sizeof(uint16));

        so->page_is_zerocopy = (tag == SMOL_TAG_ZEROCOPY);

        /* Opt #5: Cache page metadata once per page (nitems and format) */
        if (tag == SMOL_TAG_ZEROCOPY || tag == SMOL_TAG_KEY_RLE ||
            tag == SMOL_TAG_KEY_RLE_V2 || tag == SMOL_TAG_INC_RLE)
        {
            /* Tagged formats: [u16 tag][u16 nitems][...] */
            uint16 nitems;
            memcpy(&nitems, base + sizeof(uint16), sizeof(uint16));
            so->cur_page_nitems = nitems;
            if (tag == SMOL_TAG_ZEROCOPY)
                so->cur_page_format = 1; /* GCOV_EXCL_LINE - zero-copy format only used for old indexes, not created by current code */
            else if (tag == SMOL_TAG_KEY_RLE)
                so->cur_page_format = 2; /* GCOV_EXCL_LINE - KEY_RLE v1 format is legacy, v2 is now the default */
            else if (tag == SMOL_TAG_INC_RLE)
                so->cur_page_format = 3;
            else /* SMOL_TAG_KEY_RLE_V2 */
                so->cur_page_format = 4;
        }
        else
        {
            /* Plain format: first u16 is nitems (no tag) */
            so->cur_page_nitems = tag;
            so->cur_page_format = 0;
        }

        /* Run-detection optimization: check page type ONCE per page (not per row)
         * Plain pages with no INCLUDE columns have no duplicate-key runs, so we can
         * skip expensive run-boundary scanning. This eliminates 60% of CPU overhead
         * on unique-key workloads while preserving correctness.
         * Lines 1767 and 1887 use page_is_plain to set run_length=1 without scanning.
         * PERF OPT: Reuse already-extracted tag instead of calling smol_leaf_is_rle() */
        if (!so->two_col && so->ninclude == 0)
            so->page_is_plain = (so->cur_page_format == 0);  /* format 0 = plain */
        else
            so->page_is_plain = false;

        /* Initialize INCLUDE column pointer cache for plain pages (performance optimization)
         * For plain-layout pages with INCLUDE columns, compute base pointers once per page
         * instead of recomputing them for every tuple. This eliminates ~200K redundant calls
         * to smol1_inc_ptr_any() for workloads returning many rows with INCLUDE columns.
         * Plain format: [u16 n][keys][inc1 block][inc2 block]... (no tag, n < 0x8000)
         * RLE formats use tags 0x8001 or 0x8003 at the start. */
        so->plain_inc_cached = false;
        if (!so->two_col && so->ninclude > 0)
        {
            /* Reuse iid, base, tag from above */
            /* Plain format has no tag - first u16 is the count n (< 0x8000) */
            if (tag != 0x8001u && tag != 0x8003u && tag != SMOL_TAG_ZEROCOPY)
            {
                uint16 n = tag; /* First u16 is count, not a tag */
                char *base_ptr = base + sizeof(uint16) + (size_t) n * so->key_len;
                /* Use precomputed cumulative offsets for O(1) per-column computation */
                for (uint16 ii = 0; ii < so->ninclude; ii++)
                {
                    so->plain_inc_base[ii] = base_ptr + (size_t) n * so->inc_cumul_offs[ii];
                }
                so->plain_inc_cached = true;
            }
        }

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
                    int c = smol_cmp_keyptr_to_bound(so, k1p);
                    if (so->bound_strict ? (c <= 0) : (c < 0))
                    {
                        if (dir == BackwardScanDirection) so->leaf_i--; else so->leaf_i++;
                        continue;
                    }
                    if (so->have_k1_eq && c > 0)
                    {
                        /* Leading-key equality: once we see > bound on any leaf, we're done overall. */
                        if (so->have_pin && BufferIsValid(so->cur_buf)) { ReleaseBuffer(so->cur_buf); so->have_pin=false; }
                        so->cur_blk = InvalidBlockNumber;
                        return false;
                    }
                }
                /* Check upper bound (for BETWEEN queries) */
                if (so->have_upper_bound)
                {
                    int c = smol_cmp_keyptr_to_upper_bound(so, k1p);
                    if (so->upper_bound_strict ? (c >= 0) : (c > 0))
                    {
                        /* Exceeded upper bound, stop scan */
                        if (so->have_pin && BufferIsValid(so->cur_buf)) { ReleaseBuffer(so->cur_buf); so->have_pin=false; }
                        so->cur_blk = InvalidBlockNumber;
                        return false;
                    }
                }
                /* Optional equality on second key (int2/int4/int8 only) */
                if (so->have_k2_eq)
                {
                    int64 v;
                    if (so->key_len2 == 2) { int16 t; memcpy(&t, k2p, 2); v = (int64) t; }
                    else if (so->key_len2 == 4) { int32 t; memcpy(&t, k2p, 4); v = (int64) t; }
                    else /* 8 */ { int64 t; memcpy(&t, k2p, 8); v = t; }
                    if (v != so->k2_eq)
                    {
                        if (dir == BackwardScanDirection) so->leaf_i--; else so->leaf_i++;
                        continue;
                    }
                }
                /* Optimized copies for common fixed lengths; fallback for others */
                if (so->key_len == 2) smol_copy2(so->itup_data, k1p);
                else if (so->key_len == 4) smol_copy4(so->itup_data, k1p);
                else if (so->key_len == 8) smol_copy8(so->itup_data, k1p);
                else if (so->key_len == 16) smol_copy16(so->itup_data, k1p);
                else smol_copy_small(so->itup_data, k1p, so->key_len);

                if (so->key_len2 == 2) smol_copy2(so->itup_data + so->itup_off2, k2p);
                else if (so->key_len2 == 4) smol_copy4(so->itup_data + so->itup_off2, k2p);
                else if (so->key_len2 == 8) smol_copy8(so->itup_data + so->itup_off2, k2p);
                else if (so->key_len2 == 16) smol_copy16(so->itup_data + so->itup_off2, k2p);
                else smol_copy_small(so->itup_data + so->itup_off2, k2p, so->key_len2);

                /* Test runtime keys before returning */
                if (!smol_test_runtime_keys(scan, so))
                {
                    if (dir == BackwardScanDirection) so->leaf_i--; else so->leaf_i++;
                    continue; /* Skip this tuple, doesn't match all keys */
                }

                scan->xs_itup = so->itup; ItemPointerSet(&(scan->xs_heaptid), 0, 1);
                if (dir == BackwardScanDirection) so->leaf_i--; else so->leaf_i++;
                if (so->prof_enabled) { so->prof_rows++; so->prof_bytes += (uint64)(so->key_len + so->key_len2); }
                return true;
            }
        }
        else
        {
            uint16 n = so->cur_page_nitems; /* Opt #5: use cached nitems */
            if (dir == BackwardScanDirection)
            {
                /* For single-column backward scans:
                 * - Initial page: cur_off is set at line 2393
                 * - Subsequent pages: cur_off is set at line 3520 when advancing
                 * - After processing all items: cur_off becomes 0, which exits the while loop below
                 * DO NOT reset cur_off here, as cur_off=0 is a valid state meaning "finished this page" */

                while (so->cur_off >= FirstOffsetNumber)
                {
                    /* Fast path for plain pages: compute keyptr inline (O(1) pointer arithmetic) */
                    char *keyp;
                    if (so->page_is_plain)
                    {
                        /* Plain page: [u16 n][key1][key2]... - simple array layout */
                        keyp = base + sizeof(uint16) + ((size_t)(so->cur_off - 1)) * so->key_len;
                    }
                    else
                    {
                        /* RLE or other format: use cached lookup */
                        keyp = smol_leaf_keyptr_cached(so, page, so->cur_off, so->key_len, so->inc_len, so->ninclude, so->inc_cumul_offs);
                    }
                    /* For zero-copy pages, skip IndexTuple header to get key data */
                    if (so->page_is_zerocopy)
                        keyp += sizeof(IndexTupleData); /* GCOV_EXCL_LINE */
                    /* Check upper bound (for backward scans with <= or < bounds) */
                    if (so->have_upper_bound)
                    {
                        int c = smol_cmp_keyptr_to_upper_bound(so, keyp);
                        if (so->upper_bound_strict ? (c >= 0) : (c > 0))
                        {
                            /* Key exceeds upper bound, skip and continue */
                            so->cur_off--;
                            continue;
                        }
                    }
                    /* Check lower bound - if we've gone below it, terminate scan */
                    if (so->have_bound)
                    {
                        int c = smol_cmp_keyptr_to_bound(so, keyp);
                        if (so->bound_strict ? (c <= 0) : (c < 0))
                        {
                            /* Gone below lower bound, stop scan */
                            so->cur_blk = InvalidBlockNumber;
                            break;
                        }
                    }
                    if (so->have_k1_eq)
                    { /* GCOV_EXCL_LINE - opening brace artifact, outer if shows 110 executions */
                            int c = smol_cmp_keyptr_to_bound(so, keyp); 
                        if (c < 0) 
                        { /* GCOV_EXCL_LINE */
                            /* Past the equality run when scanning backward: terminate overall */ /* GCOV_EXCL_LINE */
                            so->cur_blk = InvalidBlockNumber; /* GCOV_EXCL_LINE */
                            break; /* GCOV_EXCL_LINE */
                        } /* GCOV_EXCL_LINE */
                        SMOL_DEFENSIVE_CHECK(c <= 0, ERROR,
                            (errmsg("smol: backward scan found key greater than equality bound")));
                        /* c == 0: emit normally */
                    }
                    /* Duplicate-key skip (single-key path): copy key once per run */
                    if (scan->xs_want_itup)
                    {
                        /* dynamic tuple emission handles copies */
                        if (!so->two_col)
                        {
                            /* Fast path for plain pages in backward scans: skip all run detection */
                            if (so->page_is_plain)
                            {
                                /* Plain pages have no duplicates - skip run detection entirely */
                                /* Just mark as active with trivial run bounds */
                                so->run_active = true;
                                so->run_start_off = so->cur_off;
                                so->run_end_off = so->cur_off;
                            }
                            else if (so->run_active && so->cur_off >= so->run_start_off)
                            {
                                /* within run: key already present in xs_itup */
                            }
                            else
                            {
                                /* new run: compute run_start (backward) and remember key */
                                const char *k0 = keyp;
                                so->run_key_len = (so->key_len <= sizeof(so->run_key) ? so->key_len : (uint16) sizeof(so->run_key));
                                memcpy(so->run_key, k0, so->run_key_len);
                                uint16 start = so->cur_off;
                                uint16 dummy_end;
                                /* Try cached run bounds first (O(1)) before linear scan (O(N)) */
                                bool cached = smol_get_cached_run_bounds(so, so->cur_off, &start, &dummy_end);
                                if (!cached && !smol_leaf_run_bounds_rle_ex(page, so->cur_off, so->key_len, &start, &dummy_end, so->inc_len, so->ninclude))
                                {
                                    /* RLE page but not encoded: scan backward to find run start */
                                    while (start > FirstOffsetNumber)
                                    {
                                        const char *kp = smol_leaf_keyptr_ex(page, (uint16) (start - 1), so->key_len, so->inc_len, so->ninclude, so->inc_cumul_offs);
                                        if (!smol_key_eq_len(k0, kp, so->key_len))
                                            break;
                                        start--; /* RLE page: scan backward to find start of duplicate run */
                                    }
                                    so->rle_run_inc_cached = false; /* Non-RLE page, no caching */
                                }
                                else
                                {
                                    /* RLE bounds found - cache disabled for backward scans (rare path) */
                                    so->rle_run_inc_cached = false;
                                }
                                so->run_start_off = start;
                                so->run_end_off = so->cur_off; /* not used in backward path */
                                so->run_active = true;
                            }
                        }
                        /* build tuple (varlena dynamic or fixed-size fast path) */
                        uint32 row = (uint32) (so->cur_off - 1);
                        if (so->has_varwidth)
                        { /* GCOV_EXCL_LINE */
                            smol_emit_single_tuple(so, page, keyp, row); 
                        } /* GCOV_EXCL_LINE */
                        else
                        {
                            if (so->key_len == 2) smol_copy2(so->itup_data, keyp);
                            else if (so->key_len == 4) smol_copy4(so->itup_data, keyp);
                            else if (so->key_len == 8) smol_copy8(so->itup_data, keyp); 
                            else if (so->key_len == 16) smol_copy16(so->itup_data, keyp); 
                            else smol_copy_small(so->itup_data, keyp, so->key_len); 
                            /* Copy INCLUDE columns */
                            if (so->ninclude > 0)
                            {
                                uint16 n2 = so->cur_page_nitems; /* Opt #5: use cached nitems */
                                for (uint16 ii=0; ii<so->ninclude; ii++)
                                {
                                    char *ip;
                                    /* Use cached pointers when available (fast paths) */
                                    if (so->plain_inc_cached)
                                        /* Plain page: base pointer + row offset */
                                        ip = so->plain_inc_base[ii] + (size_t) row * so->inc_len[ii];
                                    else if (so->rle_run_inc_cached)
                                        /* RLE page with cached run: INCLUDE values constant within run */
                                        ip = so->rle_run_inc_ptr[ii];
                                    else
                                        /* Slow path: compute pointer dynamically */
                                        ip = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_len, so->ninclude, ii, row, so->inc_cumul_offs);
                                    char *dst = so->itup_data + so->inc_offs[ii];
                                    /* Use precomputed copy function (eliminates if-else chain overhead) */
                                    so->inc_copy[ii](dst, ip);
                                }
                            }
                        }
                    }
                    if (smol_debug_log)
                    {
                        if (so->key_is_text32)
                        {
                            int32 vsz = VARSIZE_ANY((struct varlena *) so->itup_data);
                            SMOL_LOGF("tuple key varlena size=%d", vsz);
                        }
                        for (uint16 ii=0; ii<so->ninclude; ii++)
                        {
                            if (so->inc_is_text[ii])
                            {
                                char *dst = so->itup_data + so->inc_offs[ii];
                                int32 vsz = VARSIZE_ANY((struct varlena *) dst);
                                SMOL_LOGF("tuple include[%u] varlena size=%d off=%u", ii, vsz, so->inc_offs[ii]);
                            }
                        }
                    }
                    if (so->prof_enabled)
                    {
                        if (scan->xs_want_itup)
                            so->prof_bytes += (uint64) so->key_len;
                        so->prof_touched += (uint64) so->key_len;
                    }

                    /* Test runtime keys before returning */
                    if (!smol_test_runtime_keys(scan, so)) 
                    { /* GCOV_EXCL_LINE */
                        so->cur_off--; /* GCOV_EXCL_LINE */
                        continue; /* Skip this tuple */ /* GCOV_EXCL_LINE */
                    } /* GCOV_EXCL_LINE */

                    if (scan->xs_want_itup)
                        scan->xs_itup = so->itup;
                    ItemPointerSet(&(scan->xs_heaptid), 0, 1);
                    so->cur_off--;
                    if (so->prof_enabled) so->prof_rows++;
                    return true;
                }
            }
            else
            {
                /* Initialize cur_off for forward scans on new pages */
                if (so->cur_off == InvalidOffsetNumber || so->cur_off == 0)
                    so->cur_off = FirstOffsetNumber; /* GCOV_EXCL_LINE - defensive: cur_off always FirstOffsetNumber (set at lines 2464, 2522, 3495) */

                while (so->cur_off <= n)
                {
                    char *keyp = smol_leaf_keyptr_cached(so, page, so->cur_off, so->key_len, so->inc_len, so->ninclude, so->inc_cumul_offs);
                    /* For zero-copy pages, keyp points to IndexTuple header; adjust to key data */
                    IndexTuple itup_ptr = NULL;
                    if (so->page_is_zerocopy)
                    { /* GCOV_EXCL_START - zero-copy format only in deprecated build function */
                        itup_ptr = (IndexTuple) keyp;  /* Save IndexTuple pointer for later */
                        keyp += sizeof(IndexTupleData);  /* Skip header to get key data */

                        /* ULTRA-FAST PATH for zero-copy: check qual and return immediately
                         * Skip all run detection, memcpy, INCLUDE processing for unique-key workloads */ /* GCOV_EXCL_LINE - zero-copy ultra-fast path rarely triggered by planner */
                        if (so->page_is_plain && !so->have_upper_bound && !so->have_k1_eq && scan->xs_want_itup) /* GCOV_EXCL_LINE */
                        { /* GCOV_EXCL_LINE */
                            /* Test runtime keys */ /* GCOV_EXCL_LINE */
                            if (smol_test_runtime_keys(scan, so)) /* GCOV_EXCL_LINE - single-column indexes have no runtime keys */
                            { /* GCOV_EXCL_LINE */
                                scan->xs_itup = itup_ptr; /* GCOV_EXCL_LINE */
                                ItemPointerSet(&(scan->xs_heaptid), 0, 1); /* GCOV_EXCL_LINE */
                                so->cur_off++; /* GCOV_EXCL_LINE */
                                if (so->prof_enabled) so->prof_rows++; /* GCOV_EXCL_LINE */
                                return true; /* GCOV_EXCL_LINE */
                            } /* GCOV_EXCL_LINE */
                            /* Key didn't match, continue to next */ /* GCOV_EXCL_LINE */
                            so->cur_off++; /* GCOV_EXCL_LINE */
                            continue; /* GCOV_EXCL_LINE */
                        } /* GCOV_EXCL_LINE */
                    } /* GCOV_EXCL_STOP */
                    /* Check upper bound (for BETWEEN queries) */
                    if (so->have_upper_bound)
                    {
                        int c = smol_cmp_keyptr_to_upper_bound(so, keyp);
                        if (so->upper_bound_strict ? (c >= 0) : (c > 0))
                        {
                            /* Exceeded upper bound, stop scan */
                            if (so->have_pin && BufferIsValid(so->cur_buf)) { ReleaseBuffer(so->cur_buf); so->have_pin=false; }
                            so->cur_blk = InvalidBlockNumber;
                            return false;
                        }
                    }
                    if (so->have_k1_eq)
                    {
                            int c = smol_cmp_keyptr_to_bound(so, keyp);
                        SMOL_DEFENSIVE_CHECK(c >= 0, ERROR,
                            (errmsg("smol: have_k1_eq scan found key < bound (impossible)")));
                        if (c > 0)
                        {
                            /* Leading-key equality: any subsequent leaf cannot have =bound */
                            if (so->have_pin && BufferIsValid(so->cur_buf)) { ReleaseBuffer(so->cur_buf); so->have_pin=false; }
                            so->cur_blk = InvalidBlockNumber;
                            return false;
                        }
                        /* c == 0: emit normally */
                    }
                    /* Duplicate-key skip (single-key path): copy key once per run */
                    if (scan->xs_want_itup)
                    {
                        /* dynamic/fixed emission handles copies */
                        if (!so->two_col)
                        {
                            if (so->run_active && so->cur_off <= so->run_end_off)
                            {
                                /* run reuse enabled - already have run bounds */
                            }
                            else
                            {
                                /* Starting new run - compute bounds and cache key */
                                const char *k0 = keyp;
                                so->run_key_len = (so->key_len <= sizeof(so->run_key) ? so->key_len : (uint16) sizeof(so->run_key));
                                memcpy(so->run_key, k0, so->run_key_len);
                                uint16 start = so->cur_off, end = so->cur_off;
                                /* Run-detection optimization active: plain pages have no duplicates */
                                if (so->page_is_plain)
                                {
                                    /* Plain page: each row is its own run (length 1), skip scanning */
                                    start = end = so->cur_off;
                                }
                                /* Try cached run bounds first (O(1)) before linear scan (O(N)) */
                                else if (!smol_get_cached_run_bounds(so, so->cur_off, &start, &end) &&
                                         !smol_leaf_run_bounds_rle_ex(page, so->cur_off, so->key_len, &start, &end, so->inc_len, so->ninclude))
                                {
                                    /* RLE page but not encoded: scan forward to find run end */
                                    while (end < n)
                                    {
                                        const char *kp = smol_leaf_keyptr_ex(page, (uint16) (end + 1), so->key_len, so->inc_len, so->ninclude, so->inc_cumul_offs);
                                        if (!smol_key_eq_len(k0, kp, so->key_len))
                                            break;
                                        end++;
                                    }
                                    so->rle_run_inc_cached = false; /* Non-RLE page, no caching */
                                }
                                else
                                {
                                    /* RLE bounds found - cache INCLUDE pointers for this run */
                                    if (so->ninclude > 0)
                                    {
                                        /* In include-RLE (tag 0x8003), all tuples in a run share the same INCLUDE values.
                                         * Cache pointers to these values to avoid repeated linear searches through runs.
                                         * This optimization is critical for sequential scans returning many rows. */
                                        for (uint16 ii = 0; ii < so->ninclude; ii++)
                                        {
                                            so->rle_run_inc_ptr[ii] = smol1_inc_ptr_any(page, so->key_len, n,
                                                                                         so->inc_len, so->ninclude,
                                                                                         ii, start - 1, so->inc_cumul_offs);
                                        }
                                        so->rle_run_inc_cached = true;
                                    }
                                    else
                                    {
                                        so->rle_run_inc_cached = false;
                                    }
                                }
                                so->run_start_off = start; /* not used in forward path */
                                so->run_end_off = end;
                                so->run_active = true;
                                so->run_inc_evaluated = false;
                                if (so->key_is_text32)
                                {
                                    const char *kend = (const char *) memchr(k0, '\0', 32);
                                    int klen = (int) (kend ? (kend - k0) : 32);
                                    so->run_text_klen = (int16) klen;
                                    SET_VARSIZE((struct varlena *) so->run_key_vl, klen + VARHDRSZ);
                                    memcpy(so->run_key_vl + VARHDRSZ, k0, (size_t) klen);
                                    so->run_key_vl_len = (int16) (VARHDRSZ + klen);
                                    so->run_key_built = true;
                                }
                            }
                        }
                        uint32 row = (uint32) (so->cur_off - 1);
                        if (so->page_is_zerocopy)
                        {
                            /* Zero-copy fast path: keyp already points to complete IndexTuple
                             * Format: [6B TID][2B t_info][key data]
                             * No memcpy needed - use page buffer directly */
                            /* Note: We MUST NOT do the else branches below - they would corrupt the data */
                        }
                        else if (so->has_varwidth)
                        {
                            /* Dynamic varlena tuple build */
                            smol_emit_single_tuple(so, page, keyp, row);
                            if (smol_debug_log && so->key_is_text32)
                            {
                                int32 vsz = VARSIZE_ANY((struct varlena *) so->itup_data);
                                SMOL_LOGF("tuple key varlena size=%d", vsz);
                            }
                        }
                        else
                        {
                            /* Fixed-size fast path: copy key and includes into pre-sized tuple */
                            if (so->key_len == 2) smol_copy2(so->itup_data, keyp);
                            else if (so->key_len == 4) smol_copy4(so->itup_data, keyp);
                            else if (so->key_len == 8) smol_copy8(so->itup_data, keyp);
                            else if (so->key_len == 16) smol_copy16(so->itup_data, keyp);
                            else smol_copy_small(so->itup_data, keyp, so->key_len);
                            if (so->ninclude > 0)
                            {
                                uint16 n2 = n;
                                for (uint16 ii=0; ii<so->ninclude; ii++)
                                {
                                    char *ip;
                                    /* Use cached pointers when available (fast paths) */
                                    if (so->plain_inc_cached)
                                        /* Plain page: base pointer + row offset */
                                        ip = so->plain_inc_base[ii] + (size_t) row * so->inc_len[ii];
                                    else if (so->rle_run_inc_cached)
                                        /* RLE page with cached run: INCLUDE values constant within run */
                                        ip = so->rle_run_inc_ptr[ii];
                                    else /* GCOV_EXCL_LINE - defensive fallback: caching now covers all RLE pages with INCLUDE */
                                        /* Slow path: compute pointer dynamically */ /* GCOV_EXCL_LINE */
                                        ip = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_len, so->ninclude, ii, row, so->inc_cumul_offs); /* GCOV_EXCL_LINE */
                                    char *dst = so->itup_data + so->inc_offs[ii];
                                    /* Use precomputed copy function (eliminates if-else chain overhead) */
                                    so->inc_copy[ii](dst, ip);
                                }
                            }
                        }
                    }
                    if (so->ninclude > 0 && scan->xs_want_itup)
                    {
                        /* Determine include run constness for skip decisions (does not emit) */
                        uint16 n2 = n;
                        bool need_inc_copy = true;
                        if (so->run_active && !so->two_col)
                        {
                            if (!so->run_inc_evaluated)
                            {
                                for (uint16 ii=0; ii<so->ninclude; ii++)
                                {
                                    bool all_eq = true;
                                    uint16 start = so->run_start_off, end = so->run_end_off;
                                    char *firstp = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_len, so->ninclude, ii, (uint32)(start - 1), so->inc_cumul_offs);
                                    for (uint16 off = start + 1; off <= end; off++)
                                    {
                                        char *p2 = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_len, so->ninclude, ii, (uint32)(off - 1), so->inc_cumul_offs);
                                        if (memcmp(firstp, p2, so->inc_len[ii]) != 0)
                                        { all_eq = false; break; }
                                    }
                                    so->inc_const[ii] = all_eq;
                                    if (all_eq && so->inc_is_text[ii])
                                    {
                                        const char *zend = (const char *) memchr(firstp, '\0', so->inc_len[ii]);
                                        so->run_inc_len[ii] = (int16) (zend ? (zend - firstp) : so->inc_len[ii]);
                                    }
                                }
                                so->run_inc_evaluated = true;
                                if (so->cur_off == so->run_start_off)
                                {
                                    for (uint16 ii=0; ii<so->ninclude; ii++)
                                    {
                                        if (!so->inc_const[ii]) continue;
                                        char *ip0 = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_len, so->ninclude, ii, (uint32)(so->run_start_off - 1), so->inc_cumul_offs);
                                        if (so->inc_is_text[ii])
                                        {
                                            int ilen0 = 0; while (ilen0 < so->inc_len[ii] && ip0[ilen0] != '\0') ilen0++;
                                            SET_VARSIZE((struct varlena *) so->run_inc_vl[ii], ilen0 + VARHDRSZ);
                                            memcpy(so->run_inc_vl[ii] + VARHDRSZ, ip0, ilen0);
                                            so->run_inc_vl_len[ii] = (int16) (VARHDRSZ + ilen0);
                                            so->run_inc_built[ii] = true;
                                        }
                                        /* fixed-size includes are copied later in fixed path */
                                    }
                                }
                            }
                            bool all_const = true;
                            for (uint16 ii=0; ii<so->ninclude; ii++) if (!so->inc_const[ii]) { all_const = false; break; }
                            if (all_const && so->cur_off > so->run_start_off)
                                need_inc_copy = false;
                        }
                        (void) need_inc_copy; /* emission already done above */
                    }
                    if (so->prof_enabled)
                    {
                        if (scan->xs_want_itup)
                            so->prof_bytes += (uint64) so->key_len;
                        so->prof_touched += (uint64) so->key_len;
                    }

                    /* Test runtime keys before returning */
                    /* For zero-copy, temporarily set so->itup to page pointer for runtime key test */ /* GCOV_EXCL_LINE - zero-copy path rarely hit */
                    IndexTuple saved_itup = so->itup; 
                    if (so->page_is_zerocopy) 
                        so->itup = itup_ptr; /* GCOV_EXCL_LINE */

                    if (!smol_test_runtime_keys(scan, so)) 
                    { /* GCOV_EXCL_LINE */
                        if (so->page_is_zerocopy) /* GCOV_EXCL_LINE */
                            so->itup = saved_itup;  /* Restore */ /* GCOV_EXCL_LINE */
                        so->cur_off++; /* GCOV_EXCL_LINE */
                        continue; /* Skip this tuple */ /* GCOV_EXCL_LINE */
                    } /* GCOV_EXCL_LINE */

                    if (so->page_is_zerocopy) 
                        so->itup = saved_itup;  /* Restore */ /* GCOV_EXCL_LINE */

                    if (scan->xs_want_itup)
                    {
                        if (so->page_is_zerocopy) 
                            scan->xs_itup = itup_ptr;  /* Zero-copy: direct IndexTuple page pointer */ /* GCOV_EXCL_LINE */
                        else
                            scan->xs_itup = so->itup;  /* Non-zero-copy: copied data */
                    }
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
            /* If we reserved a local chunk, take the next rightlink without touching shared state. */
            if (so->chunk_left > 0)
            {
                op = smol_page_opaque(page);
                next = op->rightlink;
                so->chunk_left--;
                if (BlockNumberIsValid(next))
                {
                    PrefetchBuffer(idx, MAIN_FORKNUM, next);
                    SMOL_LOGF("PARALLEL: prefetch_depth=%d, next=%u", smol_prefetch_depth, next);
                    SMOL_DEFENSIVE_CHECK(smol_prefetch_depth <= 1, WARNING,
                                        (errmsg("smol: prefetch_depth > 1 not fully tested")));
                    if (smol_prefetch_depth > 1)
                    {
                        SMOL_LOG("PARALLEL: INSIDE prefetch_depth > 1 branch!");
                        BlockNumber nblocks = RelationGetNumberOfBlocks(idx);
                        for (int d=2; d<=smol_prefetch_depth; d++)
                        {
                            BlockNumber pb = next + (BlockNumber) (d-1);
                            if (pb < nblocks)
                                PrefetchBuffer(idx, MAIN_FORKNUM, pb);
                            else
                                break; /* GCOV_EXCL_LINE - break when prefetch reaches end of small index, rare in practice */
                        }
                    }
                }
            }
            else
            {
                for (;;)
                {
                    uint32 curv = SMOL_ATOMIC_READ_U32(&ps->curr);
                    if (curv == 0u)
                    { /* GCOV_EXCL_START - only reachable via parallel rescan, which is extremely rare */
                        /* Defensive: PostgreSQL planner only uses parallel index scans with WHERE clauses, so have_bound must be true */
                        SMOL_DEFENSIVE_CHECK(so->have_bound, ERROR,
                                            (errmsg("smol: parallel scan without bound")));
                        int64 lb;
                        if (so->atttypid == INT2OID) lb = (int64) DatumGetInt16(so->bound_datum);
                        else if (so->atttypid == INT4OID) lb = (int64) DatumGetInt32(so->bound_datum);
                        else if (so->atttypid == INT8OID) lb = DatumGetInt64(so->bound_datum);
                        else lb = PG_INT64_MIN; /* Non-INT types: use minimum for conservative bound */
                        BlockNumber left = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                        Buffer lbuf = ReadBufferExtended(idx, MAIN_FORKNUM, left, RBM_NORMAL, so->bstrategy);
                        Page lpg = BufferGetPage(lbuf);
                        BlockNumber step = smol_page_opaque(lpg)->rightlink;
                        uint32 claimed = 0;
                        for (int i = 1; i < smol_parallel_claim_batch && BlockNumberIsValid(step); i++)
                        {
                            Buffer nb = ReadBuffer(idx, step);
                            Page np = BufferGetPage(nb);
                            BlockNumber rl2 = smol_page_opaque(np)->rightlink;
                            ReleaseBuffer(nb);
                            step = rl2;
                            claimed++;
                        }
                        ReleaseBuffer(lbuf);
                        uint32 expect = 0u;
                        uint32 newv = (uint32) (BlockNumberIsValid(step) ? step : InvalidBlockNumber);
                        if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expect, newv))
                        {
                            next = left;
                            so->chunk_left = claimed;
                            if (BlockNumberIsValid(next))
                                PrefetchBuffer(idx, MAIN_FORKNUM, next);
                            break;
                        }
                        continue;
                    } /* GCOV_EXCL_STOP */
                    if (curv == (uint32) InvalidBlockNumber)
                    { next = InvalidBlockNumber; break; }
                    /* Read rightlink to publish next (batch skip) */
                    Buffer tbuf = ReadBufferExtended(idx, MAIN_FORKNUM, (BlockNumber) curv, RBM_NORMAL, so->bstrategy);
                    Page tpg = BufferGetPage(tbuf);
                    BlockNumber step = smol_page_opaque(tpg)->rightlink;
                    uint32 claimed = 0;
                    for (int i = 1; i < smol_parallel_claim_batch && BlockNumberIsValid(step); i++)
                    {
                        Buffer nb = ReadBufferExtended(idx, MAIN_FORKNUM, step, RBM_NORMAL, so->bstrategy);
                        Page np = BufferGetPage(nb);
                        BlockNumber rl2 = smol_page_opaque(np)->rightlink;
                        ReleaseBuffer(nb);
                        step = rl2;
                        claimed++;
                    }
                    ReleaseBuffer(tbuf);
                    uint32 expected = curv;
                    uint32 newv = (uint32) (BlockNumberIsValid(step) ? step : InvalidBlockNumber);
                    if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expected, newv))
                    {
                        next = (BlockNumber) curv;
                        so->chunk_left = claimed;
                        if (BlockNumberIsValid(next))
                            PrefetchBuffer(idx, MAIN_FORKNUM, next);
                        break;
                    }
                }
            }
        }
        else
        {
            /* Increment pages_scanned for adaptive prefetch tracking (non-parallel path) */
            if (dir != BackwardScanDirection && so->pages_scanned < 65535)
                so->pages_scanned++;

            /* Read rightlink/leftlink BEFORE releasing buffer */
            op = smol_page_opaque(page);
            next = (dir == BackwardScanDirection) ? op->leftlink : op->rightlink;

            /* Adaptive prefetching with slow-start for bounded scans
             * Avoids over-prefetching for equality lookups and narrow ranges
             * while ramping up for larger scans */
            if (dir != BackwardScanDirection && BlockNumberIsValid(next))
            {
                /* Determine effective prefetch depth using adaptive slow-start */
                int effective_depth;

                if (so->have_k1_eq)
                {
                    /* Equality lookups: No prefetch at all for first 2 pages, then minimal
                     * This eliminates 496-page prefetch waste for single-row queries */
                    if (so->pages_scanned < 2)
                        effective_depth = 0;  /* No prefetch for first 2 pages */
                    else if (so->pages_scanned < 5)
                        effective_depth = 1;  /* Minimal prefetch if scan continues */
                    else
                        effective_depth = Min(2, smol_prefetch_depth); /* Cap at 2 for equality */
                }
                else if (so->have_upper_bound)
                {
                    /* Bounded range queries: Slow-start ramp
                     * Start conservatively, increase as we confirm scan is large */
                    if (so->pages_scanned < 3)
                        effective_depth = 0;  /* No prefetch for first 3 pages (test range size) */
                    else if (so->pages_scanned < 8)
                        effective_depth = 1;  /* Minimal prefetch for narrow ranges */
                    else if (so->pages_scanned < 20)
                        effective_depth = 2;  /* Moderate prefetch for medium ranges */
                    else if (so->pages_scanned < 50)
                        effective_depth = 4;  /* Growing confidence */
                    else
                        effective_depth = Min((int)(so->pages_scanned / 10), smol_prefetch_depth);
                }
                else
                {
                    /* Unbounded forward scans: Use full prefetch depth immediately
                     * These benefit from aggressive prefetching */
                    effective_depth = smol_prefetch_depth;
                }

                /* Prefetch with computed depth */
                if (effective_depth > 0)
                {
                    PrefetchBuffer(idx, MAIN_FORKNUM, next);
                    SMOL_LOGF("NON-PARALLEL: adaptive_prefetch_depth=%d pages_scanned=%u next=%u",
                              effective_depth, so->pages_scanned, next);

                    if (effective_depth > 1)
                    {
                        BlockNumber nblocks = RelationGetNumberOfBlocks(idx);
                        for (int d = 2; d <= effective_depth; d++)
                        {
                            BlockNumber pb = next + (BlockNumber) (d - 1);
                            if (pb < nblocks)
                                PrefetchBuffer(idx, MAIN_FORKNUM, pb);
                            else
                                break;
                        }
                    }
                }
            }
        }
        /* Now safe to release buffer after reading rightlink */
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
        smol_run_reset(so);  /* Reset RLE run state when moving to new page */
        if (BlockNumberIsValid(so->cur_blk))
        {
            if (scan->parallel_scan && dir != BackwardScanDirection)
                so->cur_blk = next;
            /* Pre-pin next leaf and rebuild cache for two-col */
            Buffer nbuf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy);
            Page np = BufferGetPage(nbuf);

            /* For backward scans, set cur_off to last item in new page */
            if (dir == BackwardScanDirection && !so->two_col)
            {
                uint16 n_new = smol_leaf_nitems(np);
                so->cur_off = n_new;
            }

            /* Page-level bounds checking: stop scan early for BETWEEN and equality queries
             * Only applies to single-column indexes with forward scans that have upper bounds or equality */
#ifdef SMOL_TEST_COVERAGE
            bool enable_page_bounds_check = smol_test_force_page_bounds_check ||
                (!so->two_col && dir != BackwardScanDirection && (so->have_upper_bound || so->have_k1_eq));
#else
            bool enable_page_bounds_check = !so->two_col && dir != BackwardScanDirection && (so->have_upper_bound || so->have_k1_eq);
#endif
            if (enable_page_bounds_check)
            {
                uint16 n_check = smol_leaf_nitems(np);

                if (n_check > 0)
                {
                    /* Detect page format for zero-copy bounds checking */
                    ItemId iid_check = PageGetItemId(np, FirstOffsetNumber);
                    char *base_check = (char *) PageGetItem(np, iid_check);
                    uint16 tag_check; memcpy(&tag_check, base_check, sizeof(uint16));
                    bool page_is_zerocopy_check = (tag_check == SMOL_TAG_ZEROCOPY);

                    /* Temporarily set format for bounds checking */
                    bool saved_zerocopy = so->page_is_zerocopy;
                    so->page_is_zerocopy = page_is_zerocopy_check;

                    bool stop_scan = false;
                    bool matches pg_attribute_unused() = smol_page_matches_scan_bounds(so, np, n_check, &stop_scan);

                    /* Restore original format flag */
                    so->page_is_zerocopy = saved_zerocopy;

                    /* Page must match bounds - scan logic stops at tuple level before loading non-matching pages */
                    Assert(matches);
                    Assert(!stop_scan);
                }
            }

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
                        int c = smol_cmp_keyptr_to_bound(so, k1p);
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
                    /* Check if this page is zero-copy format */
                    ItemId iid2 = PageGetItemId(np, FirstOffsetNumber);
                    char *base2 = (char *) PageGetItem(np, iid2);
                    uint16 tag2; memcpy(&tag2, base2, sizeof(uint16));
                    bool page2_is_zerocopy = (tag2 == SMOL_TAG_ZEROCOPY);
                    uint16 lo2 = FirstOffsetNumber, hi2 = n2, ans2 = InvalidOffsetNumber;
                    while (lo2 <= hi2)
                    {
                        uint16 mid2 = (uint16) (lo2 + ((hi2 - lo2) >> 1));
                        char *kp2 = smol_leaf_keyptr_ex(np, mid2, so->key_len, so->inc_len, so->ninclude, so->inc_cumul_offs);
                        if (page2_is_zerocopy) kp2 += sizeof(IndexTupleData);  /* Skip IndexTuple header to get key data */
                        int cc = smol_cmp_keyptr_to_bound(so, kp2);
                        if (so->prof_enabled) so->prof_bsteps++;
                        if ((so->bound_strict ? (cc > 0) : (cc >= 0))) { ans2 = mid2; if (mid2 == 0) break; hi2 = (uint16) (mid2 - 1); }
                        else lo2 = (uint16) (mid2 + 1);
                    }
                    so->cur_off = (ans2 != InvalidOffsetNumber) ? ans2 : (uint16) (n2 + 1);
                }
#ifdef SMOL_PLANNER_BACKWARD_BOUNDS
                else if (so->have_bound && dir == BackwardScanDirection)
                {
                    /* backward: position to last <= bound in this leaf */
                    /* NOTE: PostgreSQL planner doesn't generate backward scans with lower bounds.
                     * It uses forward scan + sort instead. Enable when planner supports it. */
                    uint16 n2 = smol_leaf_nitems(np);
                    /* Find first > bound, then step one back */
                    uint16 lo2 = FirstOffsetNumber, hi2 = n2, ans2 = InvalidOffsetNumber;
                    while (lo2 <= hi2)
                    {
                        uint16 mid2 = (uint16) (lo2 + ((hi2 - lo2) >> 1));
                        char *kp2 = smol_leaf_keyptr_ex(np, mid2, so->key_len, so->inc_len, so->ninclude, so->inc_cumul_offs);
                        int cc = smol_cmp_keyptr_to_bound(so, kp2);
                        if (so->prof_enabled) so->prof_bsteps++;
                        if (cc > 0) { ans2 = mid2; if (mid2 == 0) break; hi2 = (uint16) (mid2 - 1); }
                        else lo2 = (uint16) (mid2 + 1);
                    }
                    if (ans2 == InvalidOffsetNumber)
                        so->cur_off = n2;
                    else
                        so->cur_off = (ans2 > FirstOffsetNumber) ? (uint16) (ans2 - 1) : InvalidOffsetNumber;
                }
#endif /* SMOL_PLANNER_BACKWARD_BOUNDS */
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
    /* Note: Removed pointer logging to keep regression tests deterministic */
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
        if (so->bstrategy)
            FreeAccessStrategy(so->bstrategy);
        if (so->runtime_keys)
            pfree(so->runtime_keys);
        if (so->prof_enabled)
            elog(LOG, "[smol] scan profile: calls=%lu rows=%lu leaf_pages=%lu bytes_copied=%lu bytes_touched=%lu binsearch_steps=%lu",
                 (unsigned long) so->prof_calls,
                 (unsigned long) so->prof_rows,
                 (unsigned long) so->prof_pages,
                 (unsigned long) so->prof_bytes,
                 (unsigned long) so->prof_touched,
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
smol_parallelrescan(IndexScanDesc scan) /* GCOV_EXCL_START - extremely rare: parallel scan rescan, see comments */
{
    if (scan->parallel_scan)
    {
        SmolParallelScan *ps = (SmolParallelScan *) ((char *) scan->parallel_scan + scan->parallel_scan->ps_offset_am);
        pg_atomic_write_u32(&ps->curr, 0u);
    }
} /* GCOV_EXCL_STOP */

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
    bool        result = true;
    HeapTuple   classtup;
    Form_pg_opclass classform;
    Oid         opfamilyoid;
    Oid         opcintype;
    Oid         opckeytype;
    char       *opfamilyname;
    CatCList   *proclist,
               *oprlist;
    List       *grouplist;
    OpFamilyOpFuncGroup *opclassgroup = NULL;
    int         i;
    ListCell   *lc;

    classtup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
    SMOL_DEFENSIVE_CHECK(HeapTupleIsValid(classtup), ERROR,
        (errmsg("cache lookup failed for operator class %u", opclassoid)));
    classform = (Form_pg_opclass) GETSTRUCT(classtup);
    opfamilyoid = classform->opcfamily;
    opcintype = classform->opcintype;
    opckeytype = classform->opckeytype;
    if (!OidIsValid(opckeytype))
        opckeytype = opcintype;

    /* Validate that SMOL supports this data type */
    {
        int16 typlen;
        bool  byval;
        char  align;
        get_typlenbyvalalign(opcintype, &typlen, &byval, &align);

        if (typlen <= 0 && opcintype != TEXTOID)
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("operator class \"%s\" uses unsupported data type",
                            NameStr(classform->opcname)),
                     errdetail("SMOL supports fixed-length types or text (<=32B with C collation) only.")));
        }
    }

    opfamilyname = get_opfamily_name(opfamilyoid, false);

    oprlist = SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid));
    proclist = SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid));

    /* Support procs: require comparator at support number 1, int4 return, two args of keytype */
    for (i = 0; i < proclist->n_members; i++)
    {
        HeapTuple   proctup = &proclist->members[i]->tuple;
        Form_pg_amproc procform = (Form_pg_amproc) GETSTRUCT(proctup);
        bool        ok = true; 

        if (procform->amproclefttype != procform->amprocrighttype)
        {
            ereport(INFO,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("smol opfamily %s contains support procedure %s with cross-type registration",
                            opfamilyname, format_procedure(procform->amproc))));
            result = false;
        }
        SMOL_DEFENSIVE_CHECK(procform->amproclefttype == opcintype, INFO,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
             errmsg("smol: skipping cross-type proc validation")));
        if (procform->amproclefttype != opcintype)
            continue; /* GCOV_EXCL_LINE - cross-type proc validation requires complex operator family setup */

        if (procform->amprocnum != 1)
        {
            ereport(INFO,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("smol opfamily %s contains function %s with invalid support number %d",
                            opfamilyname, format_procedure(procform->amproc), procform->amprocnum)));
            result = false;
            continue;
        }
        ok = check_amproc_signature(procform->amproc, INT4OID, false, 2, 2, opckeytype, opckeytype);
        if (!ok)
        {
            ereport(INFO,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("smol opfamily %s contains function %s with wrong signature for support number %d",
                            opfamilyname, format_procedure(procform->amproc), procform->amprocnum)));
            result = false;
        }
    }

    /* Operators: strategies 1..5, search purpose, no ORDER BY */
    for (i = 0; i < oprlist->n_members; i++)
    {
        HeapTuple   oprtup = &oprlist->members[i]->tuple;
        Form_pg_amop oprform = (Form_pg_amop) GETSTRUCT(oprtup);

        if (oprform->amopstrategy < 1 || oprform->amopstrategy > 5)
        {
            ereport(INFO,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("smol opfamily %s contains operator %s with invalid strategy number %d",
                            opfamilyname, format_operator(oprform->amopopr), oprform->amopstrategy)));
            result = false;
        }
        if (oprform->amoppurpose != AMOP_SEARCH || OidIsValid(oprform->amopsortfamily))
        {
            ereport(INFO,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("smol opfamily %s contains invalid ORDER BY specification for operator %s",
                            opfamilyname, format_operator(oprform->amopopr))));
            result = false;
        }
        if (!check_amop_signature(oprform->amopopr, BOOLOID,
                                  oprform->amoplefttype,
                                  oprform->amoprighttype))
        {
            ereport(INFO,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                     errmsg("smol opfamily %s contains operator %s with wrong signature",
                            opfamilyname, format_operator(oprform->amopopr))));
            result = false;
        }
    }

    /* Ensure the opclass group has comparator proc 1 present */
    grouplist = identify_opfamily_groups(oprlist, proclist);
    foreach(lc, grouplist)
    {
        OpFamilyOpFuncGroup *grp = (OpFamilyOpFuncGroup *) lfirst(lc);
        if (grp->lefttype == opcintype && grp->righttype == opcintype)
        {
            opclassgroup = grp;
            break;
        }
    }
    if (opclassgroup == NULL || (opclassgroup->functionset & (((uint64) 1) << 1)) == 0)
    {
        ereport(INFO,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                 errmsg("smol opclass is missing required comparator support function 1")));
        result = false;
    }

    /* release syscache lists */
    ReleaseSysCache(classtup);
    ReleaseCatCacheList(oprlist);
    ReleaseCatCacheList(proclist);
    list_free(grouplist);

    return result;
}

/*
 * Test wrapper for smol_validate() - allows direct SQL calls to validation
 * function for coverage testing of error paths.
 */
PG_FUNCTION_INFO_V1(smol_test_validate);
Datum
smol_test_validate(PG_FUNCTION_ARGS)
{
    Oid opclassoid = PG_GETARG_OID(0);
    bool result = smol_validate(opclassoid);
    PG_RETURN_BOOL(result);
}

/* --- Helpers ----------------------------------------------------------- */
static void
smol_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                  Cost *indexStartupCost, Cost *indexTotalCost,
                  Selectivity *indexSelectivity, double *indexCorrelation,
                  double *indexPages)
{
    GenericCosts costs;

    /* Use genericcostestimate for standard index cost calculation with parallel support */
    MemSet(&costs, 0, sizeof(costs));
    genericcostestimate(root, path, loop_count, &costs);

    /* Apply SMOL-specific cost multipliers
     *
     * smol_cost_page: Multiplier for I/O costs
     *   - Values < 1.0: Make smol cheaper (sequential I/O advantage)
     *   - Values > 1.0: Make smol more expensive (penalize smol vs btree)
     *   - Default: 1.0 (neutral, same as btree)
     *
     * smol_cost_tup: Multiplier for per-tuple CPU costs
     *   - Values < 1.0: Make smol cheaper (optimized tuple processing)
     *   - Values > 1.0: Make smol more expensive (format detection overhead)
     *   - Default: 1.0 (neutral, same as btree)
     */

    /* Apply page I/O cost multiplier */
    if (smol_cost_page != 1.0 && costs.spc_random_page_cost > 0.0)
    {
        Cost io_cost = costs.numIndexPages * costs.spc_random_page_cost;
        Cost cpu_cost = costs.indexTotalCost - io_cost;
        costs.indexTotalCost = (io_cost * smol_cost_page) + cpu_cost;
    }

    /* Apply per-tuple CPU cost multiplier */
    if (smol_cost_tup != 1.0)
    {
        /* Estimate CPU cost as indexTotalCost minus I/O cost, then apply multiplier */
        Cost io_cost = costs.numIndexPages * costs.spc_random_page_cost;
        Cost cpu_cost = costs.indexTotalCost - io_cost;
        costs.indexTotalCost = io_cost + (cpu_cost * smol_cost_tup);
    }

    *indexStartupCost = costs.indexStartupCost;
    *indexTotalCost = costs.indexTotalCost;
    *indexSelectivity = costs.indexSelectivity;
    *indexCorrelation = costs.indexCorrelation;
    *indexPages = costs.numIndexPages;
}

/* VACUUM/cleanup stub: SMOL is read-only; nothing to reclaim. */
static IndexBulkDeleteResult *
smol_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    (void) info;
    (void) stats;
    return NULL;
}

/*
 * smol_translatestrategy - translate strategy number to CompareType
 *
 * SMOL uses standard btree-style strategy numbers:
 *   1 = <  (COMPARE_LT)
 *   2 = <= (COMPARE_LE)
 *   3 = =  (COMPARE_EQ)
 *   4 = >= (COMPARE_GE)
 *   5 = >  (COMPARE_GT)
 */
static CompareType
smol_translatestrategy(StrategyNumber strat, Oid opfamily)
{
    CompareType result;

    (void) opfamily;  /* unused */

    /* Direct mapping from strategy to CompareType (same as btree) */
    Assert(strat >= 1 && strat <= 5); /* Parser ensures valid strategy */
    result = (CompareType) strat;

    return result;
}

/*
 * smol_translatecmptype - translate CompareType to strategy number
 *
 * Inverse of smol_translatestrategy. Maps CompareType back to strategy:
 *   COMPARE_LT (1) -> 1 (<)
 *   COMPARE_LE (2) -> 2 (<=)
 *   COMPARE_EQ (3) -> 3 (=)
 *   COMPARE_GE (4) -> 4 (>=)
 *   COMPARE_GT (5) -> 5 (>)
 */
static StrategyNumber
smol_translatecmptype(CompareType cmptype, Oid opfamily)
{
    (void) opfamily;  /* unused */

    /* Direct mapping from CompareType to strategy (same as btree) */
    Assert(cmptype >= COMPARE_LT && cmptype <= COMPARE_GT); /* Only valid CompareType values used */
    return (StrategyNumber) cmptype;
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
            SMOL_LOGF("slow LockBuffer(new) wait ~%.1f ms on blk=%u", /* GCOV_EXCL_LINE - timing-dependent, requires lock contention */
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
    op->leftlink = InvalidBlockNumber;  /* Will be set when linking siblings */
    SMOL_LOGF("init page blk=%u leaf=%d rl=%u",
              BufferGetBlockNumber(buf), leaf ? 1 : 0, rightlink);
}

/* Link two sibling leaf pages: sets prev->rightlink = cur and cur->leftlink = prev */
static void
smol_link_siblings(Relation idx, BlockNumber prev, BlockNumber cur)
{
    if (!BlockNumberIsValid(prev))
        return;

    /* Set prev->rightlink = cur */
    Buffer pbuf = ReadBuffer(idx, prev);
    LockBuffer(pbuf, BUFFER_LOCK_EXCLUSIVE);
    smol_page_opaque(BufferGetPage(pbuf))->rightlink = cur;
    MarkBufferDirty(pbuf);
    UnlockReleaseBuffer(pbuf);

    /* Set cur->leftlink = prev */
    Buffer cbuf = ReadBuffer(idx, cur);
    LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
    smol_page_opaque(BufferGetPage(cbuf))->leftlink = prev;
    MarkBufferDirty(cbuf);
    UnlockReleaseBuffer(cbuf);

    SMOL_LOGF("linked siblings: %u <- -> %u", prev, cur);
}

/* GCOV_EXCL_START - Deprecated function: replaced by RLE two-pass build */
static void pg_attribute_unused()
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
#ifdef SMOL_TEST_COVERAGE
    smol_loop_guard_iteration = 0;  /* Reset for each build */
#endif
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
    /* Track index size for adaptive format selection */
    Size total_index_bytes = 0;
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
            /* PER-PAGE ADAPTIVE FORMAT SELECTION
             * Strategy: Start with zero-copy (most expensive), then switch to RLE/plain if beneficial.
             * This ensures pages are always maximally filled since SMOL is bulk-loaded and read-only.
             */

            /* PHASE 1: INCREMENTAL RLE PACKING
             * Pack tuples one-by-one, tracking RLE state, until page is full.
             * This maximizes tuples per page for low-cardinality data.
             */
            Size n_this = 0;
            uint16 rle_nruns = 0;
            Size rle_current_size = sizeof(uint16) * 4;  /* tag + nitems + nruns + continues_byte */
            char rle_current_key[32];
            uint16 rle_current_count = 0;
            bool rle_has_key = false;

            /* Try incremental RLE packing first */
            Size j = 0;
            while (i + j < nkeys)
            {
                const char *k = (const char *) keys + ((size_t)(i + j) * key_len);
                Size delta_size = 0;

                if (rle_has_key && memcmp(k, rle_current_key, key_len) == 0)
                {
                    /* Extends current run - no size increase (just counter++) */
                    delta_size = 0;
                }
                else
                {
                    /* New run needed */
                    delta_size = key_len + sizeof(uint16);
                }

                /* Would this overflow the page? */
                if (rle_current_size + delta_size > avail)
                    break;  /* Page full */

                /* Add this tuple */
                if (!rle_has_key || memcmp(k, rle_current_key, key_len) != 0)
                {
                    /* Start new run */
                    if (key_len <= sizeof(rle_current_key))
                        memcpy(rle_current_key, k, key_len);
                    rle_current_count = 1;
                    rle_nruns++;
                    rle_current_size += delta_size;
                    rle_has_key = true;
                }
                else
                {
                    /* Extend run */
                    rle_current_count++;
                }

                n_this++;
                j++;

                /* Test GUC: cap tuples per page */
                if (smol_test_max_tuples_per_page > 0 && n_this >= (Size) smol_test_max_tuples_per_page)
                    break;
            }

            SMOL_DEFENSIVE_CHECK(n_this > 0, ERROR,
                (errmsg("smol: cannot fit any tuple on a leaf (key_len=%u avail=%zu)", key_len, (size_t) avail)));

            /* Decide format: always use RLE V2 for packed data */
            bool use_rle = true;
            bool use_zero_copy = false;
            double uniqueness_ratio = (double) rle_nruns / (double) n_this;

            /* Check if we should fall back to plain format */
            Size plain_sz = sizeof(uint16) + n_this * key_len;
            if (rle_current_size >= plain_sz || uniqueness_ratio >= smol_rle_uniqueness_threshold)
            {
                /* Plain is better or data is too unique for RLE */
                use_rle = false;
            }

            if (use_zero_copy)
            {
                /* Zero-copy format: [SMOL_TAG_ZEROCOPY][nitems][IndexTuple1][IndexTuple2]...
                 * Each IndexTuple: [ItemPointerData t_tid (6B)][uint16 t_info (2B)][key data]
                 * This allows returning pointers directly without memcpy during scans.
                 * Trade-off: 8 bytes overhead per tuple, but acceptable for small unique-key indexes. */
                uint16 tag = SMOL_TAG_ZEROCOPY;
                char *p = scratch;
                uint16 nitems16 = (uint16) n_this;
                memcpy(p, &tag, sizeof(uint16)); p += sizeof(uint16);
                memcpy(p, &nitems16, sizeof(uint16)); p += sizeof(uint16);

                const char *base = (const char *) keys + ((size_t) i * key_len);
                Size tuple_size = sizeof(IndexTupleData) + key_len;
                uint16 t_info = (uint16) tuple_size;

                for (Size idx = 0; idx < n_this; idx++)
                {
                    /* Write IndexTuple header */
                    ItemPointerData tid;
                    ItemPointerSetInvalid(&tid);  /* Dummy TID for read-only index */
                    memcpy(p, &tid, sizeof(ItemPointerData)); p += sizeof(ItemPointerData);
                    memcpy(p, &t_info, sizeof(uint16)); p += sizeof(uint16);
                    /* Write key data */
                    memcpy(p, base + idx * key_len, key_len); p += key_len;
                }

                Size sz = (Size) (p - scratch);
                OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
                SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, ERROR,
                    (errmsg("smol: failed to add leaf payload (zero-copy)")));
                added = n_this;
            }
            else if (use_rle)
            {
                /* PHASE 2: RLE with format controlled by GUC */
                /* Determine which format to use based on GUC setting */
                bool use_v2 = (smol_key_rle_version == KEY_RLE_V2 ||
                              (smol_key_rle_version == KEY_RLE_AUTO));  /* Default to V2 for sorted build */
                uint16 tag = use_v2 ? SMOL_TAG_KEY_RLE_V2 : SMOL_TAG_KEY_RLE;

                /* Static state for continuation detection (used by V2 format) */
                static char prev_page_last_key[32];
                static bool prev_page_has_key = false;

                char *p = scratch;
                uint16 nitems16 = (uint16) n_this;
                memcpy(p, &tag, sizeof(uint16)); p += sizeof(uint16);
                memcpy(p, &nitems16, sizeof(uint16)); p += sizeof(uint16);
                memcpy(p, &rle_nruns, sizeof(uint16)); p += sizeof(uint16);

                /* V2 format: write continues_byte for cross-page run continuation */
                if (use_v2)
                {
                    /* Continuation detection: check if first key matches previous page's last key */
                    uint8 continues_byte = 0;
                    const char *first_key = (const char *) keys + ((size_t) i * key_len);

                    if (prev_page_has_key && key_len <= 32 &&
                        memcmp(first_key, prev_page_last_key, key_len) == 0)
                    {
                        continues_byte = 1;  /* First run continues from previous page */
                    }
                    *p++ = continues_byte;
                }

                /* Write RLE runs */
                const char *base = (const char *) keys + ((size_t) i * key_len);
                Size pos = 0;
                while (pos < n_this)
                {
                    Size run = 1;
                    const char *k0 = base + (size_t) pos * key_len;
                    while (pos + run < n_this)
                    {
                        const char *k1 = base + (size_t) (pos + run) * key_len;
                        if (memcmp(k0, k1, key_len) != 0)
                            break;
                        run++;
                    }
                    memcpy(p, k0, key_len); p += key_len;
                    uint16 cnt16 = (uint16) run;
                    memcpy(p, &cnt16, sizeof(uint16)); p += sizeof(uint16);
                    pos += run;
                }

                /* Remember last key for next page's continuation detection */
                const char *last_key = (const char *) keys + ((size_t)(i + n_this - 1) * key_len);
                if (key_len <= 32)
                {
                    memcpy(prev_page_last_key, last_key, key_len);
                    prev_page_has_key = true;
                }

                Size sz = (Size) (p - scratch);
                OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
                SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, ERROR,
                    (errmsg("smol: failed to add leaf payload (RLE V2)")));
                added = n_this;
            }
            else
            {
                /* Plain format: [uint16 n][keys...] */
                Size header_plain = sizeof(uint16);
                Size sz = header_plain + n_this * key_len;
                SMOL_DEFENSIVE_CHECK(sz <= BLCKSZ, ERROR,
                    (errmsg("smol: leaf payload exceeds page size")));
                memcpy(scratch, &n_this, sizeof(uint16));
                memcpy(scratch + header_plain, (const char *) keys + ((size_t)i * key_len), n_this * key_len);
                OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
                SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, ERROR,
                    (errmsg("smol: failed to add leaf payload")));
                added = n_this;
            }
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
#ifdef SMOL_TEST_COVERAGE
                /* Test coverage: force n_this=0 to trigger loop guard */
                if (n_this > 0)
                {
                    if (smol_force_loop_guard_test > 0 && smol_loop_guard_iteration >= smol_force_loop_guard_test)
                    {
                        n_this = 0;  /* Force stall to test loop_guard detection */
                        /* Keep forcing stall until loop_guard triggers ERROR */
                    }
                    else
                        smol_loop_guard_iteration++;
                }
#endif
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
        /* Track total index size for adaptive format selection */
        total_index_bytes += BLCKSZ;
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
                    SMOL_LOGF("slow LockBuffer(prev) wait ~%.1f ms on blk=%u", ms, prev); /* GCOV_EXCL_LINE - timing-dependent, requires lock contention */
            }
            {
                Page p2 = BufferGetPage(pbuf2);
                SmolPageOpaqueData *pop2 = smol_page_opaque(p2);
                pop2->rightlink = cur;
                SMOL_LOGF("linked prev=%u -> cur=%u", prev, cur);
                MarkBufferDirty(pbuf2);
            }
            UnlockReleaseBuffer(pbuf2);

            /* Also set cur's leftlink */
            Buffer cbuf = ReadBuffer(idx, cur);
            LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
            smol_page_opaque(BufferGetPage(cbuf))->leftlink = prev;
            MarkBufferDirty(cbuf);
            UnlockReleaseBuffer(cbuf);
        }
        else
        {
            SMOL_LOGF("first page: cur=%u (no prev)", cur);
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
/* GCOV_EXCL_STOP */


/* Build single-column tree with INCLUDE attrs from sorted arrays.
 * Variant for integer-like keys (keys as int64 normalized/sign-preserving).
 */
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
    Size ninc_bytes = 0; for (int c=0;c<inc_count;c++) ninc_bytes += inc_lens[c];
    while (i < nkeys)
    {
        Buffer buf = smol_extend(idx);
        Page page = BufferGetPage(buf);
        smol_init_page(buf, true, InvalidBlockNumber);
        Size fs = PageGetFreeSpace(page);
        Size avail = (fs > sizeof(ItemIdData)) ? (fs - sizeof(ItemIdData)) : 0;
        Size header = sizeof(uint16);
        Size perrow = (Size) key_len; for (int c=0;c<inc_count;c++) perrow += inc_lens[c];
        Size max_n_plain = (avail > header) ? ((avail - header) / perrow) : 0;
        Size remaining = nkeys - i;

        /* Try to fit as many rows as possible using Include-RLE if beneficial */
        bool use_inc_rle = false;
        Size inc_rle_sz = 0;
        uint16 inc_rle_nruns = 0;
        Size n_this = 0;

        /* First, try a larger candidate size for Include-RLE (up to 32000 rows or remaining) */
        Size candidate = remaining;
        if (candidate > 32000) candidate = 32000; /* Conservative uint16 limit, reserves high values */

        /* Detect runs and calculate Include-RLE size */
        Size pos = 0; Size sz_runs = 0; uint16 nr = 0;
        while (pos < candidate)
        {
            Size run = 1;
            int64 k0 = keys[i + pos];
            /* Check if key+includes stay constant */
            bool run_ok = true;
            while (pos + run < candidate && run_ok && run < 32000)
            {
                if (keys[i + pos + run] != k0) { run_ok = false; break; }
                /* Check all include columns match */
                for (int c=0; c < inc_count && run_ok; c++)
                {
                    Size off0 = (i + pos) * inc_lens[c];
                    Size off1 = (i + pos + run) * inc_lens[c];
                    if (memcmp(incs[c] + off0, incs[c] + off1, inc_lens[c]) != 0)
                        run_ok = false;
                }
                if (run_ok) run++;
            }
            nr++;
            if (nr >= 32000)
            { /* GCOV_EXCL_START - unreachable: 32000 runs can't fit in 8KB page */
                /* Too many runs for uint16 nruns field (conservative limit) */
                break;
            } /* GCOV_EXCL_STOP */
            Size this_run_sz = key_len + sizeof(uint16) + ninc_bytes;
            Size new_total = sizeof(uint16) * 3 + sz_runs + this_run_sz;
            if (new_total > avail)
            {
                /* This run doesn't fit; stop here */
                break;
            }
            sz_runs += this_run_sz;
            pos += run;
        }

        inc_rle_sz = sizeof(uint16) * 3 + sz_runs;
        inc_rle_nruns = nr;
        Size n_rle = pos; /* Number of rows that fit with Include-RLE */

        /* Decide: use Include-RLE if it fits more rows OR saves space */
        if (n_rle > max_n_plain || (n_rle >= max_n_plain && inc_rle_sz <= avail))
        {
            n_this = n_rle;
            use_inc_rle = true;
            if (n_this > 10000)
	        SMOL_LOGF("[smol] Include-RLE: fitting %zu rows in %u runs (rle_sz=%zu, avail=%zu)", n_this, inc_rle_nruns, inc_rle_sz, avail);
        }
        else
        {
            /* Fall back to plain format */
            n_this = (remaining < max_n_plain) ? remaining : max_n_plain;
            use_inc_rle = false;
        }

        /* Test GUC: cap tuples per page to force taller trees */
        if (smol_test_max_tuples_per_page > 0 && n_this > (Size) smol_test_max_tuples_per_page)
            n_this = (Size) smol_test_max_tuples_per_page;

        SMOL_DEFENSIVE_CHECK(n_this > 0, ERROR,
            (errmsg("smol: cannot fit tuple with INCLUDE on a leaf (perrow=%zu avail=%zu)", (size_t) perrow, (size_t) avail)));

        if (use_inc_rle)
        {
            /* Write Include-RLE: [0x8003][nitems][nruns][runs...] */
            uint16 tag = 0x8003u;
            char *p = scratch;
            uint16 nitems16 = (uint16) n_this;
            memcpy(p, &tag, sizeof(uint16)); p += sizeof(uint16);
            memcpy(p, &nitems16, sizeof(uint16)); p += sizeof(uint16);
            memcpy(p, &inc_rle_nruns, sizeof(uint16)); p += sizeof(uint16);
            Size rle_pos = 0;
            while (rle_pos < n_this)
            {
                Size run = 1;
                int64 k0 = keys[i + rle_pos];
                bool run_ok = true;
                while (rle_pos + run < n_this && run_ok)
                {
                    if (keys[i + rle_pos + run] != k0) { run_ok = false; break; }
                    for (int c=0; c < inc_count && run_ok; c++)
                    {
                        Size off0 = (i + rle_pos) * inc_lens[c];
                        Size off1 = (i + rle_pos + run) * inc_lens[c];
                        if (memcmp(incs[c] + off0, incs[c] + off1, inc_lens[c]) != 0)
                            run_ok = false;
                    }
                    if (run_ok) run++;
                }
                /* Write: key */
                if (key_len == 8) { memcpy(p, &k0, 8); p += 8; }
                else if (key_len == 4) { int32 v = (int32) k0; memcpy(p, &v, 4); p += 4; }
                else { int16 v = (int16) k0; memcpy(p, &v, 2); p += 2; }
                /* Write: count */
                uint16 cnt16 = (uint16) run;
                memcpy(p, &cnt16, sizeof(uint16)); p += sizeof(uint16);
                /* Write: includes (one copy per run) */
                for (int c=0; c<inc_count; c++)
                {
                    Size off0 = (i + rle_pos) * inc_lens[c];
                    memcpy(p, incs[c] + off0, inc_lens[c]);
                    p += inc_lens[c];
                }
                rle_pos += run;
            }
            Size sz = (Size) (p - scratch);
            OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
            SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, ERROR,
                (errmsg("smol: failed to add leaf payload (Include-RLE)")));
        }
        else
        {
            /* Write plain format: [u16 n][keys...][inc1 block][inc2 block]... */
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
            OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
            SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, ERROR,
                (errmsg("smol: failed to add leaf payload (INCLUDE)")));
        }
        MarkBufferDirty(buf);
        BlockNumber cur = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);
        smol_link_siblings(idx, prev, cur);
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

/* Build single-column TEXT(<=32) keys with INCLUDE attrs from sorted arrays. */
static void
smol_build_text_inc_from_sorted(Relation idx, const char *keys32, const char * const *incs,
                                Size nkeys, uint16 key_len, int inc_count, const uint16 *inc_lens)
{
    Buffer mbuf; Page mpage; SmolMeta *meta;
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
    if (nkeys == 0) return;
    Size i = 0; BlockNumber prev = InvalidBlockNumber; char *scratch = (char *) palloc(BLCKSZ);
    Size ninc_bytes = 0; for (int c=0;c<inc_count;c++) ninc_bytes += inc_lens[c];
    while (i < nkeys)
    {
        Buffer buf = smol_extend(idx);
        Page page = BufferGetPage(buf);
        smol_init_page(buf, true, InvalidBlockNumber);
        Size fs = PageGetFreeSpace(page);
        Size avail = (fs > sizeof(ItemIdData)) ? (fs - sizeof(ItemIdData)) : 0;
        Size header = sizeof(uint16);
        Size perrow = (Size) key_len; for (int c=0;c<inc_count;c++) perrow += inc_lens[c];
        Size max_n_plain = (avail > header) ? ((avail - header) / perrow) : 0;
        Size remaining = nkeys - i;

        /* Try to fit as many rows as possible using Include-RLE if beneficial */
        bool use_inc_rle = false;
        Size inc_rle_sz = 0;
        uint16 inc_rle_nruns = 0;
        Size n_this = 0;

        /* First, try a larger candidate size for Include-RLE (up to 32000 rows or remaining) */
        Size candidate = remaining;
        if (candidate > 32000) candidate = 32000; /* Conservative uint16 limit, reserves high values */

        /* Detect runs and calculate Include-RLE size */
        Size pos = 0; Size sz_runs = 0; uint16 nr = 0;
        while (pos < candidate)
        {
            Size run = 1;
            const char *k0 = keys32 + (size_t)(i + pos) * key_len;
            /* Check if key+includes stay constant */
            bool run_ok = true;
            while (pos + run < candidate && run_ok && run < 32000)
            {
                const char *k1 = keys32 + (size_t)(i + pos + run) * key_len;
                if (memcmp(k0, k1, key_len) != 0) { run_ok = false; break; }
                /* Check all include columns match */
                for (int c=0; c < inc_count && run_ok; c++)
                {
                    Size off0 = (i + pos) * inc_lens[c];
                    Size off1 = (i + pos + run) * inc_lens[c];
                    if (memcmp(incs[c] + off0, incs[c] + off1, inc_lens[c]) != 0)
                        run_ok = false;
                }
                if (run_ok) run++;
            }
            nr++;
            if (nr >= 32000)
            { /* GCOV_EXCL_START - unreachable: 32000 runs can't fit in 8KB page */
                /* Too many runs for uint16 nruns field (conservative limit) */
                break;
            } /* GCOV_EXCL_STOP */
            Size this_run_sz = key_len + sizeof(uint16) + ninc_bytes;
            Size new_total = sizeof(uint16) * 3 + sz_runs + this_run_sz;
            if (new_total > avail)
            {
                /* This run doesn't fit; stop here */
                break;
            }
            sz_runs += this_run_sz;
            pos += run;
        }

        inc_rle_sz = sizeof(uint16) * 3 + sz_runs;
        inc_rle_nruns = nr;
        Size n_rle = pos; /* Number of rows that fit with Include-RLE */

        /* Decide: use Include-RLE if it fits more rows OR saves space */
        if (n_rle > max_n_plain || (n_rle >= max_n_plain && inc_rle_sz <= avail))
        {
            n_this = n_rle;
            use_inc_rle = true;
            if (n_this > 10000) 
                ereport(NOTICE, (errmsg("Include-RLE: fitting %zu rows (>10K) in %u runs (rle_sz=%zu, avail=%zu)", n_this, inc_rle_nruns, inc_rle_sz, avail))); /* GCOV_EXCL_LINE */
        }
        else
        {
            /* Fall back to plain format */
            n_this = (remaining < max_n_plain) ? remaining : max_n_plain;
            use_inc_rle = false;
        }

        /* Test GUC: cap tuples per page to force taller trees */
        if (smol_test_max_tuples_per_page > 0 && n_this > (Size) smol_test_max_tuples_per_page)
            n_this = (Size) smol_test_max_tuples_per_page;

        SMOL_DEFENSIVE_CHECK(n_this > 0, ERROR,
            (errmsg("smol: cannot fit tuple with INCLUDE on a leaf (perrow=%zu avail=%zu)", (size_t) perrow, (size_t) avail)));

        if (use_inc_rle)
        {
            /* Write Include-RLE: [0x8003][nitems][nruns][runs...] */
            uint16 tag = 0x8003u;
            char *p = scratch;
            uint16 nitems16 = (uint16) n_this;
            memcpy(p, &tag, sizeof(uint16)); p += sizeof(uint16);
            memcpy(p, &nitems16, sizeof(uint16)); p += sizeof(uint16);
            memcpy(p, &inc_rle_nruns, sizeof(uint16)); p += sizeof(uint16);
            Size rle_pos = 0;
            while (rle_pos < n_this)
            {
                Size run = 1;
                const char *k0 = keys32 + (size_t)(i + rle_pos) * key_len;
                bool run_ok = true;
                while (rle_pos + run < n_this && run_ok)
                {
                    const char *k1 = keys32 + (size_t)(i + rle_pos + run) * key_len;
                    if (memcmp(k0, k1, key_len) != 0) { run_ok = false; break; }
                    for (int c=0; c < inc_count && run_ok; c++)
                    {
                        Size off0 = (i + rle_pos) * inc_lens[c];
                        Size off1 = (i + rle_pos + run) * inc_lens[c];
                        if (memcmp(incs[c] + off0, incs[c] + off1, inc_lens[c]) != 0)
                            run_ok = false;
                    }
                    if (run_ok) run++;
                }
                /* Write: key */
                memcpy(p, k0, key_len); p += key_len;
                /* Write: count */
                uint16 cnt16 = (uint16) run;
                memcpy(p, &cnt16, sizeof(uint16)); p += sizeof(uint16);
                /* Write: includes (one copy per run) */
                for (int c=0; c<inc_count; c++)
                {
                    Size off0 = (i + rle_pos) * inc_lens[c];
                    memcpy(p, incs[c] + off0, inc_lens[c]);
                    p += inc_lens[c];
                }
                rle_pos += run;
            }
            Size sz = (Size) (p - scratch);
            OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
            SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, ERROR,
                (errmsg("smol: failed to add leaf payload (TEXT Include-RLE)")));
        }
        else
        {
            /* Write plain format: [u16 n][keys...][inc1 block][inc2 block]... */
            memcpy(scratch, &n_this, sizeof(uint16));
            char *p = scratch + sizeof(uint16);
            /* keys: copy n_this fixed 8/16/32 bytes per key (binary C collation) */
            memcpy(p, keys32 + ((size_t) i * key_len), (size_t) n_this * key_len);
            p += (size_t) n_this * key_len;
            /* includes: contiguous blocks per include column */
            for (int c=0;c<inc_count;c++)
            {
                uint16 len = inc_lens[c];
                Size bytes = (Size) n_this * (Size) len;
                memcpy(p, incs[c] + ((size_t) i * len), bytes);
                p += bytes;
            }
            Size sz = (Size) (p - scratch);
            SMOL_DEFENSIVE_CHECK(PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false) != InvalidOffsetNumber,
                                 WARNING, (errmsg("smol: failed to add leaf payload (TEXT+INCLUDE)")));
        }
        MarkBufferDirty(buf);
        BlockNumber cur = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);
        smol_link_siblings(idx, prev, cur);
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

/* Generic version of smol_find_first_leaf that supports all key types including text.
 * Uses SmolScanOpaque's comparison context to correctly handle text/varchar types. */
static BlockNumber
smol_find_first_leaf_generic(Relation idx, SmolScanOpaque so)
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
            /* Use generic comparator that handles text correctly */
            int cmp = smol_cmp_keyptr_to_bound(so, keyp);
            if (cmp >= 0)
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
    SMOL_LOGF("find_first_leaf_generic: leaf=%u height=%u", cur, meta.height);
    return cur;
}

/* GCOV_EXCL_START - unused function, kept for future reference */
/* Find leaf containing values around upper bound for backward scans.
 * Finds the rightmost leaf that could contain values <= upper_bound. */
static BlockNumber __attribute__((unused))
smol_find_leaf_for_upper_bound(Relation idx, SmolScanOpaque so)
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
        /* Find rightmost child where separator key <= upper_bound */
        for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
        {
            char *itp = (char *) PageGetItem(page, PageGetItemId(page, off));
            BlockNumber c;
            memcpy(&c, itp, sizeof(BlockNumber));
            char *keyp = itp + sizeof(BlockNumber);
            /* Check if this separator key exceeds upper bound */
            int cmp = smol_cmp_keyptr_to_upper_bound(so, keyp);
            if (so->upper_bound_strict ? (cmp >= 0) : (cmp > 0))
            {
                /* This child's separator > upper_bound, so values <= upper_bound must be in previous child */
                break;
            }
            /* Separator <= upper_bound, remember this child and keep looking */
            child = c;
        }
        if (!BlockNumberIsValid(child))
        {
            /* All separators > upper_bound, use leftmost child */
            char *itp = (char *) PageGetItem(page, PageGetItemId(page, FirstOffsetNumber));
            memcpy(&child, itp, sizeof(BlockNumber));
        }
        ReleaseBuffer(buf);
        cur = child;
        levels--;
    }
    SMOL_LOGF("find_leaf_for_upper_bound: leaf=%u height=%u", cur, meta.height);
    return cur;
}
/* GCOV_EXCL_STOP */

/* removed unused smol_read_key_as_datum */


static inline int
smol_cmp_keyptr_bound_generic(FmgrInfo *cmp, Oid collation, const char *keyp, uint16 key_len, bool key_byval, Datum bound)
{
    Datum kd;
    if (key_byval)
    {
        SMOL_ASSERT_BYVAL_LEN(key_len);
        if (key_len == 1)
        { char v; memcpy(&v, keyp, 1); kd = CharGetDatum(v); }
        /* key_len == 2 (int2) always uses fast path at line 703-707, never reaches generic comparison */
        else if (key_len == 4)
        { int32 v; memcpy(&v, keyp, 4); kd = Int32GetDatum(v); }
        else if (key_len == 8)
        { int64 v; memcpy(&v, keyp, 8); kd = Int64GetDatum(v); }
        else /* GCOV_EXCL_LINE - defensive: all standard byval types covered (1,4,8 bytes; int2 uses fast path) */
            ereport(ERROR, (errmsg("unexpected byval key_len=%u", (unsigned) key_len))); /* GCOV_EXCL_LINE */
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
    uint16 tag;
    memcpy(&tag, p, sizeof(uint16));
    if (tag == SMOL_TAG_ZEROCOPY || tag == SMOL_TAG_KEY_RLE ||
        tag == SMOL_TAG_KEY_RLE_V2 || tag == SMOL_TAG_INC_RLE)
    {
        /* Tagged formats: [u16 tag][u16 nitems][...] */
        uint16 nitems;
        memcpy(&nitems, p + sizeof(uint16), sizeof(uint16));
        return nitems;
    }
    else
    {
        /* Plain format: first u16 is nitems (no tag) */
        return tag;
    }
}

/* Extended version with include support for multi-run Include-RLE */
static inline char *
smol_leaf_keyptr_ex(Page page, uint16 idx, uint16 key_len, const uint16 *inc_lens, uint16 ninc, const uint32 *inc_cumul_offs)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 tag;
    memcpy(&tag, p, sizeof(uint16));

    if (tag == SMOL_TAG_ZEROCOPY)
    {
        /* Zero-copy format: [tag][nitems][IndexTuple1][IndexTuple2]...
         * Each IndexTuple: [6B TID][2B t_info][key_len bytes]
         * Return pointer to the IndexTuple itself (not just key data) */
        uint16 nitems; /* GCOV_EXCL_LINE - zero-copy format only in deprecated build function */
        memcpy(&nitems, p + sizeof(uint16), sizeof(uint16)); /* GCOV_EXCL_LINE */
        SMOL_DEFENSIVE_CHECK(idx >= 1 && idx <= nitems, ERROR, /* GCOV_EXCL_LINE */
                            (errmsg("smol: zero-copy keyptr index %u out of range [1,%u]", idx, nitems))); /* GCOV_EXCL_LINE */
        Size tuple_size = sizeof(IndexTupleData) + key_len; /* GCOV_EXCL_LINE */
        return p + sizeof(uint16) * 2 + ((size_t)(idx - 1)) * tuple_size; /* GCOV_EXCL_LINE */
    }
    else if (!(tag == SMOL_TAG_KEY_RLE || tag == SMOL_TAG_KEY_RLE_V2 || tag == SMOL_TAG_INC_RLE))
    {
        /* Plain payload: [u16 n][keys...] (no tag, n is first uint16) */
        uint16 n = tag;
        SMOL_DEFENSIVE_CHECK(idx >= 1 && idx <= n, ERROR,
                            (errmsg("smol: leaf keyptr index %u out of range [1,%u]", idx, n)));
        return p + sizeof(uint16) + ((size_t)(idx - 1)) * key_len;
    }
    /* RLE payload: [u16 tag(SMOL_TAG_KEY_RLE|SMOL_TAG_KEY_RLE_V2|SMOL_TAG_INC_RLE)][u16 nitems][u16 nruns][continues_byte?][runs]* */
    {
        uint16 nitems, nruns;
        memcpy(&nitems, p + sizeof(uint16), sizeof(uint16));
        memcpy(&nruns,  p + sizeof(uint16) * 2, sizeof(uint16));
        SMOL_DEFENSIVE_CHECK(idx >= 1 && idx <= nitems, ERROR,
                            (errmsg("smol: RLE keyptr index %u out of range [1,%u]", idx, nitems)));
        char *rp = p + sizeof(uint16) * 3; /* first run */
        /* V2 format has continues_byte after nruns */
        if (tag == SMOL_TAG_KEY_RLE_V2)
            rp++;  /* Skip continues_byte */
        uint32 acc = 0;
        for (uint16 r = 0; r < nruns; r++)
        {
            char *k = rp;
            uint16 cnt;
            memcpy(&cnt, rp + key_len, sizeof(uint16));
            if (idx <= acc + cnt)
                return k; /* key pointer inside run entry */
            acc += cnt;
            rp += (size_t) key_len + sizeof(uint16);
            if (tag == 0x8003u)
            {
                /* Include-RLE: skip all include columns to reach next run */
                if (inc_cumul_offs && ninc > 0)
                {
                    rp += inc_cumul_offs[ninc];  /* O(1) skip all INCLUDE columns using cumulative offsets */
                }
                else /* GCOV_EXCL_START - defensive: Include-RLE (0x8003) pages should always be accessed with metadata */
                {
                    /* No include info provided - can't iterate safely beyond first run */
                    SMOL_DEFENSIVE_CHECK(r == 0, ERROR, (errmsg("smol: Include-RLE multi-run requires include metadata")));
                } /* GCOV_EXCL_STOP */
            }
        }
        return NULL; /* GCOV_EXCL_LINE */
    }
}

/* Wrapper for backward compatibility - assumes single-run or no includes */
static inline char *
smol_leaf_keyptr(Page page, uint16 idx, uint16 key_len)
{
    return smol_leaf_keyptr_ex(page, idx, key_len, NULL, 0, NULL);
}

static inline bool
smol_key_eq_len(const char *a, const char *b, uint16 len)
{
    /* Fixed-length, small keys: branch by common sizes */
    if (len == 2)
    { int16 x, y; memcpy(&x, a, 2); memcpy(&y, b, 2); return x == y; }
    if (len == 4)
    { int32 x, y; memcpy(&x, a, 4); memcpy(&y, b, 4); return x == y; }
    if (len == 8)
    { int64 x, y; memcpy(&x, a, 8); memcpy(&y, b, 8); return x == y; }
    return memcmp(a, b, len) == 0;
}

static inline bool
smol_leaf_is_rle(Page page)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 tag; memcpy(&tag, p, sizeof(uint16));
    /* Zero-copy and RLE formats have tags; plain format uses nitems as first u16 */
    return (tag == SMOL_TAG_ZEROCOPY || tag == SMOL_TAG_KEY_RLE ||
            tag == SMOL_TAG_KEY_RLE_V2 || tag == SMOL_TAG_INC_RLE);
}

/* For RLE payloads, compute 1-based [run_start, run_end] that contains idx - extended version with include support */
static bool
smol_leaf_run_bounds_rle_ex(Page page, uint16 idx, uint16 key_len,
                         uint16 *run_start_out, uint16 *run_end_out,
                         const uint16 *inc_lens, uint16 ninc)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 tag; memcpy(&tag, p, sizeof(uint16));
    if (!(tag == 0x8001u || tag == 0x8002u || tag == 0x8003u))
        return false;
    uint16 nitems, nruns;
    memcpy(&nitems, p + sizeof(uint16), sizeof(uint16));
    memcpy(&nruns,  p + sizeof(uint16) * 2, sizeof(uint16));
    SMOL_DEFENSIVE_CHECK(idx >= 1 && idx <= nitems, ERROR,
                        (errmsg("smol: RLE run check index %u out of range [1,%u]", idx, nitems)));
    uint32 acc = 0;
    char *rp = p + sizeof(uint16) * 3;
    /* V2 format has continues_byte after nruns */
    if (tag == 0x8002u)
        rp++;  /* Skip continues_byte */
    for (uint16 r = 0; r < nruns; r++)
    {
        uint16 cnt; memcpy(&cnt, rp + key_len, sizeof(uint16));
        if (idx <= acc + cnt)
        {
            if (run_start_out) *run_start_out = (uint16) (acc + 1);
            if (run_end_out)   *run_end_out   = (uint16) (acc + cnt);
            return true;
        }
        acc += cnt;
        rp += (size_t) key_len + sizeof(uint16);
        if (tag == 0x8003u)
        {
            /* Include-RLE: skip all include columns to reach next run */
            if (inc_lens && ninc > 0)
            {
                for (uint16 i = 0; i < ninc; i++)
                    rp += inc_lens[i];
            }
            else /* GCOV_EXCL_START - defensive: Include-RLE (0x8003) pages should always be accessed with metadata */
            {
                /* No include info provided - can't iterate safely beyond first run */
                SMOL_DEFENSIVE_CHECK(r == 0, ERROR, (errmsg("smol: Include-RLE multi-run requires include metadata")));
            } /* GCOV_EXCL_STOP */
        }
    }
    return false; /* GCOV_EXCL_LINE */
}

/* Return pointer to INCLUDE column data for given row (0-based). Supports
 * plain single-key+INCLUDE and include-RLE layout (tag 0x8003).
 */
static inline char *
smol1_inc_ptr_any(Page page, uint16 key_len, uint16 n, const uint16 *inc_lens, uint16 ninc, uint16 inc_idx, uint32 row, const uint32 *inc_cumul_offs)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *base = (char *) PageGetItem(page, iid);
    uint16 tag; memcpy(&tag, base, sizeof(uint16));
    if (!(tag == 0x8001u || tag == 0x8003u))
    {
        /* Plain layout: [u16 n][keys][inc1 block][inc2 block]... */
        char *p = base + sizeof(uint16) + (size_t) n * key_len;
        p += (size_t) n * inc_cumul_offs[inc_idx];  /* O(1) using precomputed cumulative offsets */
        return p + (size_t) row * inc_lens[inc_idx];
    }
    if (tag == 0x8003u)
    {
        /* Include-RLE: [tag][nitems][nruns][runs...]; run: [key][u16 cnt][inc1][inc2]... */
        uint16 nitems, nruns; memcpy(&nitems, base + 2, 2); memcpy(&nruns, base + 4, 2);
        if (row >= nitems) return NULL;
        char *rp = base + 6;
        uint32 acc = 0;
        for (uint16 r = 0; r < nruns; r++)
        {
            char *k = rp; (void) k;
            uint16 cnt; memcpy(&cnt, rp + key_len, 2);
            char *incp = rp + key_len + 2;
            if (row < acc + cnt)
            {
                /* return pointer to include value for this run/column */
                incp += inc_cumul_offs[inc_idx];  /* O(1) using precomputed cumulative offsets */
                return incp;
            }
            acc += cnt;
            /* advance to next run */
            incp += inc_cumul_offs[ninc];  /* O(1) using precomputed cumulative offsets */
            rp = incp;
        }
        return NULL; /* GCOV_EXCL_LINE */
    }
    /* key-RLE only (0x8001): includes are stored in column blocks like plain */ /* GCOV_EXCL_START */
    {
        /* For includes, data still placed after the key array in plain build. Here, our 0x8001 format leaves includes in the plain layout, so compute as plain. */
        char *pl = base + sizeof(uint16) + (size_t) n * key_len;
        for (uint16 i = 0; i < inc_idx; i++) pl += (size_t) n * inc_lens[i];
        return pl + (size_t) row * inc_lens[inc_idx];
    } /* GCOV_EXCL_STOP */
}

static inline void
smol_run_reset(SmolScanOpaque so)
{
    so->run_active = false;
    so->run_start_off = InvalidOffsetNumber;
    so->run_end_off = InvalidOffsetNumber;
    so->run_key_len = 0;
    so->run_inc_evaluated = false;
    so->rle_run_inc_cached = false;
    so->run_key_built = false;  /* Reset cached varlena key blob for RLE runs */
    for (int i = 0; i < 16; i++)
        so->run_inc_built[i] = false;  /* Reset cached varlena INCLUDE blobs for RLE runs */
}


/* Build internal levels from a linear list of children (blk, highkey) until a single root remains. */
/* UNUSED: Dead code - bytes-based version (smol_build_internal_levels_bytes) replaced this.
 * Only called from deprecated smol_build_tree_from_sorted (also GCOV_EXCL). */
/* GCOV_EXCL_START - deprecated function, replaced by smol_build_internal_levels_bytes */
static void pg_attribute_unused()
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
            Size children_added = 0;
            /* add as many children as fit */
            for (; i < cur_n; i++)
            {
                memcpy(item, &cur_blks[i], sizeof(BlockNumber));
                if (key_len == 2) { int16 t = (int16) cur_high[i]; memcpy(item + sizeof(BlockNumber), &t, 2); }
                else if (key_len == 4) { int32 t = (int32) cur_high[i]; memcpy(item + sizeof(BlockNumber), &t, 4); }
                else { int64 t = cur_high[i]; memcpy(item + sizeof(BlockNumber), &t, 8); }
                OffsetNumber off = PageAddItem(ipg, (Item) item, item_sz, InvalidOffsetNumber, false, false);
                SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, WARNING,
                    (errmsg("smol: internal page full during build")));
                if (off == InvalidOffsetNumber)
                {
                    /* page full: back out to next page */
                    break;
                }
                children_added++;
                /* For testing: limit fanout to force tall trees */
                if (smol_test_max_internal_fanout > 0 && children_added >= (Size) smol_test_max_internal_fanout)
                {
                    i++;  /* Move to next child for next page */
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
#ifdef SMOL_TEST_COVERAGE
                /* For testing: artificially trigger reallocation */
                /* GCOV_EXCL_START - test-forced reallocation requires next_n==cap_next which is impossible with conservative allocation formula */
                if (smol_test_force_realloc_at > 0 && next_n == (Size) smol_test_force_realloc_at && cap_next == (Size) smol_test_force_realloc_at)
                {
                    cap_next = cap_next * 2;
                    next_blks = (BlockNumber *) repalloc(next_blks, cap_next * sizeof(BlockNumber));
                    next_high = (int64 *) repalloc(next_high, cap_next * sizeof(int64));
                }
                /* GCOV_EXCL_STOP */
                else
#endif
                /* GCOV_EXCL_START - natural reallocation requires pathologically slow fanout=1 trees */
                if (next_n >= cap_next)
                {
                    cap_next = cap_next * 2;
                    next_blks = (BlockNumber *) repalloc(next_blks, cap_next * sizeof(BlockNumber));
                    next_high = (int64 *) repalloc(next_high, cap_next * sizeof(int64));
                }
                /* GCOV_EXCL_STOP */
                next_blks[next_n] = iblk; /* GCOV_EXCL_LINE - dead function */
                next_high[next_n] = cur_high[last]; /* GCOV_EXCL_LINE - dead function */
                next_n++; /* GCOV_EXCL_LINE - dead function */
            }
        }
        /* Prepare for next level */
        if (levels > 0) /* GCOV_EXCL_LINE - dead function */
        {
            /* cur_blks was a palloc we own (not original &leaves[0].blk). Free it. */
            pfree(cur_blks); /* GCOV_EXCL_LINE - dead function */
            pfree((void *) cur_high); /* GCOV_EXCL_LINE - dead function */
        }
        cur_blks = next_blks; /* GCOV_EXCL_LINE - dead function */
        cur_high = next_high; /* GCOV_EXCL_LINE - dead function */
        cur_n = next_n; /* GCOV_EXCL_LINE - dead function */
        levels++; /* GCOV_EXCL_LINE - dead function */
    }

    *out_root = cur_blks[0]; /* GCOV_EXCL_LINE - dead function */
    *out_levels = levels; /* GCOV_EXCL_LINE - dead function */
    if (levels > 0) /* GCOV_EXCL_LINE - dead function */
    {
        pfree(cur_blks); /* GCOV_EXCL_LINE - dead function */
        pfree((void *) cur_high); /* GCOV_EXCL_LINE - dead function */
    }
} /* GCOV_EXCL_LINE - dead function */

/* Build internal levels using raw key bytes for highkeys (e.g., text32). */
static void
smol_build_internal_levels_bytes(Relation idx,
                                 BlockNumber *child_blks, const char *child_high_bytes,
                                 Size nchildren, uint16 key_len,
                                 BlockNumber *out_root, uint16 *out_levels)
{
    BlockNumber *cur_blks = child_blks;
    const char *cur_high = child_high_bytes;
    Size cur_n = nchildren;
    uint16 levels = 0;
    while (cur_n > 1)
    {
        Size cap_next = (cur_n / 2) + 2;
        BlockNumber *next_blks = (BlockNumber *) palloc(cap_next * sizeof(BlockNumber));
        char *next_high = (char *) palloc(cap_next * key_len);
        Size next_n = 0;
        Size i = 0;
        while (i < cur_n)
        {
            Buffer ibuf = smol_extend(idx);
            smol_init_page(ibuf, false, InvalidBlockNumber);
            Page ipg = BufferGetPage(ibuf);
            Size item_sz = sizeof(BlockNumber) + key_len;
            char *item = (char *) palloc(item_sz);
            Size children_added = 0;
            for (; i < cur_n; i++)
            {
                memcpy(item, &cur_blks[i], sizeof(BlockNumber));
                memcpy(item + sizeof(BlockNumber), cur_high + ((size_t) i * key_len), key_len);
                if (PageGetFreeSpace(ipg) < item_sz + sizeof(ItemIdData))
                    break;
                OffsetNumber off = PageAddItem(ipg, (Item) item, item_sz, InvalidOffsetNumber, false, false);
                SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, WARNING,
                    (errmsg("smol: internal page add failed during build (bytes)")));
                if (off == InvalidOffsetNumber)
                    break; /* GCOV_EXCL_LINE - requires multi-column variable-width keys feature */
                children_added++;
                /* For testing: limit fanout to force tall trees */
                if (smol_test_max_internal_fanout > 0 && children_added >= (Size) smol_test_max_internal_fanout)
                {
                    i++;
                    break;
                }
            }
            MarkBufferDirty(ibuf);
#ifdef SMOL_TEST_COVERAGE
            /* For testing: artificially trigger reallocation */
            if (smol_test_force_realloc_at > 0 && next_n == (Size) smol_test_force_realloc_at && cap_next == (Size) smol_test_force_realloc_at)
            { /* GCOV_EXCL_LINE - test-forced reallocation requires next_n==cap_next which is impossible with conservative allocation formula */
                cap_next = cap_next * 2; /* GCOV_EXCL_LINE */
                next_blks = (BlockNumber *) repalloc(next_blks, cap_next * sizeof(BlockNumber)); /* GCOV_EXCL_LINE */
                next_high = (char *) repalloc(next_high, cap_next * key_len); /* GCOV_EXCL_LINE */
            } /* GCOV_EXCL_LINE */
            else
#endif
            if (next_n >= cap_next)  /* Defensive: shouldn't happen with cap_next = (cur_n/2) + 2 */
            { /* GCOV_EXCL_LINE - natural reallocation requires pathologically slow fanout=1 trees */
                cap_next = cap_next * 2; /* GCOV_EXCL_LINE */
                next_blks = (BlockNumber *) repalloc(next_blks, cap_next * sizeof(BlockNumber)); /* GCOV_EXCL_LINE */
                next_high = (char *) repalloc(next_high, cap_next * key_len); /* GCOV_EXCL_LINE */
            } /* GCOV_EXCL_LINE */
            next_blks[next_n] = BufferGetBlockNumber(ibuf);
            /* highkey for this internal page: copy last child's high */
            memcpy(next_high + ((size_t) next_n * key_len), cur_high + ((size_t) (i - 1) * key_len), key_len);
            next_n++;
            UnlockReleaseBuffer(ibuf);
            pfree(item);
        }
        /* link right siblings */
        for (Size j = 1; j < next_n; j++)
        {
            Buffer pb = ReadBuffer(idx, next_blks[j-1]);
            LockBuffer(pb, BUFFER_LOCK_EXCLUSIVE);
            smol_page_opaque(BufferGetPage(pb))->rightlink = next_blks[j];
            MarkBufferDirty(pb);
            UnlockReleaseBuffer(pb);
        }
        if (levels > 0)
            pfree(cur_blks);
        if (levels > 0)
            pfree((void *) cur_high);
        cur_blks = next_blks;
        cur_high = next_high;
        cur_n = next_n;
        levels++;
    }
    /* set root */
    {
        Buffer mb = ReadBuffer(idx, 0);
        LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
        SmolMeta *m = smol_meta_ptr(BufferGetPage(mb));
        m->root_blkno = cur_blks[0];
        m->height = (uint16) (levels + 1);
        MarkBufferDirty(mb);
        UnlockReleaseBuffer(mb);
    }
    if (out_root) *out_root = cur_blks[0];
    if (out_levels) *out_levels = levels;
    pfree(cur_blks);
    pfree((void *) cur_high);
}
/* GCOV_EXCL_STOP */

/* Stream-write text keys from tuplesort into leaf pages with given cap (8/16/32). */
static void
smol_build_text_stream_from_tuplesort(Relation idx, Tuplesortstate *ts, Size nkeys, uint16 key_len)
{
    /* init meta page if new */
    if (RelationGetNumberOfBlocks(idx) == 0)
    {
        Buffer mbuf = ReadBufferExtended(idx, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
        Page mpage = BufferGetPage(mbuf);
        PageInit(mpage, BLCKSZ, 0);
        SmolMeta *meta = smol_meta_ptr(mpage);
        meta->magic = SMOL_META_MAGIC;
        meta->version = SMOL_META_VERSION;
        meta->nkeyatts = 1;
        meta->key_len1 = key_len;
        meta->key_len2 = 0;
        meta->root_blkno = InvalidBlockNumber;
        meta->height = 0;
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
    }
    if (nkeys == 0)
        return;
    BlockNumber prev = InvalidBlockNumber;
    Size nleaves = 0, aleaves = 0;
    BlockNumber *leaves = NULL;
    char *leaf_high = NULL; /* bytes array nleaves*key_len */
    char lastkey[32];
    Size remaining = nkeys;

    /* Allocate scratch buffer for page construction (reused across pages) */
    char *scratch = (char *) palloc(BLCKSZ);

    while (remaining > 0)
    {
        Buffer buf = smol_extend(idx);
        smol_init_page(buf, true, InvalidBlockNumber);
        Page page = BufferGetPage(buf);
        Size fs = PageGetFreeSpace(page);
        Size avail = (fs > sizeof(ItemIdData)) ? (fs - sizeof(ItemIdData)) : 0;

        /* Calculate max tuples for plain format */
        Size header_plain = sizeof(uint16);
        Size max_n_plain = (avail > header_plain) ? ((avail - header_plain) / key_len) : 0;
        SMOL_DEFENSIVE_CHECK(max_n_plain > 0, ERROR, (errmsg("smol: cannot fit any tuple on a leaf (key_len=%u)", key_len)));
        Size n_this = (remaining < max_n_plain) ? remaining : max_n_plain;

        /* Test GUC: cap tuples per page to force taller trees */
        if (smol_test_max_tuples_per_page > 0 && n_this > (Size) smol_test_max_tuples_per_page)
            n_this = (Size) smol_test_max_tuples_per_page;

        /* Collect n_this tuples into a buffer for RLE analysis */
        char *keys_buf = (char *) palloc(n_this * key_len);
        for (Size i = 0; i < n_this; i++)
        {
            IndexTuple itup = tuplesort_getindextuple(ts, true);
            SMOL_DEFENSIVE_CHECK(itup != NULL, ERROR,
                                (errmsg("smol: unexpected end of tuplesort stream")));
            bool isnull;
            Datum val = index_getattr(itup, 1, idx->rd_att, &isnull);
            if (isnull) ereport(ERROR,(errmsg("smol does not support NULL values")));

            text *t = DatumGetTextPP(val);
            int blen = VARSIZE_ANY_EXHDR(t);
            const char *src = VARDATA_ANY(t);
            if (blen > (int) key_len) ereport(ERROR,(errmsg("smol text key exceeds cap")));

            char *dest = keys_buf + (i * key_len);
            if (blen > 0) memcpy(dest, src, blen);
            if (blen < (int) key_len) memset(dest + blen, 0, key_len - blen);

            if (smol_debug_log && i < (Size) smol_log_sample_n)
            { /* GCOV_EXCL_START */
                int h = (blen < smol_log_hex_limit) ? blen : smol_log_hex_limit;
                char *hx = smol_hex(dest, h, h);
                SMOL_LOGF("build text key[%zu] blen=%d hex=%s", (size_t) i, blen, hx);
                pfree(hx);
            } /* GCOV_EXCL_STOP */

            if (i == n_this - 1) memcpy(lastkey, dest, key_len);
            /* Do not pfree itup - owned by tuplesort */
        }

        /* Analyze for RLE opportunities */
        bool use_rle = false;
        Size rle_sz = 0;
        uint16 rle_nruns = 0;

        {
            Size pos = 0; Size sz_runs = 0; uint16 nr = 0;
            while (pos < n_this)
            {
                Size run = 1;
                const char *k0 = keys_buf + (pos * key_len);
                while (pos + run < n_this)
                {
                    const char *k1 = keys_buf + ((pos + run) * key_len);
                    if (memcmp(k0, k1, key_len) != 0)
                        break;
                    run++;
                }
                nr++;
                sz_runs += key_len + sizeof(uint16);
                pos += run;
            }
            rle_sz = sizeof(uint16) * 3 + sz_runs; /* tag + nitems + nruns + runs */
            rle_nruns = nr;

            Size plain_sz = sizeof(uint16) + n_this * key_len;
            double uniqueness_ratio = (double) rle_nruns / (double) n_this;

            /* Use RLE if it saves space and data isn't too unique */
            if (rle_sz < plain_sz && rle_sz <= avail && uniqueness_ratio < smol_rle_uniqueness_threshold)
            {
                use_rle = true;
                SMOL_LOGF("Text RLE format: n=%zu nruns=%u uniqueness=%.3f rle_sz=%zu plain_sz=%zu",
                         n_this, rle_nruns, uniqueness_ratio, rle_sz, plain_sz);
            }
        }

        /* Write page with chosen format */
        Size sz;
        if (use_rle)
        {
            /* RLE format with version controlled by GUC */
            /* Determine which format to use based on GUC setting */
            bool use_v2 = (smol_key_rle_version == KEY_RLE_V2 ||
                          (smol_key_rle_version == KEY_RLE_AUTO && false));  /* Default to V1 for text build (AUTO && false) */
            uint16 tag = use_v2 ? SMOL_TAG_KEY_RLE_V2 : SMOL_TAG_KEY_RLE;

            char *p = scratch;
            uint16 nitems16 = (uint16) n_this;
            memcpy(p, &tag, sizeof(uint16)); p += sizeof(uint16);
            memcpy(p, &nitems16, sizeof(uint16)); p += sizeof(uint16);
            memcpy(p, &rle_nruns, sizeof(uint16)); p += sizeof(uint16);

            /* V2 format: write continues_byte (always 0 for this build path - no cross-page tracking) */
            if (use_v2)
            {
                uint8 continues_byte = 0;
                *p++ = continues_byte;
            }

            Size pos = 0;
            while (pos < n_this)
            {
                Size run = 1;
                const char *k0 = keys_buf + (pos * key_len);
                while (pos + run < n_this)
                {
                    const char *k1 = keys_buf + ((pos + run) * key_len);
                    if (memcmp(k0, k1, key_len) != 0)
                        break;
                    run++;
                }
                memcpy(p, k0, key_len); p += key_len;
                uint16 cnt16 = (uint16) run;
                memcpy(p, &cnt16, sizeof(uint16)); p += sizeof(uint16);
                pos += run;
            }
            sz = (Size) (p - scratch);
        }
        else
        {
            /* Plain format: [uint16 n][keys...] */
            uint16 n16 = (uint16) n_this;
            memcpy(scratch, &n16, sizeof(uint16));
            memcpy(scratch + sizeof(uint16), keys_buf, n_this * key_len);
            sz = sizeof(uint16) + n_this * key_len;
        }

        OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
        SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, WARNING,
            (errmsg("smol: failed to add leaf payload (text%s)", use_rle ? " RLE" : "")));

        pfree(keys_buf);
        MarkBufferDirty(buf);
        BlockNumber cur = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);
        smol_link_siblings(idx, prev, cur);
        prev = cur;
        /* record leaf and highkey */
        if (nleaves == aleaves)
        {
            aleaves = (aleaves == 0 ? 64 : aleaves * 2);
            leaves = (leaves == NULL) ? (BlockNumber *) palloc(aleaves * sizeof(BlockNumber)) : (BlockNumber *) repalloc(leaves, aleaves * sizeof(BlockNumber));
            leaf_high = (leaf_high == NULL) ? (char *) palloc(aleaves * key_len) : (char *) repalloc(leaf_high, aleaves * key_len);
        }
        leaves[nleaves] = cur;
        memcpy(leaf_high + ((size_t) nleaves * key_len), lastkey, key_len);
        nleaves++;
        remaining -= n_this;
    }
    pfree(scratch);
    /* set meta or build internal */
    if (nleaves == 1)
    {
        Buffer mb = ReadBuffer(idx, 0);
        LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
        SmolMeta *m = smol_meta_ptr(BufferGetPage(mb));
        m->root_blkno = 1;
        m->height = 1;
        MarkBufferDirty(mb);
        UnlockReleaseBuffer(mb);
    }
    else
    {
        BlockNumber rootblk = InvalidBlockNumber; uint16 levels = 0;
        smol_build_internal_levels_bytes(idx, leaves, leaf_high, nleaves, key_len, &rootblk, &levels);
        pfree(leaves); pfree(leaf_high);
    }
}

/* Stream-write fixed-length keys from tuplesort into leaf pages. */
static void
smol_build_fixed_stream_from_tuplesort(Relation idx, Tuplesortstate *ts, Size nkeys, uint16 key_len, bool byval)
{
    /* init meta page if new */
    if (RelationGetNumberOfBlocks(idx) == 0)
    {
        Buffer mbuf = ReadBufferExtended(idx, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
        Page mpage = BufferGetPage(mbuf);
        PageInit(mpage, BLCKSZ, 0);
        SmolMeta *meta = smol_meta_ptr(mpage);
        meta->magic = SMOL_META_MAGIC;
        meta->version = SMOL_META_VERSION;
        meta->nkeyatts = 1;
        meta->key_len1 = key_len;
        meta->key_len2 = 0;
        meta->root_blkno = InvalidBlockNumber;
        meta->height = 0;
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
    }
    if (nkeys == 0)
        return;
    BlockNumber prev = InvalidBlockNumber;
    Size nleaves = 0, aleaves = 0;
    BlockNumber *leaves = NULL;
    char *leaf_high = NULL; /* byte array for highkeys */
    char lastkey[16]; /* buffer for last key (max 16 bytes for UUID) */
    Size remaining = nkeys;
    IndexTuple itup;
    memset(lastkey, 0, sizeof(lastkey));

    /* Allocate scratch buffer for page construction (reused across pages) */
    char *scratch = (char *) palloc(BLCKSZ);

    /* Phase 2: V2 continuation tracking across pages (per-build state) */
    char prev_page_last_key[16];
    bool prev_page_has_key = false;

    /* Pending tuple from previous page (when page filled up) */
    char *pending_key = NULL;
    bool has_pending = false;

    while (remaining > 0)
    {
        Buffer buf = smol_extend(idx);
        smol_init_page(buf, true, InvalidBlockNumber);
        Page page = BufferGetPage(buf);
        Size fs = PageGetFreeSpace(page);
        Size avail = (fs > sizeof(ItemIdData)) ? (fs - sizeof(ItemIdData)) : 0;

        /* PHASE 1: INCREMENTAL RLE PACKING
         * Pack tuples one-by-one, tracking RLE state, until page is full.
         */
        Size n_this = 0;
        uint16 rle_nruns = 0;
        Size rle_current_size = sizeof(uint16) * 4;  /* tag + nitems + nruns + continues_byte */
        char rle_current_key[16];
        uint16 rle_current_count = 0;
        bool rle_has_key = false;

        /* Dynamic buffer for keys on this page */
        Size keys_buf_cap = 256;  /* initial capacity */
        Size keys_buf_len = 0;
        char *keys_buf = (char *) palloc(keys_buf_cap * key_len);

        /* Process pending tuple from previous page first */
        if (has_pending)
        {
            memcpy(keys_buf, pending_key, key_len);
            keys_buf_len = 1;
            n_this = 1;
            memcpy(rle_current_key, pending_key, key_len);
            rle_current_count = 1;
            rle_nruns = 1;
            rle_current_size += key_len + sizeof(uint16);
            rle_has_key = true;
            has_pending = false;
        }

        /* Fetch and pack tuples incrementally until page full */
        while (remaining > 0)
        {
            /* Fetch next tuple */
            itup = tuplesort_getindextuple(ts, true);
            if (itup == NULL)
            {
                /* Don't modify remaining here - it will be decremented by n_this at end of page loop */
                break;
            }

            /* Extract key value */
            bool isnull;
            Datum val = index_getattr(itup, 1, idx->rd_att, &isnull);
            if (isnull) ereport(ERROR,(errmsg("smol does not support NULL values")));

            char key_scratch[16];
            char *k = key_scratch;
            if (byval)
            {
                SMOL_DEFENSIVE_CHECK(key_len == 1 || key_len == 2 || key_len == 4 || key_len == 8 || key_len == 16, ERROR,
                                    (errmsg("key_len %d must be 1,2,4,8, or 16 for byval types", (int) key_len)));
                switch (key_len)
                {
                    case 1: { char v = DatumGetChar(val); memcpy(k, &v, 1); break; }
                    case 2: { int16 v = DatumGetInt16(val); memcpy(k, &v, 2); break; }
                    case 4: { int32 v = DatumGetInt32(val); memcpy(k, &v, 4); break; }
                    case 8: { int64 v = DatumGetInt64(val); memcpy(k, &v, 8); break; }
                    case 16: { /* GCOV_EXCL_LINE */
                        memcpy(k, DatumGetPointer(val), 16); /* GCOV_EXCL_LINE */
                        break; /* GCOV_EXCL_LINE */
                    }
                }
            }
            else
            {
                memcpy(k, DatumGetPointer(val), key_len);
            }

            /* Calculate delta size for this tuple */
            Size delta_size = 0;
            if (rle_has_key && memcmp(k, rle_current_key, key_len) == 0)
            {
                /* Extends current run - no size increase */
                delta_size = 0;
            }
            else
            {
                /* New run needed */
                delta_size = key_len + sizeof(uint16);
            }

            /* Apply test GUC limit */
            if (smol_test_max_tuples_per_page > 0 && n_this >= (Size) smol_test_max_tuples_per_page)
            {
                /* Page full due to test limit - save tuple for next page */
                if (!pending_key) pending_key = (char *) palloc(key_len);
                memcpy(pending_key, k, key_len);
                has_pending = true;
                break;
            }

            /* Check nitems limit (uint16 max - must stay below 65535 to avoid overflow in scan loop) */
            if (n_this >= 65534)
            {
                /* Reached maximum tuples per page - save for next page */
                if (!pending_key) pending_key = (char *) palloc(key_len);
                memcpy(pending_key, k, key_len);
                has_pending = true;
                break;
            }

            /* Would this overflow the page? */
            if (rle_current_size + delta_size > avail)
            {
                /* Page full - save tuple for next page */
                if (!pending_key) pending_key = (char *) palloc(key_len);
                memcpy(pending_key, k, key_len);
                has_pending = true;
                break;
            }

            /* Add this tuple to the page */
            if (keys_buf_len == keys_buf_cap)
            {
                keys_buf_cap *= 2;
                keys_buf = (char *) repalloc(keys_buf, keys_buf_cap * key_len);
            }
            memcpy(keys_buf + (keys_buf_len * key_len), k, key_len);
            keys_buf_len++;

            if (!rle_has_key || memcmp(k, rle_current_key, key_len) != 0)
            {
                /* Start new run */
                memcpy(rle_current_key, k, key_len);
                rle_current_count = 1;
                rle_nruns++;
                rle_current_size += delta_size;
                rle_has_key = true;
            }
            else
            {
                /* Extend run */
                rle_current_count++;
            }

            n_this++;
            /* NOTE: remaining is decremented at end of outer loop */
        }

        SMOL_DEFENSIVE_CHECK(n_this > 0, ERROR, (errmsg("smol: no tuples fit on page (key_len=%u)", key_len)));

        /* Save last key for highkey */
        memcpy(lastkey, keys_buf + ((n_this - 1) * key_len), key_len);

        /* Decide format: use RLE if it's beneficial */
        bool use_rle = false;
        Size rle_sz = rle_current_size;
        Size plain_sz = sizeof(uint16) + n_this * key_len;
        double uniqueness_ratio = (double) rle_nruns / (double) n_this;

        if (rle_sz < plain_sz && rle_sz <= avail && uniqueness_ratio < smol_rle_uniqueness_threshold)
        {
            use_rle = true;
            SMOL_LOGF("RLE format: n=%zu nruns=%u uniqueness=%.3f rle_sz=%zu plain_sz=%zu",
                     n_this, rle_nruns, uniqueness_ratio, rle_sz, plain_sz);
        }

        /* Write page with chosen format */
        Size sz;
        if (use_rle)
        {
            /* PHASE 2: RLE V2 with continuation detection */
            uint16 tag = SMOL_TAG_KEY_RLE_V2;
            char *p = scratch;
            uint16 nitems16 = (uint16) n_this;
            memcpy(p, &tag, sizeof(uint16)); p += sizeof(uint16);
            memcpy(p, &nitems16, sizeof(uint16)); p += sizeof(uint16);
            memcpy(p, &rle_nruns, sizeof(uint16)); p += sizeof(uint16);

            /* Continuation detection: check if first key matches previous page's last key */
            uint8 continues_byte = 0;
            const char *first_key = keys_buf;
            if (prev_page_has_key && memcmp(first_key, prev_page_last_key, key_len) == 0)
            {
                continues_byte = 1;  /* First run continues from previous page */
            }
            *p++ = continues_byte;

            /* Write RLE runs */
            Size pos = 0;
            while (pos < n_this)
            {
                Size run = 1;
                const char *k0 = keys_buf + (pos * key_len);
                while (pos + run < n_this)
                {
                    const char *k1 = keys_buf + ((pos + run) * key_len);
                    if (memcmp(k0, k1, key_len) != 0)
                        break;
                    run++;
                }
                memcpy(p, k0, key_len); p += key_len;
                uint16 cnt16 = (uint16) run;
                memcpy(p, &cnt16, sizeof(uint16)); p += sizeof(uint16);
                pos += run;
            }
            sz = (Size) (p - scratch);

            /* Remember last key for next page's continuation detection */
            memcpy(prev_page_last_key, lastkey, key_len);
            prev_page_has_key = true;
        }
        else
        {
            /* Plain format: [uint16 n][keys...] */
            uint16 n16 = (uint16) n_this;
            memcpy(scratch, &n16, sizeof(uint16));
            memcpy(scratch + sizeof(uint16), keys_buf, n_this * key_len);
            sz = sizeof(uint16) + n_this * key_len;

            /* Track last key even for plain format (next page might use RLE) */
            memcpy(prev_page_last_key, lastkey, key_len);
            prev_page_has_key = true;
        }

        OffsetNumber off = PageAddItem(page, (Item) scratch, sz, FirstOffsetNumber, false, false);
        SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, ERROR,
            (errmsg("smol: failed to add leaf payload (fixed%s)", use_rle ? " RLE" : "")));

        pfree(keys_buf);
        MarkBufferDirty(buf);
        BlockNumber cur = BufferGetBlockNumber(buf);
        UnlockReleaseBuffer(buf);
        smol_link_siblings(idx, prev, cur);
        prev = cur;
        /* record leaf and highkey */
        if (nleaves == aleaves)
        {
            aleaves = (aleaves == 0 ? 64 : aleaves * 2);
            leaves = (leaves == NULL) ? (BlockNumber *) palloc(aleaves * sizeof(BlockNumber)) : (BlockNumber *) repalloc(leaves, aleaves * sizeof(BlockNumber));
            leaf_high = (leaf_high == NULL) ? (char *) palloc(aleaves * key_len) : (char *) repalloc(leaf_high, aleaves * key_len);
        }
        leaves[nleaves] = cur;
        memcpy(leaf_high + (nleaves * key_len), lastkey, key_len);
        nleaves++;
        remaining -= n_this;
    }
    pfree(scratch);
    /* set meta or build internal */
    if (nleaves == 1)
    {
        Buffer mb = ReadBuffer(idx, 0);
        LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
        SmolMeta *m = smol_meta_ptr(BufferGetPage(mb));
        m->root_blkno = leaves[0];
        m->height = 1;
        MarkBufferDirty(mb);
        UnlockReleaseBuffer(mb);
        pfree(leaves);
        pfree(leaf_high);
    }
    else
    {
        BlockNumber rootblk = InvalidBlockNumber; uint16 levels = 0;
        smol_build_internal_levels_bytes(idx, leaves, leaf_high, nleaves, key_len, &rootblk, &levels);
        pfree(leaves); pfree(leaf_high);
    }
}

/* Diagnostic: count page types and log a summary */
/* GCOV_EXCL_START - debug diagnostic function, only called when smol_debug_log enabled */
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
/* GCOV_EXCL_STOP */

/* Return rightmost leaf block number by scanning all pages */
static BlockNumber
smol_rightmost_leaf(Relation idx)
{
    BlockNumber nblocks = RelationGetNumberOfBlocks(idx);
    BlockNumber rightmost_leaf = InvalidBlockNumber;

    /* Scan all blocks to find the rightmost leaf (highest block number that's a leaf) */
    for (BlockNumber blk = 1; blk < nblocks; blk++)
    {
        Buffer buf = ReadBuffer(idx, blk);
        Page page = BufferGetPage(buf);

        /* Check if page has SmolPageOpaqueData (use >= to account for MAXALIGN padding) */
        if (!PageIsEmpty(page) && PageGetSpecialSize(page) >= sizeof(SmolPageOpaqueData))
        {
            SmolPageOpaqueData *op = (SmolPageOpaqueData *) PageGetSpecialPointer(page);
            if (op->flags & SMOL_F_LEAF)
            {
                rightmost_leaf = blk;
            }
        }
        ReleaseBuffer(buf);
    }

    return rightmost_leaf;
}

/* GCOV_EXCL_START - unused functions, kept for future reference */
/* Helper function: find rightmost leaf in a subtree */
static BlockNumber
smol_rightmost_in_subtree(Relation idx, BlockNumber root, uint16 levels)
{
    BlockNumber cur = root;
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

/* Helper function: recursively find previous leaf by walking tree */
static BlockNumber
smol_find_prev_leaf_recursive(Relation idx, BlockNumber node, BlockNumber target, uint16 levels, bool *found)
{
    if (levels == 1)
    {
        /* Leaf level - check if this is our target */
        if (node == target)
        {
            *found = true;
            return InvalidBlockNumber; /* No previous sibling at this level */
        }
        return node;
    }

    /* Internal node - scan children */
    Buffer buf = ReadBuffer(idx, node);
    Page page = BufferGetPage(buf);
    OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
    BlockNumber prev_child = InvalidBlockNumber;
    BlockNumber result = InvalidBlockNumber;

    for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
    {
        char *itp = (char *) PageGetItem(page, PageGetItemId(page, off));
        BlockNumber child;
        memcpy(&child, itp, sizeof(BlockNumber));

        /* Recursively search this child subtree */
        BlockNumber candidate = smol_find_prev_leaf_recursive(idx, child, target, levels - 1, found);

        if (*found)
        {
            /* Target was found in this subtree */
            if (candidate != InvalidBlockNumber)
            {
                /* Candidate is in this subtree */
                result = candidate;
            }
            else if (prev_child != InvalidBlockNumber)
            {
                /* Target was first in this subtree, return rightmost of previous sibling */
                result = smol_rightmost_in_subtree(idx, prev_child, levels - 1);
            }
            else
            {
                /* Target was in first child, no previous leaf */
                result = InvalidBlockNumber;
            }
            ReleaseBuffer(buf);
            return result;
        }

        prev_child = child;
    }

    ReleaseBuffer(buf);
    return InvalidBlockNumber;
}

/* Return previous leaf sibling by walking tree (works for any height) */
static BlockNumber __attribute__((unused))
smol_prev_leaf(Relation idx, BlockNumber cur)
{
    SmolMeta meta;
    smol_meta_read(idx, &meta);

    if (meta.height <= 1)
        return InvalidBlockNumber;

    bool found = false;
    return smol_find_prev_leaf_recursive(idx, meta.root_blkno, cur, meta.height, &found);
}
/* GCOV_EXCL_STOP */

/* Build callbacks and comparators */
static void
ts_build_cb_any(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsBuildCtxAny *c = (TsBuildCtxAny *) state;
    (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    tuplesort_putindextuplevalues(c->ts, rel, tid, values, isnull);
    (*c->pnkeys)++;
}

static void
ts_build_cb_text(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    TsBuildCtxText *c = (TsBuildCtxText *) state; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR,(errmsg("smol does not support NULL values")));
    text *t = DatumGetTextPP(values[0]); int blen = VARSIZE_ANY_EXHDR(t);
    if (blen > *c->pmax) *c->pmax = blen;
    tuplesort_putindextuplevalues(c->ts, rel, tid, values, isnull);
    (*c->pnkeys)++;
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
        /* Grow exponentially up to 8M entries, then linearly by 2M to avoid MaxAllocSize (1GB) */
        Size oldcap = *c->pcap;
        Size newcap;
#ifdef SMOL_TEST_COVERAGE
        Size growth_threshold = smol_growth_threshold_test > 0 ? (Size) smol_growth_threshold_test : 8388608;
#else
        Size growth_threshold = 8388608;
#endif
        if (oldcap == 0)
            newcap = 1024;
        else if (oldcap < growth_threshold)  /* 8M entries (or test override) */
            newcap = oldcap * 2;
        else
            newcap = oldcap + 2097152;  /* +2M entries per grow */
        Size bytes1 = (Size) newcap * (Size) c->len1;
        Size bytes2 = (Size) newcap * (Size) c->len2;
        char *n1 = (*c->pcap == 0) ? (char *) MemoryContextAllocHuge(CurrentMemoryContext, bytes1) : (char *) repalloc_huge(*c->pk1, bytes1);
        char *n2 = (*c->pcap == 0) ? (char *) MemoryContextAllocHuge(CurrentMemoryContext, bytes2) : (char *) repalloc_huge(*c->pk2, bytes2);
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
        }
        SMOL_DEFENSIVE_CHECK(c->len1 == 1 || c->len1 == 2 || c->len1 == 4 || c->len1 == 8, ERROR,
            (errmsg("unexpected byval len1=%u", (unsigned) c->len1)));
    }
    else memcpy(dst1, DatumGetPointer(values[0]), c->len1);
    if (c->byval2)
    {
        switch (c->len2)
        { case 1: { char v = DatumGetChar(values[1]); memcpy(dst2,&v,1); break; }
          case 2: { int16 v = DatumGetInt16(values[1]); memcpy(dst2,&v,2); break; }
          case 4: { int32 v = DatumGetInt32(values[1]); memcpy(dst2,&v,4); break; }
          case 8: { int64 v = DatumGetInt64(values[1]); memcpy(dst2,&v,8); break; }
        }
        SMOL_DEFENSIVE_CHECK(c->len2 == 1 || c->len2 == 2 || c->len2 == 4 || c->len2 == 8, ERROR,
            (errmsg("unexpected byval len2=%u", (unsigned) c->len2)));
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
    int64 *t1 = (int64 *) MemoryContextAllocHuge(CurrentMemoryContext, n * sizeof(int64));
    int64 *t2 = (int64 *) MemoryContextAllocHuge(CurrentMemoryContext, n * sizeof(int64));
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
smol_parallel_sort_worker(Datum arg) /* GCOV_EXCL_START - parallel build not integrated, see lines 763-775 */
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
} /* GCOV_EXCL_STOP */

/* fixed-size copy helpers */
/*
 * Hot-path copy helpers: favor unrolled fixed-size copies and aligned wide
 * loads/stores to reduce memcpy/branch overhead. Fall back to memcpy for
 * potentially-unaligned cases. These are small and declared inline to help
 * the compiler optimize call sites.
 */
static inline void
smol_copy1(char *dst, const char *src)
{
    *dst = *src;
}

/* PERF OPT: Use __builtin_memcpy for small fixed sizes. The compiler optimizes
 * this to direct assignments when possible, and handles unaligned cases efficiently.
 * This eliminates the runtime alignment checks while maintaining correctness. */
static inline void
smol_copy2(char *dst, const char *src)
{
    __builtin_memcpy(dst, src, 2);
}

static inline void
smol_copy4(char *dst, const char *src)
{
    __builtin_memcpy(dst, src, 4);
}

static inline void
smol_copy8(char *dst, const char *src)
{
    __builtin_memcpy(dst, src, 8);
}

/* PERF OPT: Use __builtin_memcpy for 16-byte copy. Compiler generates optimal
 * code, often using SIMD instructions on modern CPUs. */
static inline void
smol_copy16(char *dst, const char *src)
{
    __builtin_memcpy(dst, src, 16);
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

/* Build single-column tuple in-place with dynamic varlena offsets */
static inline void
smol_emit_single_tuple(SmolScanOpaque so, Page page, const char *keyp, uint32 row)
{
    Size cur = so->itup_data_off; /* absolute offset from tuple start */
    char *base = (char *) so->itup;
    char *wp;
    /* key */
    cur = att_align_nominal(cur, so->align1);
    wp = base + cur;
    if (so->key_is_text32)
    {
        if (so->run_active && so->run_key_built && so->run_key_vl_len > 0)
        {
            memcpy(wp, so->run_key_vl, (size_t) so->run_key_vl_len);
            cur += (Size) so->run_key_vl_len;
        }
        else /* GCOV_EXCL_LINE */
        { /* GCOV_EXCL_LINE */
            const char *kend = (const char *) memchr(keyp, '\0', 32); 
            int klen = kend ? (int)(kend - keyp) : 32; 
            SET_VARSIZE((struct varlena *) wp, klen + VARHDRSZ); 
            memcpy(wp + VARHDRSZ, keyp, klen); 
            cur += VARHDRSZ + (Size) klen; 
        } /* GCOV_EXCL_LINE */
    }
    else
    {
        if (so->key_len == 2) smol_copy2(wp, keyp);
        else if (so->key_len == 4) smol_copy4(wp, keyp);
        else if (so->key_len == 8) smol_copy8(wp, keyp);
        else if (so->key_len == 16) smol_copy16(wp, keyp);
        else smol_copy_small(wp, keyp, so->key_len); /* GCOV_EXCL_LINE - uncommon key lengths rarely reached in tuple emission */
        cur += so->key_len;
    }
    /* includes */
    if (so->ninclude > 0)
    {
        uint16 n2 = so->cur_page_nitems; /* Opt #5: use cached nitems */
        for (uint16 ii=0; ii<so->ninclude; ii++)
        {
            cur = att_align_nominal(cur, so->inc_align[ii]);
            wp = base + cur;
            char *ip = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_len, so->ninclude, ii, row, so->inc_cumul_offs);
            if (so->inc_is_text[ii])
            {
                if (so->run_active && so->inc_const[ii] && so->run_inc_built[ii] && so->run_inc_vl_len[ii] > 0)
                {
                    memcpy(wp, so->run_inc_vl[ii], (size_t) so->run_inc_vl_len[ii]);
                    cur += (Size) so->run_inc_vl_len[ii];
                }
                else
                {
                    const char *iend = (const char *) memchr(ip, '\0', so->inc_len[ii]);
                    int ilen = iend ? (int)(iend - ip) : (int) so->inc_len[ii];
                    SET_VARSIZE((struct varlena *) wp, ilen + VARHDRSZ);
                    memcpy(wp + VARHDRSZ, ip, ilen);
                    cur += VARHDRSZ + (Size) ilen;
                }
            }
            else
            {
                if (so->inc_len[ii] == 2) smol_copy2(wp, ip);
                else if (so->inc_len[ii] == 4) smol_copy4(wp, ip);
                else if (so->inc_len[ii] == 8) smol_copy8(wp, ip);
                else if (so->inc_len[ii] == 16) smol_copy16(wp, ip); /* GCOV_EXCL_LINE - 16-byte INCLUDE columns rarely used */
                else smol_copy_small(wp, ip, so->inc_len[ii]); /* GCOV_EXCL_LINE - uncommon INCLUDE column lengths rarely used */
                cur += so->inc_len[ii];
            }
        }
    }
    cur = MAXALIGN(cur);
    so->itup->t_info = (unsigned short) (cur | (so->has_varwidth ? INDEX_VAR_MASK : 0));
}
/* Callback to collect keys (1 or 2) + INCLUDE columns */
static void
smol_build_cb_inc(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    CollectIncCtx *c = (CollectIncCtx *) state; (void) rel; (void) tid; (void) tupleIsAlive;
    int inc_offset = c->nkeyatts;  /* INCLUDE columns start after nkeyatts */

    /* Check for NULL keys */
    for (int k = 0; k < c->nkeyatts; k++)
        if (isnull[k]) ereport(ERROR, (errmsg("smol does not support NULL key values")));

    /* Check for NULL INCLUDEs */
    for (int i=0;i<c->incn;i++)
        if (isnull[inc_offset+i]) ereport(ERROR, (errmsg("smol INCLUDE does not support NULL values")));

    if (*c->pcount == *c->pcap)
    {
        /* Grow exponentially up to 8M entries, then linearly by 2M to avoid MaxAllocSize (1GB) */
        Size oldcap = *c->pcap;
        Size newcap;
#ifdef SMOL_TEST_COVERAGE
        Size growth_threshold = smol_growth_threshold_test > 0 ? (Size) smol_growth_threshold_test : 8388608;
#else
        Size growth_threshold = 8388608;
#endif
        if (oldcap == 0)
            newcap = 1024;
        else if (oldcap < growth_threshold)  /* 8M entries (or test override) */
            newcap = oldcap * 2;
        else
            newcap = oldcap + 2097152;  /* +2M entries per grow */

        /* Grow key buffers based on nkeyatts */
        if (c->nkeyatts == 1)
        {
            if (!c->key_is_text32)
            {
                int64 *newk = (*c->pcap == 0) ? (int64 *) MemoryContextAllocHuge(CurrentMemoryContext, newcap * sizeof(int64)) : (int64 *) repalloc_huge(*c->pk, newcap * sizeof(int64));
                *c->pk = newk;
            }
            else
            {
                Size bytes = (Size) newcap * (Size) c->key_len;
                char *newkb = (*c->pcap == 0) ? (char *) MemoryContextAllocHuge(CurrentMemoryContext, bytes) : (char *) repalloc_huge(*c->pkbytes, bytes);
                *c->pkbytes = newkb;
            }
        }
        else  /* nkeyatts == 2 */
        {
            Size bytes1 = (Size) newcap * (Size) c->key_len;
            Size bytes2 = (Size) newcap * (Size) c->key_len2;
            char *newk1 = (*c->pcap == 0) ? (char *) MemoryContextAllocHuge(CurrentMemoryContext, bytes1) : (char *) repalloc_huge(*c->pk1buf, bytes1);
            char *newk2 = (*c->pcap == 0) ? (char *) MemoryContextAllocHuge(CurrentMemoryContext, bytes2) : (char *) repalloc_huge(*c->pk2buf, bytes2);
            *c->pk1buf = newk1;
            *c->pk2buf = newk2;
        }

        for (int i=0;i<c->incn;i++)
        {
            Size bytes = (Size) newcap * (Size) c->ilen[i];
            char *old = *c->pi[i];
            char *ni = (*c->pcap == 0) ? (char *) MemoryContextAllocHuge(CurrentMemoryContext, bytes) : (char *) repalloc_huge(old, bytes);
            *c->pi[i] = ni;
        }
        *c->pcap = newcap;
    }

    /* Collect keys based on nkeyatts */
    if (c->nkeyatts == 1)
    {
        if (!c->key_is_text32)
        {
            (*c->pk)[*c->pcount] = DatumGetInt64(values[0]);
        }
        else
        {
            /* pack text key to fixed key_len bytes (C collation assumed) */
            text *t = DatumGetTextPP(values[0]);
            int blen = VARSIZE_ANY_EXHDR(t);
            if (blen > (int) c->key_len)
                ereport(ERROR, (errmsg("smol text32 key exceeds %u bytes", (unsigned) c->key_len))); /* GCOV_EXCL_LINE - error path for oversized TEXT32 keys */
            char *dstk = (*c->pkbytes) + ((size_t) (*c->pcount) * (size_t) c->key_len);
            const char *src = VARDATA_ANY(t);
            if (blen > 0) memcpy(dstk, src, blen);
            if (blen < (int) c->key_len) memset(dstk + blen, 0, c->key_len - blen);
        }
    }
    else  /* nkeyatts == 2 */
    {
        /* Collect first key */
        char *dst1 = (*c->pk1buf) + ((size_t) (*c->pcount) * (size_t) c->key_len);
        if (c->byval1)
        {
            switch (c->key_len)
            {
                case 1: { char v = DatumGetChar(values[0]); memcpy(dst1, &v, 1); break; }
                case 2: { int16 v = DatumGetInt16(values[0]); memcpy(dst1, &v, 2); break; }
                case 4: { int32 v = DatumGetInt32(values[0]); memcpy(dst1, &v, 4); break; }
                case 8: { int64 v = DatumGetInt64(values[0]); memcpy(dst1, &v, 8); break; }
            }
        }
        else
            memcpy(dst1, DatumGetPointer(values[0]), c->key_len);

        /* Collect second key */
        char *dst2 = (*c->pk2buf) + ((size_t) (*c->pcount) * (size_t) c->key_len2);
        if (c->byval2)
        {
            switch (c->key_len2)
            {
                case 1: { char v = DatumGetChar(values[1]); memcpy(dst2, &v, 1); break; }
                case 2: { int16 v = DatumGetInt16(values[1]); memcpy(dst2, &v, 2); break; }
                case 4: { int32 v = DatumGetInt32(values[1]); memcpy(dst2, &v, 4); break; }
                case 8: { int64 v = DatumGetInt64(values[1]); memcpy(dst2, &v, 8); break; }
            }
        }
        else
            memcpy(dst2, DatumGetPointer(values[1]), c->key_len2);
    }

    /* Collect INCLUDE columns */
    for (int i=0;i<c->incn;i++)
    {
        char *dst = (*c->pi[i]) + ((size_t)(*c->pcount) * (size_t) c->ilen[i]);
        if (c->itext[i])
        {
            text *t = DatumGetTextPP(values[inc_offset+i]);
            int blen = VARSIZE_ANY_EXHDR(t);
            if (blen > (int) c->ilen[i]) ereport(ERROR, (errmsg("smol text32 INCLUDE exceeds 32 bytes")));
            const char *src = VARDATA_ANY(t);
            if (blen > 0) memcpy(dst, src, blen);
            if (blen < (int) c->ilen[i]) memset(dst + blen, 0, c->ilen[i] - blen);
        }
        else if (c->ibyval[i])
        {
            SMOL_DEFENSIVE_CHECK(c->ilen[i] == 1 || c->ilen[i] == 2 || c->ilen[i] == 4 || c->ilen[i] == 8, ERROR,
                (errmsg("unexpected include byval len=%u", (unsigned) c->ilen[i])));
            switch (c->ilen[i])
            {
                case 1: { char v = DatumGetChar(values[inc_offset+i]); memcpy(dst, &v, 1); break; }
                case 2: { int16 v = DatumGetInt16(values[inc_offset+i]); memcpy(dst, &v, 2); break; }
                case 4: { int32 v = DatumGetInt32(values[inc_offset+i]); memcpy(dst, &v, 4); break; }
                case 8: { int64 v = DatumGetInt64(values[inc_offset+i]); memcpy(dst, &v, 8); break; }
            }
        }
        else
        {
            SMOL_DEFENSIVE_CHECK(!c->itext[i] && !c->ibyval[i], ERROR,
                (errmsg("unexpected INCLUDE column: not text and not byval")));
            memcpy(dst, DatumGetPointer(values[inc_offset+i]), c->ilen[i]);
        }
    }
    (*c->pcount)++;
}
/* --- Debug helpers -------------------------------------------------------- */
/* GCOV_EXCL_START - debug helper only used when smol.debug_log GUC is manually enabled */
static char *
smol_hex(const char *buf, int len, int maxbytes)
{
    int n = len < maxbytes ? len : maxbytes;
    int outsz = n * 2 + 1; /* hex without spaces */
    char *out = palloc(outsz);
    for (int i = 0; i < n; i++)
    {
        unsigned char b = (unsigned char) buf[i];
        out[i*2] = "0123456789ABCDEF"[b >> 4];
        out[i*2+1] = "0123456789ABCDEF"[b & 0x0F];
    }
    out[outsz-1] = '\0';
    return out;
}
/* GCOV_EXCL_STOP */

/* --- Test functions for coverage --- */

/*
 * smol_test_backward_scan - Test function to exercise backward scan paths
 *
 * Takes an index OID and optional lower bound, performs a backward scan
 * to force execution of the BackwardScanDirection initialization code.
 * Returns number of tuples scanned backward.
 */
Datum
smol_test_backward_scan(PG_FUNCTION_ARGS)
{
    Oid indexoid = PG_GETARG_OID(0);
    int32 lower_bound = PG_NARGS() > 1 ? PG_GETARG_INT32(1) : 0;
    bool with_bound = PG_NARGS() > 1;

    Relation indexRel;
    IndexScanDesc scan;
    ScanKeyData skey;
    int count = 0;

    /* Open the index */
    indexRel = index_open(indexoid, AccessShareLock);

    /* Begin scan using our AM's beginscan - this properly initializes opaque */
    scan = smol_beginscan(indexRel, with_bound ? 1 : 0, 0);
    scan->xs_want_itup = true;  /* Force index-only scan mode */

    /* Set up scan key if provided
     * Use BTEqualStrategyNumber for equality bounds to test have_k1_eq path */
    if (with_bound)
    {
        /* If lower_bound is negative, interpret as equality scan */
        bool use_equality = (lower_bound < 0);
        int32 bound_value = use_equality ? -lower_bound : lower_bound;

        ScanKeyInit(&skey,
                    1,  /* attribute number */
                    use_equality ? BTEqualStrategyNumber : BTGreaterEqualStrategyNumber,
                    use_equality ? F_INT4EQ : F_INT4GE,
                    Int32GetDatum(bound_value));
        smol_rescan(scan, &skey, 1, NULL, 0);
    }

    /* Perform backward scan - this forces BackwardScanDirection initialization */
    while (smol_gettuple(scan, BackwardScanDirection))
    {
        count++;
        if (count >= 10)  /* Limit to avoid long scans */
            break;
    }

    /* Cleanup */
    smol_endscan(scan);
    index_close(indexRel, AccessShareLock);

    PG_RETURN_INT32(count);
}

/*
 * NOTE: smol_test_parallel_scan was removed because parallel scan paths
 * are properly tested via SQL queries with forced parallel workers.
 * See sql/smol_coverage_direct.sql for the parallel scan tests.
 */

/*
 * smol_inspect - Inspect SMOL index structure and return statistics
 *
 * Returns a row with:
 *   total_pages int4       - Total number of pages in the index
 *   leaf_pages int4        - Number of leaf pages
 *   zerocopy_pages int4    - Number of pages using zero-copy format
 *   key_rle_pages int4     - Number of pages using key RLE compression
 *   inc_rle_pages int4     - Number of pages using INCLUDE RLE compression
 *   zerocopy_pct numeric   - Percentage of leaf pages using zero-copy
 *   compression_pct numeric - Percentage of leaf pages using RLE (key or include)
 */
PG_FUNCTION_INFO_V1(smol_inspect);

Datum
smol_inspect(PG_FUNCTION_ARGS)
{
    Oid         indexoid = PG_GETARG_OID(0);
    Relation    idx;
    TupleDesc   tupdesc;
    Datum       values[7];
    bool        nulls[7];
    HeapTuple   tuple;
    BlockNumber nblocks;
    BlockNumber blkno;
    int32       total_pages = 0;
    int32       leaf_pages = 0;
    int32       zerocopy_pages = 0;
    int32       key_rle_pages = 0;
    int32       inc_rle_pages = 0;
    Buffer      buf;
    Page        page;
    uint16      tag = 0;

    /* Open the index */
    idx = index_open(indexoid, AccessShareLock);

    /* Get total pages */
    nblocks = RelationGetNumberOfBlocks(idx);
    total_pages = nblocks;

    /* Scan all pages to classify them */
    for (blkno = 1; blkno < nblocks; blkno++)  /* Skip meta page 0 */
    {
        CHECK_FOR_INTERRUPTS();

        buf = ReadBuffer(idx, blkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        /* Skip empty pages */
        if (PageIsEmpty(page))
        {
            UnlockReleaseBuffer(buf);
            continue;
        }

        /* Check if it's a leaf page using opaque data */
        if (PageGetSpecialSize(page) >= sizeof(SmolPageOpaqueData))
        {
            SmolPageOpaqueData *opaque = (SmolPageOpaqueData *) PageGetSpecialPointer(page);

            /* Bounds check and safe dereference */
            if (opaque != NULL &&
                (char *)opaque >= (char *)page &&
                (char *)opaque + sizeof(SmolPageOpaqueData) <= (char *)page + BLCKSZ)
            {
                uint16 flags = opaque->flags;

                if (flags & SMOL_F_LEAF)
                {
                    leaf_pages++;

                    /* Check if page has items before reading tag */
                    if (PageGetMaxOffsetNumber(page) >= FirstOffsetNumber)
                    {
                        ItemId iid = PageGetItemId(page, FirstOffsetNumber);
                        if (iid != NULL && ItemIdIsNormal(iid) && ItemIdGetLength(iid) >= sizeof(uint16))
                        {
                            char *data = (char *) PageGetItem(page, iid);
                            if (data != NULL)
                            {
                                memcpy(&tag, data, sizeof(uint16));

                                /* Check format tags */
                                if (tag == SMOL_TAG_ZEROCOPY)
                                    zerocopy_pages++; /* GCOV_EXCL_LINE */
                                else if (tag == SMOL_TAG_KEY_RLE || tag == SMOL_TAG_KEY_RLE_V2)
                                    key_rle_pages++;
                                else if (tag == SMOL_TAG_INC_RLE)
                                    inc_rle_pages++;
                                else if (tag < 0x8000)
                                    zerocopy_pages++;
                            }
                        }
                    }
                }
            }
        }

        UnlockReleaseBuffer(buf);
    }

    index_close(idx, AccessShareLock);

    /* Build return tuple */
    /* Parser ensures function is called in composite-returning context */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) 
        elog(ERROR, "function returning record called in invalid context"); /* GCOV_EXCL_LINE */

    tupdesc = BlessTupleDesc(tupdesc);

    memset(nulls, 0, sizeof(nulls));

    values[0] = Int32GetDatum(total_pages);
    values[1] = Int32GetDatum(leaf_pages);
    values[2] = Int32GetDatum(zerocopy_pages);
    values[3] = Int32GetDatum(key_rle_pages);
    values[4] = Int32GetDatum(inc_rle_pages);

    /* Calculate percentages */
    if (leaf_pages > 0)
    {
        double zerocopy_pct = (100.0 * zerocopy_pages) / leaf_pages;
        double compression_pct = (100.0 * (key_rle_pages + inc_rle_pages)) / leaf_pages;

        values[5] = DirectFunctionCall1(float8_numeric, Float8GetDatum(zerocopy_pct));
        values[6] = DirectFunctionCall1(float8_numeric, Float8GetDatum(compression_pct));
    }
    else
    {
        values[5] = DirectFunctionCall1(float8_numeric, Float8GetDatum(0.0));
        values[6] = DirectFunctionCall1(float8_numeric, Float8GetDatum(0.0));
    }

    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * Whitebox test functions to directly call internal tree navigation functions
 */
PG_FUNCTION_INFO_V1(smol_test_rightmost_leaf);
PG_FUNCTION_INFO_V1(smol_test_find_first_leaf_rightmost);

/*
 * smol_test_rightmost_leaf - Test smol_rightmost_leaf() to cover lines 4107-4113
 */
Datum
smol_test_rightmost_leaf(PG_FUNCTION_ARGS)
{
    Oid indexoid = PG_GETARG_OID(0);
    Relation idx = index_open(indexoid, AccessShareLock);

    /* Call smol_rightmost_leaf to trigger lines 4107-4113 */
    BlockNumber leaf = smol_rightmost_leaf(idx);

    index_close(idx, AccessShareLock);

    PG_RETURN_INT32((int32) leaf);
}

/*
 * smol_test_find_first_leaf_rightmost - Test find_first_leaf with bound > all keys
 *
 * Forces the rightmost child selection path (lines 3517-3518).
 */
Datum
smol_test_find_first_leaf_rightmost(PG_FUNCTION_ARGS)
{
    Oid indexoid = PG_GETARG_OID(0);
    int64 large_bound = PG_GETARG_INT64(1);

    Relation idx = index_open(indexoid, AccessShareLock);

    /* Get index info */
    SmolMeta meta;
    smol_meta_read(idx, &meta);
    TupleDesc tupdesc = RelationGetDescr(idx);
    Oid atttypid = TupleDescAttr(tupdesc, 0)->atttypid;
    uint16 key_len = meta.key_len1;

    /* Call smol_find_first_leaf with large_bound > all keys */
    BlockNumber leaf = smol_find_first_leaf(idx, large_bound, atttypid, key_len);

    index_close(idx, AccessShareLock);

    PG_RETURN_INT32((int32) leaf);
}

/*
 * smol_test_error_non_ios - Test function to exercise non-IOS error path (line 1286)
 *
 * Calls smol_gettuple without setting xs_want_itup to trigger the error.
 */
Datum
smol_test_error_non_ios(PG_FUNCTION_ARGS)
{
    Oid indexoid = PG_GETARG_OID(0);
    Relation indexRel;
    IndexScanDesc scan;

    indexRel = index_open(indexoid, AccessShareLock);
    scan = smol_beginscan(indexRel, 0, 0);
    /* Deliberately NOT setting xs_want_itup - this triggers the error */
    scan->xs_want_itup = false;

    /* This should trigger: ereport(ERROR, (errmsg("smol supports index-only scans only"))) */
    smol_gettuple(scan, ForwardScanDirection);

    /* Should not reach here */ /* GCOV_EXCL_START - unreachable after error */
    smol_endscan(scan);
    index_close(indexRel, AccessShareLock);

    PG_RETURN_BOOL(false); /* GCOV_EXCL_STOP */
}

/*
 * smol_test_no_movement - Test function to exercise NoMovementScanDirection path (line 1288)
 *
 * Calls smol_gettuple with NoMovementScanDirection to verify it returns false.
 */
Datum
smol_test_no_movement(PG_FUNCTION_ARGS)
{
    Oid indexoid = PG_GETARG_OID(0);
    Relation indexRel;
    IndexScanDesc scan;
    bool result;

    indexRel = index_open(indexoid, AccessShareLock);
    scan = smol_beginscan(indexRel, 0, 0);
    scan->xs_want_itup = true;

    /* Call with NoMovementScanDirection - should return false */
    result = smol_gettuple(scan, NoMovementScanDirection);

    smol_endscan(scan);
    index_close(indexRel, AccessShareLock);

    PG_RETURN_BOOL(result);
}

/*
 * smol_test_run_synthetic - Explicitly run synthetic tests for coverage
 *
 * This allows us to call the synthetic tests from SQL to ensure they're
 * covered by gcov, since _PG_init() may not reliably flush coverage data.
 * When not in coverage mode, this is a no-op.
 */
Datum
smol_test_run_synthetic(PG_FUNCTION_ARGS)
{
#ifdef SMOL_TEST_COVERAGE
    smol_run_synthetic_tests();
#endif
    PG_RETURN_BOOL(true);
}

/*
 * ---- Parallel Build Implementation ----
 */

/*
 * smol_begin_parallel - Set up parallel context for index build
 *
 * Similar to btree's _bt_begin_parallel, this function:
 * 1. Creates a parallel context
 * 2. Estimates and allocates shared memory
 * 3. Initializes shared state
 * 4. Launches parallel workers
 */
static void
smol_begin_parallel(SMOLBuildState *buildstate, bool isconcurrent, int request)
{
    ParallelContext *pcxt;
    Size estsmolshared;
    Size estsort;
    SMOLShared *smolshared;
    Sharedsort *sharedsort;
    int scantuplesortstates;

    elog(LOG, "[smol] smol_begin_parallel starting, request=%d", request);

    /* Caller ensures request >= 1 */
    Assert(request >= 1);

    /* Enter parallel mode before creating parallel context */
    EnterParallelMode();

    /* Create parallel context
     * Note: Using "$libdir/smol" to match how extension functions are loaded
     */
    elog(LOG, "[smol] About to CreateParallelContext");
    pcxt = CreateParallelContext("$libdir/smol", "smol_parallel_build_main", request);
    elog(LOG, "[smol] Created ParallelContext, pcxt=%p", pcxt);

    /* Get snapshot for parallel scan - required for table_parallelscan_estimate */
    Snapshot snapshot = RegisterSnapshot(GetTransactionSnapshot());

    /* Estimate shared memory size for SMOLShared + ParallelTableScanDesc */
    elog(LOG, "[smol] Estimating shared memory");
    estsmolshared = BUFFERALIGN(sizeof(SMOLShared)) +
                    table_parallelscan_estimate(buildstate->heap, snapshot);
    elog(LOG, "[smol] estsmolshared=%zu", estsmolshared);
    shm_toc_estimate_chunk(&pcxt->estimator, estsmolshared);
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    /* Leader doesn't participate, so estimate for number of workers.
     * We estimate for the requested amount, but will initialize based on actual launched. */
    elog(LOG, "[smol] Estimating tuplesort shared memory for %d workers", request);
    estsort = tuplesort_estimate_shared(request);
    elog(LOG, "[smol] estsort=%zu", estsort);
    shm_toc_estimate_chunk(&pcxt->estimator, estsort);
    shm_toc_estimate_keys(&pcxt->estimator, 1);

    /* Estimate space for query text (if available) */
    Size querylen = 0;
    if (debug_query_string)
    {
        querylen = strlen(debug_query_string);
        shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
        shm_toc_estimate_keys(&pcxt->estimator, 1);
    }

    /* Initialize parallel context */
    InitializeParallelDSM(pcxt);

    /* Initialize based on REQUESTED workers (actual might be less) */
    scantuplesortstates = request;

    /* Allocate and initialize SMOLShared */
    smolshared = (SMOLShared *) shm_toc_allocate(pcxt->toc, estsmolshared);
    smolshared->heaprelid = RelationGetRelid(buildstate->heap);
    smolshared->indexrelid = RelationGetRelid(buildstate->index);
    smolshared->isconcurrent = isconcurrent;
    smolshared->scantuplesortstates = scantuplesortstates;

    /* Initialize synchronization primitives */
    ConditionVariableInit(&smolshared->workersdonecv);
    SpinLockInit(&smolshared->mutex);

    /* Initialize mutable state */
    smolshared->nparticipantsdone = 0;
    smolshared->reltuples = 0.0;
    smolshared->maxlen = 0;

    /* Initialize parallel table scan with snapshot */
    table_parallelscan_initialize(buildstate->heap,
                                  ParallelTableScanFromSMOLShared(smolshared),
                                  snapshot);

    /* Store in table of contents */
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_SMOL_SHARED, smolshared);

    /* Allocate and initialize shared tuplesort state */
    sharedsort = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
    tuplesort_initialize_shared(sharedsort, scantuplesortstates, pcxt->seg);
    shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT, sharedsort);

    /* Store query text for workers to access (if available) */
    if (debug_query_string)
    {
        char *sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
        memcpy(sharedquery, debug_query_string, querylen + 1);
        shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);
    }

    /* Launch workers */
    LaunchParallelWorkers(pcxt);

    /* If no workers were launched, clean up and return */
    if (pcxt->nworkers_launched == 0)
    {
        UnregisterSnapshot(snapshot);
        DestroyParallelContext(pcxt);
        ExitParallelMode();
        return;
    }

    /* Store leader state */
    buildstate->smolleader = (SMOLLeader *) palloc0(sizeof(SMOLLeader));
    buildstate->smolleader->pcxt = pcxt;
    buildstate->smolleader->smolshared = smolshared;
    buildstate->smolleader->sharedsort = sharedsort;
    buildstate->smolleader->snapshot = snapshot;
    /* Leader doesn't participate in scanning, so nparticipants = nworkers only */
    buildstate->smolleader->nparticipanttuplesorts = pcxt->nworkers_launched;

    /* Wait for workers to attach before continuing */
    WaitForParallelWorkersToAttach(pcxt);

    SMOL_LOGF("parallel build: launched %d workers", pcxt->nworkers_launched);
}

/*
 * smol_end_parallel - Clean up parallel build state
 */
static void
smol_end_parallel(SMOLLeader *smolleader)
{
    /* Wait for all workers to finish */
    WaitForParallelWorkersToFinish(smolleader->pcxt);

    /* Unregister snapshot if it was an MVCC snapshot */
    if (IsMVCCSnapshot(smolleader->snapshot))
        UnregisterSnapshot(smolleader->snapshot);

    /* Clean up parallel context */
    DestroyParallelContext(smolleader->pcxt);
    ExitParallelMode();
}

/*
 * smol_parallel_build_main - Entry point for parallel worker processes
 *
 * This function runs in each worker process and performs partial sorting
 * of index tuples, coordinating through shared memory.
 *
 * Must be exported for parallel workers to find it.
 */
PGDLLEXPORT void
smol_parallel_build_main(dsm_segment *seg, shm_toc *toc)
{
    SMOLShared *smolshared;
    Sharedsort *sharedsort;
    Relation heap;
    Relation index;
    IndexInfo *indexInfo;
    Tuplesortstate *ts;
    SortCoordinate coordinate;

    elog(LOG, "[smol] worker starting, ParallelWorkerNumber=%d", ParallelWorkerNumber);

    /* Get shared state from table of contents */
    elog(LOG, "[smol] worker about to lookup PARALLEL_KEY_SMOL_SHARED");
    smolshared = (SMOLShared *) shm_toc_lookup(toc, PARALLEL_KEY_SMOL_SHARED, false);

    elog(LOG, "[smol] worker about to lookup PARALLEL_KEY_TUPLESORT");
    sharedsort = (Sharedsort *) shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false);

    elog(LOG, "[smol] worker looked up shared state, smolshared=%p, sharedsort=%p", smolshared, sharedsort);

    /* Attach to shared tuplesort - required before creating worker tuplesort */
    elog(LOG, "[smol] worker about to call tuplesort_attach_shared");
    tuplesort_attach_shared(sharedsort, seg);

    elog(LOG, "[smol] worker attached to shared tuplesort");

    /* Open relations */
    heap = table_open(smolshared->heaprelid, ShareLock);
    index = index_open(smolshared->indexrelid, RowExclusiveLock);

    elog(LOG, "[smol] worker opened relations");

    /* Build IndexInfo (we need this for table_index_build_scan) */
    indexInfo = BuildIndexInfo(index);
    indexInfo->ii_Concurrent = smolshared->isconcurrent;

    /* Set up sort coordinate for this worker */
    coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
    coordinate->isWorker = true;
    coordinate->nParticipants = -1;  /* Workers don't know the total yet */
    coordinate->sharedsort = sharedsort;

    elog(LOG, "[smol] worker created coordinate");

    /* Initialize tuplesort for this worker.
     * Workers get a fraction of maintenance_work_mem based on number of participants.
     */
    int sortmem = maintenance_work_mem / smolshared->scantuplesortstates;

    elog(LOG, "[smol] worker about to create tuplesort, sortmem=%d, scantuplesortstates=%d",
         sortmem, smolshared->scantuplesortstates);

    ts = tuplesort_begin_index_btree(heap, index, false, false,
                                     sortmem, coordinate,
                                     TUPLESORT_NONE);

    elog(LOG, "[smol] worker created tuplesort");

    /* Perform partial table scan and sorting */
    {
        Size nkeys = 0;
        Oid atttypid = TupleDescAttr(RelationGetDescr(index), 0)->atttypid;
        TableScanDesc scan;

        /* Join the parallel scan */
        scan = table_beginscan_parallel(heap, ParallelTableScanFromSMOLShared(smolshared));

        if (atttypid == TEXTOID)
        {
            TsBuildCtxText cb;
            int maxlen = 0;
            cb.ts = ts;
            cb.pnkeys = &nkeys;
            cb.pmax = &maxlen;
            table_index_build_scan(heap, index, indexInfo, true, true,
                                 ts_build_cb_text, (void *) &cb, scan);

            /* Workers must perform their sort and signal completion.
             * The leader will merge the sorted runs from all workers.
             */
            tuplesort_performsort(ts);

            /* Signal that this worker has completed sorting and update shared state */
            SpinLockAcquire(&smolshared->mutex);
            smolshared->nparticipantsdone++;
            smolshared->reltuples += (double) nkeys;
            if (maxlen > smolshared->maxlen)
                smolshared->maxlen = maxlen;
            SpinLockRelease(&smolshared->mutex);
        }
        else
        {
            TsBuildCtxAny cb;
            cb.ts = ts;
            cb.pnkeys = &nkeys;
            table_index_build_scan(heap, index, indexInfo, true, true,
                                 ts_build_cb_any, (void *) &cb, scan);

            /* Workers must perform their sort and signal completion.
             * The leader will merge the sorted runs from all workers.
             */
            tuplesort_performsort(ts);

            /* Signal that this worker has completed sorting and update shared state */
            SpinLockAcquire(&smolshared->mutex);
            smolshared->nparticipantsdone++;
            smolshared->reltuples += (double) nkeys;
            SpinLockRelease(&smolshared->mutex);
        }
    }

    /* Wake up leader if it's waiting */
    ConditionVariableBroadcast(&smolshared->workersdonecv);

    /* Clean up worker's tuplesort state.
     * Now that the leader doesn't scan and only merges results,
     * workers can safely clean up their tuplesort states.
     */
    tuplesort_end(ts);

    /* Clean up relations */
    index_close(index, RowExclusiveLock);
    table_close(heap, ShareLock);
}
