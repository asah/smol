/* smol.c - PostgreSQL SMOL index access method */
#include "smol.h"

PG_MODULE_MAGIC;

/* GUC variable definitions (not extern declarations - those are in smol.h) */
bool smol_debug_log = false;
bool smol_profile_log = false;
double smol_cost_page = 1.0;
double smol_cost_tup = 0.01;
int smol_parallel_claim_batch = 16;
int smol_prefetch_depth = 0;
double smol_rle_uniqueness_threshold = 0.95;
int smol_key_rle_version = KEY_RLE_AUTO;
bool smol_use_position_scan = true;

/* Zone maps + bloom filters GUCs */
bool smol_zone_maps = true;
bool smol_bloom_filters = true;
bool smol_build_zone_maps = true;
bool smol_build_bloom_filters = true;
int smol_bloom_nhash = 2;

#ifdef SMOL_TEST_COVERAGE
int smol_test_keylen_inflate = 0;
int smol_simulate_atomic_race = 0;
int smol_atomic_race_counter = 0;
int smol_cas_fail_counter = 0;
int smol_cas_fail_every = 0;
int smol_growth_threshold_test = 0;
int smol_force_loop_guard_test = 0;
int smol_loop_guard_iteration = 0;
int smol_test_force_realloc_at = 0;
bool smol_test_force_page_bounds_check = false;
int smol_test_force_parallel_workers = 0;
int smol_test_max_internal_fanout = 0;
int smol_test_max_tuples_per_page = 0;
int smol_test_leaf_offset = 0;
#endif

/* Sorting globals (used by smol_build.c) */
char *smol_sort_k1_buffer = NULL, *smol_sort_k2_buffer = NULL;
uint16 smol_sort_key_len1 = 0, smol_sort_key_len2 = 0;
bool smol_sort_byval1 = false, smol_sort_byval2 = false;
FmgrInfo smol_sort_cmp1, smol_sort_cmp2;
Oid smol_sort_coll1 = InvalidOid, smol_sort_coll2 = InvalidOid;
Oid smol_sort_typoid1 = InvalidOid, smol_sort_typoid2 = InvalidOid;

#ifdef SMOL_TEST_COVERAGE
/* Forward declaration */
static void smol_run_synthetic_tests(void);
#endif

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

    DefineCustomBoolVariable("smol.use_position_scan",
                             "Use position-based scan optimization",
                             "When on, SMOL uses two tree searches to find start/end positions and eliminates per-tuple comparisons.",
                             &smol_use_position_scan,
                             true,
                             PGC_USERSET,
                             0,
                            NULL, NULL, NULL);

    /* RLE is always considered; no GUC. */
    /* Logging constants (progress_log_every, wait_log_ms) are now internal #defines */

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

    DefineCustomIntVariable("smol.test_leaf_offset",
                            "TEST ONLY: Force find_first_leaf to return N blocks earlier",
                            "For coverage testing: forces scan through multiple leaves (0=disabled)",
                            &smol_test_leaf_offset,
                            0, /* default: disabled */
                            0, /* min */
                            1000, /* max */
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);
#endif

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

    /* Selectivity constants (selec_eq, selec_range) removed - using hard-coded estimates */

#ifdef SMOL_TEST_COVERAGE
    /* Evil testing hacks - only available in coverage builds */
    DefineCustomIntVariable("smol.test_keylen_inflate",
                            "Test coverage: artificially inflate key_len calculations",
                            NULL,
                            &smol_test_keylen_inflate,
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

    /* Removed minor tuning GUCs for simplicity: skip_dup_copy, log_hex_limit, log_sample_n */

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

    DefineCustomRealVariable("smol.rle_uniqueness_threshold",
                            "Uniqueness threshold for RLE format (nruns/nitems)",
                            "If nruns/nitems >= this threshold, keys are considered unique",
                            &smol_rle_uniqueness_threshold,
                            0.98, 0.0, 1.0,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

    /* Zone maps + bloom filters GUCs */
    DefineCustomBoolVariable("smol.zone_maps",
                            "Enable zone map filtering during scan",
                            "When on, SMOL uses min/max statistics to skip subtrees that can't match query predicates.",
                            &smol_zone_maps,
                            true,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomBoolVariable("smol.bloom_filters",
                            "Enable bloom filter checks during scan",
                            "When on, SMOL uses bloom filters to skip subtrees for equality predicates.",
                            &smol_bloom_filters,
                            true,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomBoolVariable("smol.build_zone_maps",
                            "Collect zone maps during index build",
                            "When on, SMOL stores min/max statistics in internal nodes (must be set before CREATE INDEX).",
                            &smol_build_zone_maps,
                            true,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomBoolVariable("smol.build_bloom_filters",
                            "Build bloom filters during index build",
                            "When on, SMOL builds bloom filters for each page (must be set before CREATE INDEX).",
                            &smol_build_bloom_filters,
                            true,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("smol.bloom_nhash",
                            "Number of hash functions for bloom filters (1-4)",
                            "Higher values reduce false positives but increase computation cost.",
                            &smol_bloom_nhash,
                            2, 1, 4,
                            PGC_USERSET, 0,
                            NULL, NULL, NULL);

#ifdef SMOL_TEST_COVERAGE
    /* Run synthetic tests on first load */
    smol_run_synthetic_tests();
#endif
}

/* --- Handler: wire a minimal IndexAmRoutine --- */
PG_FUNCTION_INFO_V1(smol_handler);
Datum
smol_handler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *am = makeNode(IndexAmRoutine);

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
    am->amgettuple = smol_gettuple;
    am->amgetbitmap = NULL;
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

/* AM callback stubs and helpers */
bool
smol_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
            Relation heapRel, IndexUniqueCheck checkUnique, bool indexUnchanged,
            struct IndexInfo *indexInfo)
{
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("smol is read-only: aminsert is not supported")));
    return false;
}

bytea *
smol_options(Datum reloptions, bool validate)
{
    (void) reloptions; (void) validate;
    return NULL;
}

bool
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
    Assert(HeapTupleIsValid(classtup)); /* Validator should only be called with valid opclassoid */
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
        if (procform->amproclefttype != opcintype)
            continue;

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

void
smol_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
                  Cost *indexStartupCost, Cost *indexTotalCost,
                  Selectivity *indexSelectivity, double *indexCorrelation,
                  double *indexPages)
{
    GenericCosts costs;

    /* Use genericcostestimate for standard index cost calculation with parallel support */
    MemSet(&costs, 0, sizeof(costs));
    genericcostestimate(root, path, loop_count, &costs);

    /* Apply SMOL-specific cost multipliers */
    if (smol_cost_page != 1.0 && costs.spc_random_page_cost > 0.0)
    {
        Cost io_cost = costs.numIndexPages * costs.spc_random_page_cost;
        Cost cpu_cost = costs.indexTotalCost - io_cost;
        costs.indexTotalCost = (io_cost * smol_cost_page) + cpu_cost;
    }

    if (smol_cost_tup != 1.0)
    {
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

IndexBulkDeleteResult *
smol_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    (void) info;
    (void) stats;
    return NULL;
}

CompareType
smol_translatestrategy(StrategyNumber strat, Oid opfamily)
{
    CompareType result;
    (void) opfamily;

    Assert(strat >= 1 && strat <= 5);
    result = (CompareType) strat;

    return result;
}

StrategyNumber
smol_translatecmptype(CompareType cmptype, Oid opfamily)
{
    (void) opfamily;

    Assert(cmptype >= COMPARE_LT && cmptype <= COMPARE_GT);
    return (StrategyNumber) cmptype;
}

#ifdef SMOL_TEST_COVERAGE
/* Synthetic tests for copy functions */
static void
smol_run_synthetic_tests(void)
{
    /* Synthetic tests for unaligned copy paths */
    {
        char src_buf[64] __attribute__((aligned(16)));
        char dst_buf[64] __attribute__((aligned(16)));

        /* Initialize source with test pattern */
        for (int i = 0; i < 64; i++) src_buf[i] = (char)(0x10 + i);

        /* Test smol_copy2 with unaligned pointers */
        memset(dst_buf, 0, 64);
        smol_copy2(dst_buf + 1, src_buf + 1);
        Assert(dst_buf[1] == src_buf[1] && dst_buf[2] == src_buf[2]);

        /* Test smol_copy16 with unaligned pointers */
        memset(dst_buf, 0, 64);
        smol_copy16(dst_buf + 1, src_buf + 1);
        for (int i = 0; i < 16; i++)
            Assert(dst_buf[1 + i] == src_buf[1 + i]);

        /* Test smol_copy_small for all switch cases */
        for (int len = 1; len <= 16; len++)
        {
            memset(dst_buf, 0, 64);
            smol_copy_small(dst_buf, src_buf, len);
            for (int i = 0; i < len; i++)
                Assert(dst_buf[i] == src_buf[i]);
        }

        /* Test default case with various sizes */
        int default_test_lengths[] = {
            17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
            33, 34, 35, 36, 40, 48, 50, 60
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

    /* Test smol_options() - AM option parsing */
    {
        Assert(smol_options(PointerGetDatum(NULL), false) == NULL);
        Assert(smol_options(PointerGetDatum(NULL), true) == NULL);
        elog(DEBUG1, "SMOL: smol_options() synthetic test passed");
    }
}

#endif  /* SMOL_TEST_COVERAGE */

/*
 * smol_test_run_synthetic - Explicitly run synthetic tests for coverage
 *
 * This allows us to call the synthetic tests from SQL to ensure they're
 * covered by gcov, since _PG_init() may not reliably flush coverage data.
 * When not in coverage mode, this is a no-op.
 */
PG_FUNCTION_INFO_V1(smol_test_run_synthetic);
Datum
smol_test_run_synthetic(PG_FUNCTION_ARGS)
{
#ifdef SMOL_TEST_COVERAGE
    smol_run_synthetic_tests();
#endif
    PG_RETURN_BOOL(true);
}
