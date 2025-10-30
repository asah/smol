/*
 * smol_build.c
 *   Build functions for SMOL index access method
 *
 * This file contains:
 * - Index build functionality (smol_build, smol_buildempty)
 * - Tree construction from sorted data
 * - Parallel build support
 * - Tuplesort integration
 */

#include "smol.h"

/* Forward declarations for static functions */
static int smol_pair_qsort_cmp(const void *a, const void *b);
static int smol_qsort_cmp_bytes(const void *a, const void *b);
static void ts_build_cb_any(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void ts_build_cb_text(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void smol_build_cb_pair(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void smol_sort_pairs_rows64(int64 *k1, int64 *k2, Size n);
static void smol_build_cb_inc(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
static void smol_begin_parallel(SMOLBuildState *buildstate, bool isconcurrent, int request);
static void smol_end_parallel(SMOLLeader *smolleader);
static void smol_build_tree1_inc_from_sorted(Relation idx, const int64 *keys, const char * const *incs, Size nkeys, uint16 key_len, int inc_count, const uint16 *inc_lens);
static void smol_build_text_inc_from_sorted(Relation idx, const char *keys32, const char * const *incs, Size nkeys, uint16 key_len, int inc_count, const uint16 *inc_lens);
static void smol_build_internal_levels(Relation idx, BlockNumber *leaf_blks, const int64 *leaf_highkeys, Size nleaves, uint16 key_len, BlockNumber *out_root, uint16 *out_levels);
static void smol_build_internal_levels_bytes(Relation idx, BlockNumber *leaf_blks, const char *leaf_highkeys, Size nleaves, uint16 key_len, BlockNumber *out_root, uint16 *out_levels);
static void smol_build_internal_levels_with_stats(Relation idx, SmolLeafStats *leaf_stats, Size nleaves, uint16 key_len, BlockNumber *out_root, uint16 *out_levels);
static void smol_build_text_stream_from_tuplesort(Relation idx, Tuplesortstate *ts, Size nkeys, uint16 key_len);
static void smol_build_fixed_stream_from_tuplesort(Relation idx, Tuplesortstate *ts, Size nkeys, uint16 key_len, bool byval);


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

static int
smol_pair_qsort_cmp(const void *pa, const void *pb)
{
    uint32 ia = *(const uint32 *) pa, ib = *(const uint32 *) pb;
    char *a1 = smol_sort_k1_buffer + (size_t) ia * smol_sort_key_len1;
    char *b1 = smol_sort_k1_buffer + (size_t) ib * smol_sort_key_len1;

    /* Fast path: inline comparison for common integer types */
    int32 r1;
    if (smol_sort_typoid1 == INT2OID && smol_sort_key_len1 == 2)
    {
        int16 v1 = *(int16*)a1, v2 = *(int16*)b1;
        r1 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else if (smol_sort_typoid1 == INT4OID && smol_sort_key_len1 == 4)
    {
        int32 v1 = *(int32*)a1, v2 = *(int32*)b1;
        r1 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else if (smol_sort_typoid1 == INT8OID && smol_sort_key_len1 == 8)
    {
        int64 v1 = *(int64*)a1, v2 = *(int64*)b1;
        r1 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else
    {
        /* Generic path: use FunctionCall2Coll for other types */
        Datum da1 = smol_sort_byval1 ? (smol_sort_key_len1==1?CharGetDatum(*a1): smol_sort_key_len1==2?Int16GetDatum(*(int16*)a1): smol_sort_key_len1==4?Int32GetDatum(*(int32*)a1): Int64GetDatum(*(int64*)a1)) : PointerGetDatum(a1);
        Datum db1 = smol_sort_byval1 ? (smol_sort_key_len1==1?CharGetDatum(*b1): smol_sort_key_len1==2?Int16GetDatum(*(int16*)b1): smol_sort_key_len1==4?Int32GetDatum(*(int32*)b1): Int64GetDatum(*(int64*)b1)) : PointerGetDatum(b1);
        r1 = DatumGetInt32(FunctionCall2Coll(&smol_sort_cmp1, smol_sort_coll1, da1, db1));
    }
    if (r1 != 0) return r1;

    char *a2 = smol_sort_k2_buffer + (size_t) ia * smol_sort_key_len2;
    char *b2 = smol_sort_k2_buffer + (size_t) ib * smol_sort_key_len2;

    /* Fast path: inline comparison for common integer types */
    int32 r2;
    if (smol_sort_typoid2 == INT2OID && smol_sort_key_len2 == 2)
    {
        int16 v1 = *(int16*)a2, v2 = *(int16*)b2;
        r2 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else if (smol_sort_typoid2 == INT4OID && smol_sort_key_len2 == 4)
    {
        int32 v1 = *(int32*)a2, v2 = *(int32*)b2;
        r2 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else if (smol_sort_typoid2 == INT8OID && smol_sort_key_len2 == 8)
    {
        int64 v1 = *(int64*)a2, v2 = *(int64*)b2;
        r2 = (v1 > v2) ? 1 : ((v1 < v2) ? -1 : 0);
    }
    else
    {
        /* Generic path: use FunctionCall2Coll for other types */
        Datum da2 = smol_sort_byval2 ? (smol_sort_key_len2==1?CharGetDatum(*a2): smol_sort_key_len2==2?Int16GetDatum(*(int16*)a2): smol_sort_key_len2==4?Int32GetDatum(*(int32*)a2): Int64GetDatum(*(int64*)a2)) : PointerGetDatum(a2);
        Datum db2 = smol_sort_byval2 ? (smol_sort_key_len2==1?CharGetDatum(*b2): smol_sort_key_len2==2?Int16GetDatum(*(int16*)b2): smol_sort_key_len2==4?Int32GetDatum(*(int32*)b2): Int64GetDatum(*(int64*)b2)) : PointerGetDatum(b2);
        r2 = DatumGetInt32(FunctionCall2Coll(&smol_sort_cmp2, smol_sort_coll2, da2, db2));
    }
    return r2;
}

/* qsort comparator for fixed-size byte keys (uses smol_sort_k1_buffer/smol_sort_key_len1) */
static int
smol_qsort_cmp_bytes(const void *pa, const void *pb)
{
    uint32 ia = *(const uint32 *) pa, ib = *(const uint32 *) pb;
    const char *a = smol_sort_k1_buffer + (size_t) ia * smol_sort_key_len1;
    const char *b = smol_sort_k1_buffer + (size_t) ib * smol_sort_key_len1;
    return memcmp(a, b, smol_sort_key_len1);
}

/* Radix sort for two-column int64 pairs - sorts by k2 then k1 for row-major layout */
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

/* Background worker: sort assigned bucket ranges in-place inside DSM arrays */
/* --- Minimal implementations --- */
IndexBuildResult *
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

    /* Enforce INCLUDE column limit (hardcoded array size in SmolMeta and SmolScanOpaque) */
    if (ninclude > 16)
        ereport(ERROR, (errmsg("smol supports at most 16 INCLUDE columns, got %d", ninclude)));

    if (ninclude > 0)
    {
        /* INCLUDE columns (fixed-width ints or text) - supports single or multi-key indexes */
        int inc_count = ninclude;
        uint16 inc_lens[16]; bool inc_byval[16]; bool inc_is_text[16];
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

        /* Validate total row size (conservative estimate with alignment overhead) */
        {
            Size total_row_size = key_len;
            if (nkeyatts == 2)
                total_row_size += key_len2;
            for (int i = 0; i < inc_count; i++)
                total_row_size += inc_lens[i];
            /* Add conservative alignment overhead: ~8 bytes per column */
            total_row_size += (nkeyatts + inc_count) * 8;
            /* Add IndexTuple header overhead (~8 bytes) */
            total_row_size += MAXALIGN(sizeof(IndexTupleData));

            /* Warn if row size exceeds 250 bytes (leaves room for ~32 rows/page minimum) */
            if (total_row_size > 250)
                ereport(WARNING,
                    (errmsg("smol index row size may be large: estimated %zu bytes", total_row_size),
                     errdetail("Large rows reduce the number of tuples per page, degrading performance."),
                     errhint("Consider reducing the number or size of INCLUDE columns.")));
        }

        /* Collect keys + includes into arrays */
        Size cap = 0, n = 0;
        int64 *karr = NULL;
        char *incarr[16]; memset(incarr, 0, sizeof(incarr));
        SmolIncludeContext cctx; memset(&cctx, 0, sizeof(cctx));
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
                smol_sort_k1_buffer = k1buf; smol_sort_k2_buffer = k2buf; smol_sort_key_len1 = key_len; smol_sort_key_len2 = key_len2; smol_sort_byval1 = cctx.byval1; smol_sort_byval2 = cctx.byval2; smol_sort_coll1 = coll1; smol_sort_coll2 = coll2; smol_sort_typoid1 = typoid1; smol_sort_typoid2 = typoid2; memcpy(&smol_sort_cmp1, &cmp1, sizeof(FmgrInfo)); memcpy(&smol_sort_cmp2, &cmp2, sizeof(FmgrInfo));
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
                    SmolMeta *m = smol_meta_ptr(pg); m->magic=SMOL_META_MAGIC; m->version=SMOL_META_VERSION; m->nkeyatts=2; m->key_len1=key_len; m->key_len2=key_len2; m->root_blkno=InvalidBlockNumber; m->height=0; m->inc_count=inc_count; m->directory_blkno=InvalidBlockNumber;
                    for (int i=0;i<inc_count;i++) { m->inc_len[i]=inc_lens[i]; }
                    smol_meta_init_zone_maps(m);
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
                    smol_sort_k1_buffer = kbytes; smol_sort_key_len1 = key_len; /* reuse globals for simple cmp */
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
            /* Empty index: metapage already initialized by smol_buildempty */
            if (!cctx.key_is_text32)
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
        /* Single-key text: sort with tuplesort then pack to 32-byte padded keys (supports any collation) */
        TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_LT_OPR);
        Tuplesortstate *ts;
        SmolTextBuildContext cb; int maxlen = 0;
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

        /* Choose key length based on max text length (8/16/32 bytes)
         * We store original text (not transformed), so all collations use same sizing */
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
        SmolTuplesortContext gcb; gcb.ts = ts; gcb.pnkeys = &nkeys;

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
        SmolPairContext cctx = { &k1buf, &k2buf, &cap, &n, key_len, key_len2, false, false };
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
                smol_sort_k1_buffer = k1buf; smol_sort_k2_buffer = k2buf; smol_sort_key_len1 = key_len; smol_sort_key_len2 = key_len2; smol_sort_byval1 = cctx.byval1; smol_sort_byval2 = cctx.byval2; smol_sort_coll1 = coll1; smol_sort_coll2 = coll2; smol_sort_typoid1 = typoid1; smol_sort_typoid2 = typoid2; memcpy(&smol_sort_cmp1, &cmp1, sizeof(FmgrInfo)); memcpy(&smol_sort_cmp2, &cmp2, sizeof(FmgrInfo));
                qsort(idx, n, sizeof(uint32), smol_pair_qsort_cmp);
                INSTR_TIME_SET_CURRENT(t_sort_end);
            }
            /* init meta if new */
            if (RelationGetNumberOfBlocks(index) == 0)
            {
                Buffer mb = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
                LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE); Page pg = BufferGetPage(mb); PageInit(pg, BLCKSZ, 0);
                SmolMeta *m = smol_meta_ptr(pg); m->magic=SMOL_META_MAGIC; m->version=SMOL_META_VERSION; m->nkeyatts=2; m->key_len1=key_len; m->key_len2=key_len2; m->root_blkno=InvalidBlockNumber; m->height=0; m->directory_blkno=InvalidBlockNumber; smol_meta_init_zone_maps(m); MarkBufferDirty(mb); UnlockReleaseBuffer(mb);
            }
            /* Write leaves in generic row-major 2-key layout:
             * payload: [uint16 nrows][row0: k1||k2][row1: k1||k2]...
             * One ItemId (FirstOffsetNumber) per leaf.
             */
            Size i = 0; BlockNumber prev = InvalidBlockNumber; char *scratch = (char *) palloc(BLCKSZ);

            /* Track leaf pages for building internal levels with zone map stats */
            Size nleaves = 0, aleaves = 0;
            SmolLeafStats *leaf_stats = NULL;
            Oid typid = atttypid; /* First key type for zone maps */

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
                if (BlockNumberIsValid(prev)) { Buffer pb = ReadBuffer(index, prev); LockBuffer(pb, BUFFER_LOCK_EXCLUSIVE); Page pp=BufferGetPage(pb); smol_page_opaque(pp)->rightlink=cur; MarkBufferDirty(pb); UnlockReleaseBuffer(pb);} prev = cur;

                /* Track this leaf and collect zone map statistics */
                if (nleaves == aleaves)
                {
                    aleaves = (aleaves == 0 ? 64 : aleaves * 2);
                    leaf_stats = (leaf_stats == NULL) ? (SmolLeafStats *) palloc(aleaves * sizeof(SmolLeafStats))
                                                      : (SmolLeafStats *) repalloc(leaf_stats, aleaves * sizeof(SmolLeafStats));
                }

                /* Collect zone map statistics for this leaf (first key only for two-column) */
                if (smol_build_zone_maps)
                {
                    /* For two-column indexes, extract first key column for zone map statistics */
                    char *keys_for_stats = (char *) palloc(n_this * key_len);
                    if (use_radix)
                    {
                        /* Data is already sorted, copy k1 values */
                        for (Size j = 0; j < n_this; j++)
                            memcpy(keys_for_stats + j * key_len, k1buf + (i + j) * key_len, key_len);
                    }
                    else
                    {
                        /* Use index permutation */
                        for (Size j = 0; j < n_this; j++)
                        {
                            uint32 id = idx[i + j];
                            memcpy(keys_for_stats + j * key_len, k1buf + id * key_len, key_len);
                        }
                    }
                    smol_collect_leaf_stats(&leaf_stats[nleaves], keys_for_stats, n_this, key_len, typid, cur);
                    pfree(keys_for_stats);
                }
                else
                {
                    /* Zone maps disabled: fill minimal stats */
                    leaf_stats[nleaves].blk = cur;
                    /* Store last key's first 4 bytes as maxkey */
                    if (use_radix)
                        memcpy(&leaf_stats[nleaves].maxkey, k1buf + (i + n_this - 1) * key_len, Min(sizeof(int32), key_len));
                    else
                        memcpy(&leaf_stats[nleaves].maxkey, k1buf + idx[i + n_this - 1] * key_len, Min(sizeof(int32), key_len));
                    leaf_stats[nleaves].minkey = 0;
                    leaf_stats[nleaves].row_count = 0;
                    leaf_stats[nleaves].distinct_count = 0;
                    leaf_stats[nleaves].bloom_filter = 0;
                    leaf_stats[nleaves].padding = 0;
                }

                nleaves++;
                i += n_this;
            }

            /* Build internal levels with zone map aggregation if we have multiple leaves */
            if (nleaves > 1)
            {
                BlockNumber rootblk;
                uint16 levels;
                smol_build_internal_levels_with_stats(index, leaf_stats, nleaves, key_len, &rootblk, &levels);
            }
            else if (nleaves == 1)
            {
                /* Single leaf: set root directly */
                Buffer mb = ReadBuffer(index, 0);
                LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
                Page pg = BufferGetPage(mb);
                SmolMeta *m = smol_meta_ptr(pg);
                m->root_blkno = leaf_stats[0].blk;
                m->height = 1;
                MarkBufferDirty(mb);
                UnlockReleaseBuffer(mb);
            }

            if (leaf_stats) pfree(leaf_stats);
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

    /* Build leaf directory for parallel scans (only for large indexes) */
    BlockNumber nblocks = RelationGetNumberOfBlocks(index);

    /* DISABLED: Directory-based parallel scan has race condition causing 1-4% overcounting */
    /* TODO: Debug and fix the chunk boundary logic in smol_scan.c */
    if (nblocks > 1000000)  /* Effectively disabled - was: Build directory for large indexes (>100 blocks) */
    { /* GCOV_EXCL_START - directory feature disabled due to bug */
        elog(LOG, "smol_build: calling directory build");
        BlockNumber dir_blk = smol_build_and_write_directory(index);

        if (BlockNumberIsValid(dir_blk))
        {
            Buffer mbuf = ReadBuffer(index, 0);
            Page mpage;
            SmolMeta *meta;

            LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
            mpage = BufferGetPage(mbuf);
            meta = smol_meta_ptr(mpage);
            meta->directory_blkno = dir_blk;
            MarkBufferDirty(mbuf);
            UnlockReleaseBuffer(mbuf);

            SMOL_LOGF("stored directory block %u in metadata", dir_blk);
        }
    } /* GCOV_EXCL_STOP */

    return res;
}

void
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
    meta->directory_blkno = InvalidBlockNumber;  /* No directory until index is large enough */
    meta->collation_oid = InvalidOid;  /* No data yet, will be set during build */
    /* Set default INCLUDE lengths (will be overwritten by build callback if data exists) */
    for (int i = 0; i < ninclude && i < 16; i++)
    {
        meta->inc_len[i] = sizeof(int32); /* Default assumption */
    }
    /* Initialize zone map fields */
    smol_meta_init_zone_maps(meta);
    MarkBufferDirty(buf);
    UnlockReleaseBuffer(buf);
}



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
        meta->directory_blkno = InvalidBlockNumber;
        meta->collation_oid = InvalidOid;  /* Fixed-width keys, no collation */
        for (int i=0;i<inc_count;i++) meta->inc_len[i] = inc_lens[i];
        smol_meta_init_zone_maps(meta);
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
    }
    if (nkeys == 0)
        return;

    Size i = 0; BlockNumber prev = InvalidBlockNumber; char *scratch = (char *) palloc(BLCKSZ);
    Size ninc_bytes = 0; for (int c=0;c<inc_count;c++) ninc_bytes += inc_lens[c];

    /* Track leaf pages for building internal levels (with zone map stats) */
    Size nleaves = 0, aleaves = 0;
    SmolLeafStats *leaf_stats = NULL;

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
                /* TEST-ONLY: smol_test_* GUC check (compiled out in production) */
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

        /* Track this leaf and collect zone map statistics */
        if (nleaves == aleaves)
        {
            aleaves = (aleaves == 0 ? 64 : aleaves * 2);
            leaf_stats = (leaf_stats == NULL) ? (SmolLeafStats *) palloc(aleaves * sizeof(SmolLeafStats))
                                              : (SmolLeafStats *) repalloc(leaf_stats, aleaves * sizeof(SmolLeafStats));
        }

        /* Collect zone map statistics for this leaf */
        if (smol_build_zone_maps)
        {
            /* keys is int64 array, need to extract first/last correctly */
            int64 first_val = keys[i];
            int64 last_val = keys[i + n_this - 1];

            leaf_stats[nleaves].blk = cur;
            leaf_stats[nleaves].row_count = n_this;

            /* Extract min/max based on actual key type */
            if (key_len == 2)
            {
                leaf_stats[nleaves].minkey = (int32)(int16)first_val;
                leaf_stats[nleaves].maxkey = (int32)(int16)last_val;
            }
            else if (key_len == 4)
            {
                leaf_stats[nleaves].minkey = (int32)first_val;
                leaf_stats[nleaves].maxkey = (int32)last_val;
            }
            else /* key_len == 8 */
            {
                leaf_stats[nleaves].minkey = (int32)first_val;
                leaf_stats[nleaves].maxkey = (int32)last_val;
            }

            leaf_stats[nleaves].distinct_count = (uint16)Min(n_this, 65535);
            leaf_stats[nleaves].bloom_filter = 0;
            leaf_stats[nleaves].padding = 0;
        }
        else
        {
            /* Zone maps disabled: fill minimal stats */
            leaf_stats[nleaves].blk = cur;
            leaf_stats[nleaves].maxkey = (int32)keys[i + n_this - 1]; /* highkey */
            leaf_stats[nleaves].minkey = 0;
            leaf_stats[nleaves].row_count = 0;
            leaf_stats[nleaves].distinct_count = 0;
            leaf_stats[nleaves].bloom_filter = 0;
            leaf_stats[nleaves].padding = 0;
        }
        nleaves++;

        i += n_this;
    }

    /* Build internal levels if we have multiple leaves */
    if (nleaves == 1)
    {
        /* Single leaf: set it as root */
        mbuf = ReadBuffer(idx, 0);
        LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
        mpage = BufferGetPage(mbuf);
        meta = smol_meta_ptr(mpage);
        meta->root_blkno = 1;
        meta->height = 1;
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
    }
    else
    {
        /* Multiple leaves: build internal levels with zone map stats */
        BlockNumber rootblk = InvalidBlockNumber;
        uint16 levels = 0;

        smol_build_internal_levels_with_stats(idx, leaf_stats, nleaves, key_len, &rootblk, &levels);

        pfree(leaf_stats);
    }

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
        meta->directory_blkno = InvalidBlockNumber;
        meta->collation_oid = InvalidOid;  /* Fixed-width keys, no collation */
        for (int i=0;i<inc_count;i++) meta->inc_len[i] = inc_lens[i];
        smol_meta_init_zone_maps(meta);
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
    }
    if (nkeys == 0) return;
    Size i = 0; BlockNumber prev = InvalidBlockNumber; char *scratch = (char *) palloc(BLCKSZ);
    Size ninc_bytes = 0; for (int c=0;c<inc_count;c++) ninc_bytes += inc_lens[c];

    /* Track leaf pages for building internal levels (with zone map stats) */
    Size nleaves = 0, aleaves = 0;
    SmolLeafStats *leaf_stats = NULL;
    Oid typid = TEXTOID;  /* Text key type for zone maps */

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
                /* TEST-ONLY: smol_test_* GUC check (compiled out in production) */
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

        /* Track this leaf and collect zone map statistics */
        if (nleaves == aleaves)
        {
            aleaves = (aleaves == 0 ? 64 : aleaves * 2);
            leaf_stats = (leaf_stats == NULL) ? (SmolLeafStats *) palloc(aleaves * sizeof(SmolLeafStats))
                                              : (SmolLeafStats *) repalloc(leaf_stats, aleaves * sizeof(SmolLeafStats));
        }

        /* Collect zone map statistics for this leaf */
        if (smol_build_zone_maps)
        {
            smol_collect_leaf_stats(&leaf_stats[nleaves], keys32 + i * key_len, n_this, key_len, typid, cur);
        }
        else
        {
            /* Zone maps disabled: fill minimal stats */
            leaf_stats[nleaves].blk = cur;
            /* For text, store first 4 bytes of last key as maxkey */
            memcpy(&leaf_stats[nleaves].maxkey, keys32 + (i + n_this - 1) * key_len, Min(sizeof(int32), key_len));
            leaf_stats[nleaves].minkey = 0;
            leaf_stats[nleaves].row_count = 0;
            leaf_stats[nleaves].distinct_count = 0;
            leaf_stats[nleaves].bloom_filter = 0;
            leaf_stats[nleaves].padding = 0;
        }
        nleaves++;

        i += n_this;
    }

    /* Build internal levels if we have multiple leaves */
    if (nleaves == 1)
    {
        /* Single leaf: set it as root */
        mbuf = ReadBuffer(idx, 0);
        LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
        mpage = BufferGetPage(mbuf);
        meta = smol_meta_ptr(mpage);
        meta->root_blkno = 1;
        meta->height = 1;
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
    }
    else
    {
        /* Multiple leaves: build internal levels with zone map stats */
        BlockNumber rootblk = InvalidBlockNumber;
        uint16 levels = 0;

        smol_build_internal_levels_with_stats(idx, leaf_stats, nleaves, key_len, &rootblk, &levels);

        pfree(leaf_stats);
    }

    pfree(scratch);
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
                /* TEST-ONLY: smol_test_* GUC check (compiled out in production) */
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
/*
 * smol_build_internal_levels_with_stats - Build internal tree levels with zone map metadata
 *
 * This version accepts SmolLeafStats and aggregates zone map statistics up the tree.
 */
static void
smol_build_internal_levels_with_stats(Relation idx,
                                      SmolLeafStats *leaf_stats, Size nleaves,
                                      uint16 key_len,
                                      BlockNumber *out_root, uint16 *out_levels)
{
    SmolMeta meta;
    smol_meta_read(idx, &meta);
    bool zone_maps_enabled = meta.zone_maps_enabled;

    /* Convert leaf stats to current level stats */
    Size cur_n = nleaves;
    SmolLeafStats *cur_stats = leaf_stats;
    uint16 levels = 0;

    while (cur_n > 1)
    {
        Size cap_next = (cur_n / 2) + 2;
        SmolLeafStats *next_stats = (SmolLeafStats *) palloc(cap_next * sizeof(SmolLeafStats));
        Size next_n = 0;
        Size i = 0;

        while (i < cur_n)
        {
            Buffer ibuf = smol_extend(idx);
            smol_init_page(ibuf, false, InvalidBlockNumber);
            Page ipg = BufferGetPage(ibuf);
            Size item_sz = sizeof(SmolInternalItem);
            SmolInternalItem *item = (SmolInternalItem *) palloc(item_sz);
            Size children_added = 0;

            /* Initialize aggregated stats for this internal node */
            SmolLeafStats aggregated;
            aggregated.blk = BufferGetBlockNumber(ibuf);
            aggregated.minkey = INT32_MAX;
            aggregated.maxkey = INT32_MIN;
            aggregated.row_count = 0;
            aggregated.distinct_count = 0;
            aggregated.bloom_filter = 0;
            aggregated.padding = 0;

            for (; i < cur_n; i++)
            {
                /* Build internal node entry with zone map metadata */
                item->child = cur_stats[i].blk;
                item->highkey = cur_stats[i].maxkey;

                if (zone_maps_enabled)
                {
                    item->minkey = cur_stats[i].minkey;
                    item->row_count = cur_stats[i].row_count;
                    item->distinct_count = cur_stats[i].distinct_count;
                    item->bloom_filter = cur_stats[i].bloom_filter;
                    item->padding = 0;

                    /* Aggregate statistics for parent level */
                    if (cur_stats[i].minkey < aggregated.minkey)
                        aggregated.minkey = cur_stats[i].minkey;
                    if (cur_stats[i].maxkey > aggregated.maxkey)
                        aggregated.maxkey = cur_stats[i].maxkey;
                    aggregated.row_count += cur_stats[i].row_count;
                    /* Sum distinct counts, saturate at UINT16_MAX */
                    if ((uint32)aggregated.distinct_count + (uint32)cur_stats[i].distinct_count > UINT16_MAX)
                        aggregated.distinct_count = UINT16_MAX;
                    else
                        aggregated.distinct_count += cur_stats[i].distinct_count;
                    /* OR bloom filters together */
                    aggregated.bloom_filter |= cur_stats[i].bloom_filter;
                }
                else
                {
                    /* Zone maps disabled: zero out fields */
                    item->minkey = 0;
                    item->row_count = 0;
                    item->distinct_count = 0;
                    item->bloom_filter = 0;
                    item->padding = 0;
                }

                if (PageGetFreeSpace(ipg) < item_sz + sizeof(ItemIdData))
                    break;

                OffsetNumber off = PageAddItem(ipg, (Item) item, item_sz, InvalidOffsetNumber, false, false);
                SMOL_DEFENSIVE_CHECK(off != InvalidOffsetNumber, WARNING,
                    (errmsg("smol: internal page add failed during build (with stats)")));
                if (off == InvalidOffsetNumber)
                    break; /* GCOV_EXCL_LINE - Defensive: PageAddItem should never fail during build */

                children_added++;

                /* For testing: limit fanout to force tall trees */
                if (smol_test_max_internal_fanout > 0 && children_added >= (Size) smol_test_max_internal_fanout)
                {
                    i++;
                    break;
                }
            }

            MarkBufferDirty(ibuf);

            /* Reallocation check (same as original function) */
#ifdef SMOL_TEST_COVERAGE
            if (smol_test_force_realloc_at > 0 && next_n == (Size) smol_test_force_realloc_at && cap_next == (Size) smol_test_force_realloc_at)
            {
                cap_next = cap_next * 2; /* GCOV_EXCL_LINE - Test-only: forced reallocation via GUC */
                next_stats = (SmolLeafStats *) repalloc(next_stats, cap_next * sizeof(SmolLeafStats)); /* GCOV_EXCL_LINE */
            }
            else
#endif
            if (next_n >= cap_next)
            { /* GCOV_EXCL_START - Reallocation only with extremely tall trees */
                cap_next = cap_next * 2;
                next_stats = (SmolLeafStats *) repalloc(next_stats, cap_next * sizeof(SmolLeafStats));
            } /* GCOV_EXCL_STOP */

            next_stats[next_n] = aggregated;
            next_n++;
            UnlockReleaseBuffer(ibuf);
            pfree(item);
        }

        /* Link right siblings */
        for (Size j = 1; j < next_n; j++)
        {
            Buffer pb = ReadBuffer(idx, next_stats[j-1].blk);
            LockBuffer(pb, BUFFER_LOCK_EXCLUSIVE);
            smol_page_opaque(BufferGetPage(pb))->rightlink = next_stats[j].blk;
            MarkBufferDirty(pb);
            UnlockReleaseBuffer(pb);
        }

        if (levels > 0)
            pfree(cur_stats);

        cur_stats = next_stats;
        cur_n = next_n;
        levels++;
    }

    /* Set root */
    {
        Buffer mb = ReadBuffer(idx, 0);
        LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
        SmolMeta *m = smol_meta_ptr(BufferGetPage(mb));
        m->root_blkno = cur_stats[0].blk;
        m->height = (uint16) (levels + 1);
        MarkBufferDirty(mb);
        UnlockReleaseBuffer(mb);
    }

    if (out_root) *out_root = cur_stats[0].blk;
    if (out_levels) *out_levels = levels;
    pfree(cur_stats);
}

/* GCOV_EXCL_START - Legacy function: replaced by smol_build_internal_levels_with_stats */
static void __attribute__((unused))
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
                /* TEST-ONLY: smol_test_* GUC check (compiled out in production) */
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

/* Stream-write text keys from tuplesort into leaf pages with given cap (8/16/32).
 * Stores original text (not transformed). Collation handling is done at scan time. */
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
        meta->directory_blkno = InvalidBlockNumber;
        meta->collation_oid = InvalidOid;  /* Collation stored in index descriptor, not used here */
        smol_meta_init_zone_maps(meta);
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
    }
    if (nkeys == 0)
        return;
    BlockNumber prev = InvalidBlockNumber;
    Size nleaves = 0, aleaves = 0;
    SmolLeafStats *leaf_stats = NULL;
    Oid typid = TEXTOID;
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
                /* TEST-ONLY: smol_test_* GUC check (compiled out in production) */
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

            /* Store original text (zero-padded) for all collations.
             * Tuplesort has already sorted using the correct collation comparator,
             * so the keys are in the right order. We store original text to support
             * index-only scans. During scan, we'll transform both stored keys and
             * query bounds for comparison. */
            if (blen > 0) memcpy(dest, src, blen);
            if (blen < (int) key_len) memset(dest + blen, 0, key_len - blen);

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
        /* record leaf statistics */
        if (nleaves == aleaves)
        {
            aleaves = (aleaves == 0 ? 64 : aleaves * 2);
            leaf_stats = (leaf_stats == NULL) ? (SmolLeafStats *) palloc(aleaves * sizeof(SmolLeafStats)) : (SmolLeafStats *) repalloc(leaf_stats, aleaves * sizeof(SmolLeafStats));
        }
        if (smol_build_zone_maps)
        {
            /* Collect zone map stats by re-reading the leaf page we just wrote */
            Buffer rbuf = ReadBuffer(idx, cur);
            LockBuffer(rbuf, BUFFER_LOCK_SHARE);
            Page rpage = BufferGetPage(rbuf);
            OffsetNumber roff = FirstOffsetNumber;
            ItemId iid = PageGetItemId(rpage, roff);
            Item item = PageGetItem(rpage, iid);
            /* Extract keys from the leaf payload for stats collection */
            uint16 tag = *(uint16 *) item;
            char *keys_for_stats;
            Size n_for_stats;
            if (tag == SMOL_TAG_KEY_RLE || tag == SMOL_TAG_KEY_RLE_V2)
            {
                /* Decode RLE to get all keys */
                char *p = (char *) item;
                uint16 nitems, nruns;
                p += sizeof(uint16); /* skip tag */
                memcpy(&nitems, p, sizeof(uint16)); p += sizeof(uint16);
                memcpy(&nruns, p, sizeof(uint16)); p += sizeof(uint16);
                if (tag == SMOL_TAG_KEY_RLE_V2)
                    p++; /* skip continues_byte */
                keys_for_stats = (char *) palloc(nitems * key_len);
                Size pos = 0;
                for (uint16 r = 0; r < nruns; r++)
                {
                    char *run_key = p; p += key_len;
                    uint16 cnt; memcpy(&cnt, p, sizeof(uint16)); p += sizeof(uint16);
                    for (uint16 c = 0; c < cnt; c++)
                        memcpy(keys_for_stats + (pos++ * key_len), run_key, key_len);
                }
                n_for_stats = nitems;
            }
            else
            {
                /* Plain format */
                char *p = (char *) item;
                uint16 nitems;
                memcpy(&nitems, p, sizeof(uint16)); p += sizeof(uint16);
                keys_for_stats = p;
                n_for_stats = nitems;
            }
            smol_collect_leaf_stats(&leaf_stats[nleaves], keys_for_stats, n_for_stats, key_len, typid, cur);
            if (tag == SMOL_TAG_KEY_RLE || tag == SMOL_TAG_KEY_RLE_V2)
                pfree(keys_for_stats);
            UnlockReleaseBuffer(rbuf);
        }
        else
        {
            /* Minimal stats when zone maps disabled */
            leaf_stats[nleaves].blk = cur;
            memcpy(&leaf_stats[nleaves].maxkey, lastkey, sizeof(int32));
            leaf_stats[nleaves].minkey = 0;
            leaf_stats[nleaves].row_count = 0;
            leaf_stats[nleaves].distinct_count = 0;
            leaf_stats[nleaves].bloom_filter = 0;
        }
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
        m->root_blkno = leaf_stats[0].blk;
        m->height = 1;
        MarkBufferDirty(mb);
        UnlockReleaseBuffer(mb);
        pfree(leaf_stats);
    }
    else
    {
        BlockNumber rootblk = InvalidBlockNumber; uint16 levels = 0;
        smol_build_internal_levels_with_stats(idx, leaf_stats, nleaves, key_len, &rootblk, &levels);
        pfree(leaf_stats);
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
        meta->directory_blkno = InvalidBlockNumber;
        meta->collation_oid = InvalidOid;  /* Fixed-width keys, no collation */
        smol_meta_init_zone_maps(meta);
        MarkBufferDirty(mbuf);
        UnlockReleaseBuffer(mbuf);
    }
    if (nkeys == 0)
        return;
    BlockNumber prev = InvalidBlockNumber;
    Size nleaves = 0, aleaves = 0;
    SmolLeafStats *leaf_stats = NULL;
    Oid typid = TupleDescAttr(idx->rd_att, 0)->atttypid;
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
                /* TEST-ONLY: smol_test_* GUC check (compiled out in production) */
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

            /* Would this overflow the page (RLE format)? */
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
        /* record leaf statistics */
        if (nleaves == aleaves)
        {
            aleaves = (aleaves == 0 ? 64 : aleaves * 2);
            leaf_stats = (leaf_stats == NULL) ? (SmolLeafStats *) palloc(aleaves * sizeof(SmolLeafStats)) : (SmolLeafStats *) repalloc(leaf_stats, aleaves * sizeof(SmolLeafStats));
        }
        if (smol_build_zone_maps)
        {
            /* Collect zone map stats by re-reading the leaf page we just wrote */
            Buffer rbuf = ReadBuffer(idx, cur);
            LockBuffer(rbuf, BUFFER_LOCK_SHARE);
            Page rpage = BufferGetPage(rbuf);
            OffsetNumber roff = FirstOffsetNumber;
            ItemId iid = PageGetItemId(rpage, roff);
            Item item = PageGetItem(rpage, iid);
            /* Extract keys from the leaf payload for stats collection */
            uint16 tag = *(uint16 *) item;
            char *keys_for_stats;
            Size n_for_stats;
            if (tag == SMOL_TAG_KEY_RLE_V2)
            {
                /* Decode RLE V2 to get all keys */
                char *p = (char *) item;
                uint16 nitems, nruns;
                p += sizeof(uint16); /* skip tag */
                memcpy(&nitems, p, sizeof(uint16)); p += sizeof(uint16);
                memcpy(&nruns, p, sizeof(uint16)); p += sizeof(uint16);
                p++; /* skip continues_byte */
                keys_for_stats = (char *) palloc(nitems * key_len);
                Size pos = 0;
                for (uint16 r = 0; r < nruns; r++)
                {
                    char *run_key = p; p += key_len;
                    uint16 cnt; memcpy(&cnt, p, sizeof(uint16)); p += sizeof(uint16);
                    for (uint16 c = 0; c < cnt; c++)
                        memcpy(keys_for_stats + (pos++ * key_len), run_key, key_len);
                }
                n_for_stats = nitems;
            }
            else
            {
                /* Plain format */
                char *p = (char *) item;
                uint16 nitems;
                memcpy(&nitems, p, sizeof(uint16)); p += sizeof(uint16);
                keys_for_stats = p;
                n_for_stats = nitems;
            }
            smol_collect_leaf_stats(&leaf_stats[nleaves], keys_for_stats, n_for_stats, key_len, typid, cur);
            if (tag == SMOL_TAG_KEY_RLE_V2)
                pfree(keys_for_stats);
            UnlockReleaseBuffer(rbuf);
        }
        else
        {
            /* Minimal stats when zone maps disabled */
            leaf_stats[nleaves].blk = cur;
            memcpy(&leaf_stats[nleaves].maxkey, lastkey, sizeof(int32));
            leaf_stats[nleaves].minkey = 0;
            leaf_stats[nleaves].row_count = 0;
            leaf_stats[nleaves].distinct_count = 0;
            leaf_stats[nleaves].bloom_filter = 0;
        }
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
        m->root_blkno = leaf_stats[0].blk;
        m->height = 1;
        MarkBufferDirty(mb);
        UnlockReleaseBuffer(mb);
        pfree(leaf_stats);
    }
    else
    {
        BlockNumber rootblk = InvalidBlockNumber; uint16 levels = 0;
        smol_build_internal_levels_with_stats(idx, leaf_stats, nleaves, key_len, &rootblk, &levels);
        pfree(leaf_stats);
    }
}

/* --- Test functions for coverage --- */

/*
 * smol_test_backward_scan - Test function to exercise backward scan paths
 *
 * Takes an index OID and optional lower bound, performs a backward scan
 * to force execution of the BackwardScanDirection initialization code.
 * Returns number of tuples scanned backward.
 */
PG_FUNCTION_INFO_V1(smol_test_backward_scan);
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
 *   key_rle_pages int4     - Number of pages using key RLE compression
 *   inc_rle_pages int4     - Number of pages using INCLUDE RLE compression
 *   compression_pct numeric - Percentage of leaf pages using RLE (key or include)
 */
PG_FUNCTION_INFO_V1(smol_inspect);

Datum
smol_inspect(PG_FUNCTION_ARGS)
{
    Oid         indexoid = PG_GETARG_OID(0);
    Relation    idx;
    TupleDesc   tupdesc;
    Datum       values[5];
    bool        nulls[5];
    HeapTuple   tuple;
    BlockNumber nblocks;
    BlockNumber blkno;
    int32       total_pages = 0;
    int32       leaf_pages = 0;
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
                                if (tag == SMOL_TAG_KEY_RLE || tag == SMOL_TAG_KEY_RLE_V2)
                                    key_rle_pages++;
                                else if (tag == SMOL_TAG_INC_RLE)
                                    inc_rle_pages++;
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
    values[2] = Int32GetDatum(key_rle_pages);
    values[3] = Int32GetDatum(inc_rle_pages);

    /* Calculate percentages */
    if (leaf_pages > 0)
    {
        double compression_pct = (100.0 * (key_rle_pages + inc_rle_pages)) / leaf_pages;
        values[4] = DirectFunctionCall1(float8_numeric, Float8GetDatum(compression_pct));
    }
    else
    {
        values[4] = DirectFunctionCall1(float8_numeric, Float8GetDatum(0.0));
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
 * smol_test_error_non_ios - Test function to exercise non-index-only scan error
 *
 * Calls smol_gettuple without setting xs_want_itup to trigger the
 * "smol supports index-only scans only" error in smol_gettuple.
 */
PG_FUNCTION_INFO_V1(smol_test_error_non_ios);
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
 * smol_test_no_movement - Test function to exercise NoMovementScanDirection handling
 *
 * Calls smol_gettuple with NoMovementScanDirection to verify the scan direction
 * check in smol_gettuple returns false immediately.
 */
PG_FUNCTION_INFO_V1(smol_test_no_movement);
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
            SmolTextBuildContext cb;
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
            SmolTuplesortContext cb;
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

/* ---- Table scan callbacks for build ---- */

static void
ts_build_cb_any(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    SmolTuplesortContext *c = (SmolTuplesortContext *) state;
    (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR, (errmsg("smol does not support NULL values")));
    tuplesort_putindextuplevalues(c->ts, rel, tid, values, isnull);
    (*c->pnkeys)++;
}

static void
ts_build_cb_text(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    SmolTextBuildContext *c = (SmolTextBuildContext *) state; (void) tupleIsAlive;
    if (isnull[0]) ereport(ERROR,(errmsg("smol does not support NULL values")));
    text *t = DatumGetTextPP(values[0]); int blen = VARSIZE_ANY_EXHDR(t);
    if (blen > *c->pmax) *c->pmax = blen;
    tuplesort_putindextuplevalues(c->ts, rel, tid, values, isnull);
    (*c->pnkeys)++;
}

/* 2-col builder helper */
static void
smol_build_cb_pair(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    SmolPairContext *c = (SmolPairContext *) state;
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
    if (smol_debug_log && SMOL_PROGRESS_LOG_EVERY > 0 && (*c->pcount % (Size) SMOL_PROGRESS_LOG_EVERY) == 0)
        SMOL_LOGF("collect pair: tuples=%zu", *c->pcount); /* GCOV_EXCL_LINE - debug-only logging */
}

static void
smol_build_cb_inc(Relation rel, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    SmolIncludeContext *c = (SmolIncludeContext *) state; (void) rel; (void) tid; (void) tupleIsAlive;
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
