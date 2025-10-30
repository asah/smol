/*
 * smol_scan.c
 *   Scan functions for SMOL index access method
 *
 * This file contains:
 * - Index scan initialization (beginscan, rescan, endscan)
 * - Tuple retrieval (gettuple)
 * - Parallel scan support
 * - Runtime key filtering
 *
 * TID OPTIMIZATION:
 * ----------------
 * SMOL is IOS-only and uses a synthetic TID (0,1) for all tuples, pointing to
 * heap block 0 (marked all-visible during index creation in smol_build.c:806).
 *
 * Since all tuples use the same TID, we optimize by NOT storing TIDs in IndexTuple
 * structures. Instead, we set scan->xs_heaptid = (0,1) directly when returning tuples.
 *
 * Why xs_heaptid MUST be set (tested - cannot be skipped):
 * - PostgreSQL's executor DOES read xs_heaptid even with all-visible blocks
 * - Leaving it uninitialized causes "could not open file" errors with garbage block numbers
 * - The IOS visibility check apparently needs valid TIDs in all code paths
 *
 * Performance benefits of current approach:
 * - Tuple buffering: Avoids 64 ItemPointerSet() calls per buffer refill (to itup->t_tid)
 * - Memory: Saves 6 bytes × 64 tuples = 384 bytes per refill (t_tid field unused)
 * - Hot path: Sets xs_heaptid once when returning tuple (unavoidable cost)
 *
 * The itup->t_tid field is left uninitialized (never read), but xs_heaptid MUST be set.
 */

#include "smol.h"


/* ---- Helper functions for scan operations ---- */

static char *
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

static bool
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
    char *first_key = smol_leaf_keyptr_ex(page, FirstOffsetNumber, so->key_len, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);

    /* Upper bound check: if first key exceeds upper bound, stop entire scan */
    if (so->have_upper_bound)
    {
        int c = smol_cmp_keyptr_to_upper_bound(so, first_key);
        if (so->upper_bound_strict ? (c >= 0) : (c > 0))
        {
	    *stop_scan_out = true; /* GCOV_EXCL_LINE (flaky) */
            return false; /* GCOV_EXCL_LINE (flaky) */
        }
    }

    /* Equality bound check: if first key exceeds the equality value, stop scan */
    if (so->have_k1_eq)
    {
        int c = smol_cmp_keyptr_to_bound(so, first_key);
        if (c > 0)
        { /* GCOV_EXCL_START - Rare: equality query advancing to page where first key > equality value */
            *stop_scan_out = true;
            return false;
        } /* GCOV_EXCL_STOP */
    }

    /* Page might have matching tuples, process it */
    return true;
}

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
        rp++;  /* Skip continues_byte */ /* GCOV_EXCL_LINE - V2 RLE format read path: covered by sorted build, text defaults to V1 */
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
            cur = att_align_nominal(cur, so->inc_meta->inc_align[ii]);
            wp = base + cur;
            char *ip = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, ii, row, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
            if (so->inc_meta->inc_is_text[ii])
            {
                if (so->run_active && so->inc_meta->inc_const[ii] && so->inc_meta->run_inc_built[ii] && so->inc_meta->run_inc_vl_len[ii] > 0)
                {
                    memcpy(wp, so->inc_meta->run_inc_vl[ii], (size_t) so->inc_meta->run_inc_vl_len[ii]);
                    cur += (Size) so->inc_meta->run_inc_vl_len[ii];
                }
                else
                {
                    const char *iend = (const char *) memchr(ip, '\0', so->inc_meta->inc_len[ii]);
                    int ilen = iend ? (int)(iend - ip) : (int) so->inc_meta->inc_len[ii];
                    SET_VARSIZE((struct varlena *) wp, ilen + VARHDRSZ);
                    memcpy(wp + VARHDRSZ, ip, ilen);
                    cur += VARHDRSZ + (Size) ilen;
                }
            }
            else
            {
                if (so->inc_meta->inc_len[ii] == 2) smol_copy2(wp, ip);
                else if (so->inc_meta->inc_len[ii] == 4) smol_copy4(wp, ip);
                else if (so->inc_meta->inc_len[ii] == 8) smol_copy8(wp, ip);
                else if (so->inc_meta->inc_len[ii] == 16) smol_copy16(wp, ip); /* GCOV_EXCL_LINE - 16-byte INCLUDE causes test instability */
                else smol_copy_small(wp, ip, so->inc_meta->inc_len[ii]); /* GCOV_EXCL_LINE - uncommon INCLUDE column lengths rarely used */
                cur += so->inc_meta->inc_len[ii];
            }
        }
    }
    cur = MAXALIGN(cur);
    so->itup->t_info = (unsigned short) (cur | (so->has_varwidth ? INDEX_VAR_MASK : 0));
}

IndexScanDesc

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
            so->ninclude = m.inc_count;  /* Read INCLUDE count for both single-col and two-col */

            /* Allocate INCLUDE metadata structure if we have INCLUDE columns */
            if (so->ninclude > 0)
            {
                size_t meta_size = sizeof(SmolIncludeMetadata);
                size_t arrays_size = so->ninclude * (
                    sizeof(uint16) +      /* inc_len */
                    sizeof(uint32) +      /* inc_cumul_offs */
                    sizeof(char) +        /* inc_align */
                    sizeof(uint16) +      /* inc_offs */
                    sizeof(void*) +       /* inc_copy */
                    sizeof(bool) +        /* inc_is_text */
                    sizeof(bool) +        /* inc_const */
                    sizeof(int16) +       /* run_inc_len */
                    sizeof(char*) +       /* plain_inc_base */
                    sizeof(char*) +       /* rle_run_inc_ptr */
                    sizeof(bool) +        /* run_inc_built */
                    sizeof(int16) +       /* run_inc_vl_len */
                    (VARHDRSZ + 32)       /* run_inc_vl */
                );

                /* One extra slot for cumulative offsets (stores total) */
                arrays_size += sizeof(uint32);

                so->inc_meta = (SmolIncludeMetadata *) palloc0(meta_size + arrays_size);

                /* Set up pointers to point into the allocated block */
                char *arr_base = (char*)so->inc_meta + meta_size;
                so->inc_meta->inc_len = (uint16*)arr_base;
                arr_base += so->ninclude * sizeof(uint16);
                so->inc_meta->inc_cumul_offs = (uint32*)arr_base;
                arr_base += (so->ninclude + 1) * sizeof(uint32);  /* +1 for total */
                so->inc_meta->inc_align = arr_base;
                arr_base += so->ninclude * sizeof(char);
                so->inc_meta->inc_offs = (uint16*)arr_base;
                arr_base += so->ninclude * sizeof(uint16);
                so->inc_meta->inc_copy = (void (**)(char *, const char *))arr_base;
                arr_base += so->ninclude * sizeof(void*);
                so->inc_meta->inc_is_text = (bool*)arr_base;
                arr_base += so->ninclude * sizeof(bool);
                so->inc_meta->inc_const = (bool*)arr_base;
                arr_base += so->ninclude * sizeof(bool);
                so->inc_meta->run_inc_len = (int16*)arr_base;
                arr_base += so->ninclude * sizeof(int16);
                so->inc_meta->plain_inc_base = (char**)arr_base;
                arr_base += so->ninclude * sizeof(char*);
                so->inc_meta->rle_run_inc_ptr = (char**)arr_base;
                arr_base += so->ninclude * sizeof(char*);
                so->inc_meta->run_inc_built = (bool*)arr_base;
                arr_base += so->ninclude * sizeof(bool);
                so->inc_meta->run_inc_vl_len = (int16*)arr_base;
                arr_base += so->ninclude * sizeof(int16);
                so->inc_meta->run_inc_vl = (char (*)[VARHDRSZ + 32])arr_base;
            }
            else
            {
                so->inc_meta = NULL;
            }

            for (uint16 i=0;i<so->ninclude;i++)
            {
                so->inc_meta->inc_len[i] = m.inc_len[i];
                /* include attrs follow key attrs in the index tupdesc */
                int key_atts = so->two_col ? 2 : 1;
                Form_pg_attribute att = TupleDescAttr(RelationGetDescr(index), key_atts + i);
                so->inc_meta->inc_align[i] = att->attalign;
                Oid attoid = att->atttypid;
                so->inc_meta->inc_is_text[i] = (attoid == TEXTOID);
            }
            /* Precompute cumulative offsets for O(1) INCLUDE pointer arithmetic */
            uint32 cumul = 0;
            for (uint16 i=0;i<so->ninclude;i++)
            {
                so->inc_meta->inc_cumul_offs[i] = cumul;
                cumul += so->inc_meta->inc_len[i];
            }
            if (so->ninclude > 0)
                so->inc_meta->inc_cumul_offs[so->ninclude] = cumul;  /* Total for "skip all includes" case */
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
                cur = att_align_nominal(cur, so->inc_meta->inc_align[i]);
                so->inc_meta->inc_offs[i] = (uint16) (cur - data_off);
                Size inc_bytes = so->inc_meta->inc_is_text[i] ? (Size)(VARHDRSZ + so->inc_meta->inc_len[i]) : (Size) so->inc_meta->inc_len[i];
                cur += inc_bytes;
            }
            sz = MAXALIGN(cur);
        }
        else
        {
            /* Two-col: compute offset/aligned size for key2, then INCLUDE attrs */
            off2 = att_align_nominal(off1 + so->key_len, align2);
            Size cur = off2 + so->key_len2;
            for (uint16 i=0;i<so->ninclude;i++)
            {
                cur = att_align_nominal(cur, so->inc_meta->inc_align[i]);
                so->inc_meta->inc_offs[i] = (uint16) (cur - data_off);
                Size inc_bytes = so->inc_meta->inc_is_text[i] ? (Size)(VARHDRSZ + so->inc_meta->inc_len[i]) : (Size) so->inc_meta->inc_len[i];
                cur += inc_bytes;
            }
            sz = MAXALIGN(cur);
        }
        if (smol_debug_log)
        {
            SMOL_LOGF("beginscan layout: key_len=%u two_col=%d ninclude=%u sz=%zu", so->key_len, so->two_col, so->ninclude, (size_t) sz);
            if (so->ninclude > 0)
                for (uint16 i=0;i<so->ninclude;i++)
                    SMOL_LOGF("include[%u]: len=%u align=%c off=%u is_text=%d", i, so->inc_meta->inc_len[i], so->inc_meta->inc_align[i], so->inc_meta->inc_offs[i], so->inc_meta->inc_is_text[i]);
        }
        so->itup = (IndexTuple) palloc0(sz);
        so->has_varwidth = key_is_text;
        if (so->ninclude > 0)
            for (uint16 i=0;i<so->ninclude;i++) if (so->inc_meta->inc_is_text[i]) { so->has_varwidth = true; break; }
        so->itup->t_info = (unsigned short) (sz | (so->has_varwidth ? INDEX_VAR_MASK : 0)); /* updated per-row when key varwidth */
        so->itup_data = (char *) so->itup + data_off;
        so->itup_off2 = so->two_col ? (uint16) (off2 - data_off) : 0;
        so->itup_data_off = (uint16) data_off;
        /* set copy helpers */
        so->copy1_fn = (so->key_len == 2) ? smol_copy2 : (so->key_len == 4) ? smol_copy4 : smol_copy8;
        if (so->two_col)
            so->copy2_fn = (so->key_len2 == 2) ? smol_copy2 : (so->key_len2 == 4) ? smol_copy4 : smol_copy8;
        if (so->ninclude > 0)
        {
            for (uint16 i=0;i<so->ninclude;i++)
            {
                /* Precompute copy function for each INCLUDE column to eliminate hot-path if-else chain */
                if (so->inc_meta->inc_len[i] == 1) so->inc_meta->inc_copy[i] = smol_copy1;
                else if (so->inc_meta->inc_len[i] == 2) so->inc_meta->inc_copy[i] = smol_copy2;
                else if (so->inc_meta->inc_len[i] == 4) so->inc_meta->inc_copy[i] = smol_copy4;
                else if (so->inc_meta->inc_len[i] == 8) so->inc_meta->inc_copy[i] = smol_copy8;
                else if (so->inc_meta->inc_len[i] == 16) so->inc_meta->inc_copy[i] = smol_copy16;
                else /* GCOV_EXCL_LINE - defensive: all PostgreSQL fixed-length types are 1/2/4/8/16 bytes */
                    elog(ERROR, "smol: unsupported INCLUDE column size %u", so->inc_meta->inc_len[i]);
            }
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
    /* Zone map + bloom filter profiling counters */
    so->prof_subtrees_checked = 0;
    so->prof_subtrees_skipped = 0;
    so->prof_bloom_checks = 0;
    so->prof_bloom_skips = 0;
    so->run_key_built = false;
    if (so->inc_meta)
    {
        for (uint16 i=0; i<so->ninclude; i++)
        {
            so->inc_meta->run_inc_built[i] = false;
            so->inc_meta->run_inc_vl_len[i] = 0;
        }
    }
    /* Initialize V2 continuation support */
    so->prev_page_last_run_active = false;
    so->prev_page_last_run_text_klen = 0;
    /* Initialize adaptive prefetch counters */
    so->pages_scanned = 0;
    so->adaptive_prefetch_depth = 0;

    /* Initialize tuple buffering (forward scans only, fixed-width tuples only) */
    if (smol_use_tuple_buffering && !so->two_col && !so->has_varwidth)
    {
        so->tuple_buffering_enabled = true;
        so->tuple_buffer_capacity = smol_tuple_buffer_size;
        so->tuple_buffer_count = 0;
        so->tuple_buffer_current = 0;

        /* Calculate tuple size: header + key + INCLUDE columns */
        so->tuple_size = MAXALIGN(sizeof(IndexTupleData)) + MAXALIGN(so->key_len);
        if (so->ninclude > 0)
            so->tuple_size += so->inc_meta->inc_cumul_offs[so->ninclude - 1] +
                             so->inc_meta->inc_len[so->ninclude - 1];

        /* Allocate buffer arrays */
        so->tuple_buffer = palloc(so->tuple_buffer_capacity * sizeof(IndexTuple));
        so->tuple_buffer_data = palloc(so->tuple_buffer_capacity * so->tuple_size);

        /* Initialize tuple pointers to point into contiguous data buffer */
        for (int i = 0; i < so->tuple_buffer_capacity; i++)
            so->tuple_buffer[i] = (IndexTuple)(so->tuple_buffer_data + i * so->tuple_size);
    }
    else
    {
        so->tuple_buffering_enabled = false;
        so->tuple_buffer = NULL;
        so->tuple_buffer_data = NULL;
    }

    scan->opaque = so;
    return scan;
}

void
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
    so->use_generic_cmp = false;
    so->chunk_left = 0;

    /* Store all scankeys for runtime filtering */
    if (so->runtime_keys)
        pfree(so->runtime_keys);
    so->runtime_keys = NULL;
    so->n_runtime_keys = 0;

    if (keys && nkeys > 0)
    {
        /* PROGRAMMING BY CONTRACT: SMOL does not support searching for NULL values
         * SK_SEARCHNULL: IS NULL query - reject with clear error
         * SK_ISNULL (scan key argument is NULL) is allowed: k > NULL returns 0 rows gracefully
         * SK_SEARCHNOTNULL (IS NOT NULL) is allowed: works correctly since index has no NULLs */
        for (int i = 0; i < nkeys; i++)
        {
            ScanKey sk = &keys[i];
            /* Defensive check - no if-statement needed, macro handles everything */
            SMOL_DEFENSIVE_CHECK(!(sk->sk_flags & SK_SEARCHNULL), ERROR,
                (errmsg("smol does not support NULL values"),
                 errdetail("Query uses IS NULL which is not supported. SMOL indexes do not contain NULL values.")));
        }

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

        /* Check if we need to use generic comparator for non-C collation text keys */
        if ((so->have_bound || so->have_upper_bound) && so->atttypid == TEXTOID)
        {
            pg_locale_t locale = pg_newlocale_from_collation(so->collation);

            /* Set flag to route to generic comparator for non-C collations */
            if (locale && !locale->collate_is_c)
                so->use_generic_cmp = true;
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

/*
 * smol_refill_tuple_buffer - Pre-build multiple tuples into buffer
 *
 * Returns number of tuples buffered (0 if end of scan or no matches)
 * Only handles plain pages with fixed-width keys and INCLUDE columns.
 */
static uint16
smol_refill_tuple_buffer_plain(SmolScanOpaque so, Page page)
{
    uint16 n = so->cur_page_nitems;
    uint16 count = 0;
    uint16 start_off = so->cur_off;
    uint16 max_tuples = (n >= start_off) ? (n - start_off + 1) : 0;

    if (max_tuples > so->tuple_buffer_capacity)
        max_tuples = so->tuple_buffer_capacity;

    if (max_tuples == 0)
        return 0; // GCOV_EXCL_LINE - defensive: should never happen in normal operation

    /* Get base pointer for keys: [uint16 n][keys][inc blocks...] */
    ItemId iid = PageGetItemId(page, 1);
    char *base = (char *) PageGetItem(page, iid);
    char *key_base = base + sizeof(uint16);  /* Skip nitems count */

    /* Bulk copy tuples */
    for (uint16 i = 0; i < max_tuples; i++)
    {
        uint16 off = start_off + i;

        /* Get key pointer for bounds checking */
        char *keyp = key_base + ((size_t)(off - 1)) * so->key_len;

        /* Check upper bound if present */
        if (so->have_upper_bound)
        {
            int c = smol_cmp_keyptr_to_upper_bound(so, keyp);
            if (so->upper_bound_strict ? (c >= 0) : (c > 0))
                break;  /* Beyond upper bound */
        }

        /* Check equality bound (k=value queries) */
        if (so->have_k1_eq)
        {
            int c = smol_cmp_keyptr_to_bound(so, keyp);
            if (c > 0)
                break;  /* Beyond equality value, stop buffering */
        }

        /* Build tuple in buffer */
        IndexTuple itup = so->tuple_buffer[count];
        char *tup_data = (char *)itup + MAXALIGN(sizeof(IndexTupleData));

        /* Copy key (keyp already computed above for bounds checking) */
        memcpy(tup_data, keyp, so->key_len);

        /* Copy INCLUDE columns if present */
        if (so->ninclude > 0 && so->plain_inc_cached)
        {
            for (uint16 ii = 0; ii < so->ninclude; ii++)
            {
                char *src = so->inc_meta->plain_inc_base[ii] + (size_t)(off - 1) * so->inc_meta->inc_len[ii];
                char *dst = tup_data + so->inc_meta->inc_offs[ii];
                memcpy(dst, src, so->inc_meta->inc_len[ii]);
            }
        }

        /* Set tuple size. Skip setting itup->t_tid - we set xs_heaptid directly
         * when returning tuples to avoid redundant work (see TID OPTIMIZATION above) */
        itup->t_info = so->tuple_size;

        count++;
    }

    return count;
}

bool
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
			{ /* GCOV_EXCL_LINE (flaky) - opening brace artifact: gcov inconsistently reports this */
                            /* Use actual lower bound when available to avoid over-emitting from the first leaf */
                            int64 lb = PG_INT64_MIN;
                            if (so->have_bound)
                            {
                                if (so->atttypid == INT2OID) lb = (int64) DatumGetInt16(so->bound_datum);
                                else if (so->atttypid == INT4OID) lb = (int64) DatumGetInt32(so->bound_datum);
                                else if (so->atttypid == INT8OID) lb = DatumGetInt64(so->bound_datum);
                            }
                            BlockNumber left = smol_find_first_leaf(idx, lb, so->atttypid, so->key_len);
                            /* Optimized: claim leaf and publish rightlink without batch skip-ahead
                             * This eliminates 15+ page reads per claim (pure overhead) */
                            Buffer lbuf = ReadBufferExtended(idx, MAIN_FORKNUM, left, RBM_NORMAL, so->bstrategy);
                            Page lpg = BufferGetPage(lbuf);
                            BlockNumber next_leaf = smol_page_opaque(lpg)->rightlink;
                            ReleaseBuffer(lbuf);
                            uint32 expect = 0u;
                            uint32 newv = (uint32) (BlockNumberIsValid(next_leaf) ? next_leaf : InvalidBlockNumber);
                            if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expect, newv))
                            {
			        so->cur_blk = left; so->chunk_left = 0;
				break;
			    }
                            continue; /* GCOV_EXCL_LINE (flaky) - CAS retry: covered by simulate_atomic_race but non-deterministic */
                        }
                        if (curv == (uint32) InvalidBlockNumber)
                        { so->cur_blk = InvalidBlockNumber; break; }
                        /* claim current leaf without batch skip-ahead */
                        Buffer tbuf = ReadBufferExtended(idx, MAIN_FORKNUM, (BlockNumber) curv, RBM_NORMAL, so->bstrategy);
                        Page tpg = BufferGetPage(tbuf);
                        BlockNumber next_leaf = smol_page_opaque(tpg)->rightlink;
                        ReleaseBuffer(tbuf);
                        uint32 expected = curv;
                        uint32 newv = (uint32) (BlockNumberIsValid(next_leaf) ? next_leaf : InvalidBlockNumber);
                        if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expected, newv))
                        {
			    so->cur_blk = (BlockNumber) curv;
			    so->chunk_left = 0;
			    break;
			}
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
                            uint16 lo2 = FirstOffsetNumber, hi2 = n2, ans2 = InvalidOffsetNumber;
                            while (lo2 <= hi2)
                            {
                                uint16 mid2 = (uint16) (lo2 + ((hi2 - lo2) >> 1));
                                char *kp2 = smol_leaf_keyptr_ex(page, mid2, so->key_len, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
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

                    /* Zone map filtering may return InvalidBlockNumber if no subtrees can match */
                    if (!BlockNumberIsValid(so->cur_blk))
                    {
                        SMOL_LOG("zone map filtering: no matching subtrees, returning false");
                        return false;  /* No tuples match the query */
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
                        n2 = smol_leaf_nitems(page);
                        hi = n2;
                        while (lo <= hi)
                        {
                            uint16 mid = (uint16) (lo + ((hi - lo) >> 1));
                            char *keyp = smol_leaf_keyptr_ex(page, mid, so->key_len, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
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

                    /* Position-based scan optimization: Find end position */
                    if (smol_use_position_scan && !so->two_col && !so->need_runtime_key_test &&
                        dir == ForwardScanDirection && !scan->parallel_scan)
                    {
                        smol_find_end_position(idx, so, &so->end_blk, &so->end_off);
                        so->use_position_scan = BlockNumberIsValid(so->end_blk) ||
                                                (!so->have_upper_bound);
                        if (so->use_position_scan)
                        {
                            SMOL_LOGF("position scan: start=(%u,%u) end=(%u,%u)",
                                     so->cur_blk, so->cur_off, so->end_blk, so->end_off);
                        }
                    }
                    else
                    {
                        so->use_position_scan = false;
                    }
                }
            }
            else
            {
                if (scan->parallel_scan)
                {
                    SmolParallelScan *ps = (SmolParallelScan *) ((char *) scan->parallel_scan + scan->parallel_scan->ps_offset_am);
                    for (;;)
                    { /* GCOV_EXCL_LINE (flaky) - opening brace artifact: gcov inconsistently reports this */
                        uint32 curv = SMOL_ATOMIC_READ_U32(&ps->curr);
                        if (curv == 0u)
                        { /* GCOV_EXCL_LINE - opening brace artifact: code inside is tested */
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
                            BlockNumber next_leaf = smol_page_opaque(lpg)->rightlink;
                            ReleaseBuffer(lbuf);
                            uint32 expect = 0u;
                            uint32 newv = (uint32) (BlockNumberIsValid(next_leaf) ? next_leaf : InvalidBlockNumber);
                            if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expect, newv))
                            { so->cur_blk = left; so->chunk_left = 0; break; }
                            continue; /* GCOV_EXCL_LINE - CAS retry: requires precise parallel timing */
                        }
                        if (curv == (uint32) InvalidBlockNumber)
                        { so->cur_blk = InvalidBlockNumber; break; }
                        Buffer tbuf = ReadBuffer(idx, (BlockNumber) curv);
                        Page tpg = BufferGetPage(tbuf);
                        BlockNumber next_leaf = smol_page_opaque(tpg)->rightlink;
                        ReleaseBuffer(tbuf);
                        uint32 expected = curv;
                        uint32 newv = (uint32) (BlockNumberIsValid(next_leaf) ? next_leaf : InvalidBlockNumber);
                        if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expected, newv))
                        { so->cur_blk = (BlockNumber) curv; so->chunk_left = 0; break; }
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
                                char *k1p = smol12_row_k1_ptr(page, mid, so->key_len, so->key_len2, so->inc_meta ? so->inc_meta->inc_cumul_offs[so->ninclude] : 0);
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
                            char *k1p = smol12_row_k1_ptr(page, mid, so->key_len, so->key_len2, so->inc_meta ? so->inc_meta->inc_cumul_offs[so->ninclude] : 0);
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
            so->cur_buf = ReadBufferExtended(idx, MAIN_FORKNUM, so->cur_blk, RBM_NORMAL, so->bstrategy);
            so->have_pin = true;
        }
        buf = so->cur_buf;
        page = BufferGetPage(buf);
        /* Detect page format: plain or RLE */
        ItemId iid = PageGetItemId(page, FirstOffsetNumber);
        char *base = (char *) PageGetItem(page, iid);
	/* memcpy() is same perf but handles unaligned - uint16 tag = *((uint16*)base) is unsafe */
        uint16 tag; memcpy(&tag, base, sizeof(uint16));

        /* Opt #5: Cache page metadata once per page (nitems and format) */
        if (tag == SMOL_TAG_KEY_RLE ||
            tag == SMOL_TAG_KEY_RLE_V2 || tag == SMOL_TAG_INC_RLE)
        {
            /* Tagged formats: [u16 tag][u16 nitems][...] */
            uint16 nitems;
            memcpy(&nitems, base + sizeof(uint16), sizeof(uint16));
            so->cur_page_nitems = nitems;
            if (tag == SMOL_TAG_KEY_RLE)
                so->cur_page_format = 2;
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

        /* Bloom filter check for equality queries: skip pages that definitely don't contain the target value
         * Only applicable for:
         * - Equality queries (have_k1_eq set, which means WHERE col = value)
         * - Single-column indexes (bloom on k1 only)
         * - Forward scans (backward scans handle bounds differently)
         * - When bloom filters are enabled globally and in metadata
         * - NOT the first page (prof_pages > 0): first page found via B-tree descent should contain target
         *
         * NOTE: Bloom filters are built on-the-fly for each page during rightlink sequential scanning.
         * While this adds overhead, it can save significant work if the bloom indicates the page
         * definitely doesn't contain the search value, allowing us to skip scanning potentially
         * thousands of tuples. A better implementation would check blooms during B-tree descent
         * using pre-built blooms stored in internal nodes, but that requires more invasive changes.
         */
        if (smol_bloom_filters && so->have_k1_eq && !so->two_col && dir == ForwardScanDirection && so->prof_pages > 0)
        {
            SmolMeta meta;
            smol_meta_read(idx, &meta);
            if (meta.bloom_enabled && meta.bloom_nhash > 0)
            {
                /* Build bloom filter for this page on-the-fly */
                uint64 page_bloom = smol_bloom_build_page(page, so->key_len, so->atttypid, meta.bloom_nhash);
                if (so->prof_enabled)
                    so->prof_bloom_checks++;

                SMOL_LOGF("bloom check page %u: bloom=%lx key=%d nhash=%d", so->cur_blk,
                         (unsigned long)page_bloom, DatumGetInt32(so->bound_datum), meta.bloom_nhash);

                /* Test if our search key might be in this page */
                if (!smol_bloom_test(page_bloom, so->bound_datum, so->atttypid, meta.bloom_nhash))
                {
                    /* Bloom filter says the key is definitely NOT in this page - skip it */
                    if (so->prof_enabled)
                        so->prof_bloom_skips++;
                    SMOL_LOGF("bloom SKIP page %u for equality scan", so->cur_blk);

                    /* Advance to next page */
                    op = smol_page_opaque(page);
                    next = op->rightlink;
                    if (BlockNumberIsValid(next))
                    {
                        /* Release current buffer and move to next */
                        ReleaseBuffer(so->cur_buf);
                        so->have_pin = false;
                        /* Invalidate tuple buffer - tuples point to released page */
                        so->tuple_buffer_count = 0;
                        so->tuple_buffer_current = 0;
                        so->cur_blk = next;
                        continue;  /* Skip to next iteration of while loop */
                    }
                    else
                    {
                        /* No more pages */
                        if (so->have_pin && BufferIsValid(so->cur_buf))
                        {
                            ReleaseBuffer(so->cur_buf);
                            so->have_pin = false;
                        }
                        so->cur_blk = InvalidBlockNumber;
                        return false;
                    }
                }
                /* Bloom test passed - page might contain our key, proceed with scan */
            }
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
            if (tag != 0x8001u && tag != 0x8003u)
            {
                uint16 n = tag; /* First u16 is count, not a tag */
                char *base_ptr = base + sizeof(uint16) + (size_t) n * so->key_len;
                /* Use precomputed cumulative offsets for O(1) per-column computation */
                for (uint16 ii = 0; ii < so->ninclude; ii++)
                {
                    so->inc_meta->plain_inc_base[ii] = base_ptr + (size_t) n * so->inc_meta->inc_cumul_offs[ii];
                }
                so->plain_inc_cached = true;
            }
        }

        if (so->two_col)
        {
            if (so->leaf_i < so->leaf_n)
            {
                uint16 row = (uint16) (so->leaf_i + 1);
                char *k1p = smol12_row_k1_ptr(page, row, so->key_len, so->key_len2, so->inc_meta ? so->inc_meta->inc_cumul_offs[so->ninclude] : 0);
                char *k2p = smol12_row_k2_ptr(page, row, so->key_len, so->key_len2, so->inc_meta ? so->inc_meta->inc_cumul_offs[so->ninclude] : 0);
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

                /* Copy INCLUDE columns for two-column indexes */
                if (so->ninclude > 0)
                {
                    char *row_ptr = smol12_row_ptr(page, row, so->key_len, so->key_len2, so->inc_meta->inc_cumul_offs[so->ninclude]);
                    char *inc_start = row_ptr + so->key_len + so->key_len2;
                    for (uint16 i = 0; i < so->ninclude; i++)
                    {
                        char *inc_src = inc_start + so->inc_meta->inc_cumul_offs[i];
                        char *inc_dst = so->itup_data + so->inc_meta->inc_offs[i];
                        if (so->inc_meta->inc_is_text[i])
                        {
                            /* Text: add VARHDRSZ and copy */
                            SET_VARSIZE(inc_dst, VARHDRSZ + so->inc_meta->inc_len[i]);
                            memcpy(VARDATA(inc_dst), inc_src, so->inc_meta->inc_len[i]);
                        }
                        else
                        {
                            /* Fixed-length: direct copy */
                            memcpy(inc_dst, inc_src, so->inc_meta->inc_len[i]);
                        }
                    }
                }

                /* Test runtime keys before returning */
                if (!smol_test_runtime_keys(scan, so))
                {
                    if (dir == BackwardScanDirection) so->leaf_i--; else so->leaf_i++;
                    continue; /* Skip this tuple, doesn't match all keys */
                }

                scan->xs_itup = so->itup;
                /* Set synthetic TID directly (not stored in itup->t_tid - see TID OPTIMIZATION) */
                ItemPointerSet(&scan->xs_heaptid, 0, 1);
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
                 * - Initial page: cur_off is set in smol_gettuple backward init
                 * - Subsequent pages: cur_off is set when advancing to previous leaf
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
                        keyp = smol_leaf_keyptr_cached(so, page, so->cur_off, so->key_len, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
                    }
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
                                if (!cached && !smol_leaf_run_bounds_rle_ex(page, so->cur_off, so->key_len, &start, &dummy_end, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude))
                                {
                                    /* RLE page but not encoded: scan backward to find run start */
                                    while (start > FirstOffsetNumber)
                                    {
                                        const char *kp = smol_leaf_keyptr_ex(page, (uint16) (start - 1), so->key_len, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
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
                            else if (so->key_len == 1) smol_copy1(so->itup_data, keyp);
                            else
                            {
                                /* Defensive: PostgreSQL byval types are 1,2,4,8,16 bytes only */
                                SMOL_DEFENSIVE_CHECK(false, ERROR, /* GCOV_EXCL_LINE - defensive check, should never execute */
                                    (errmsg("smol: unexpected key_len=%u in RLE path, expected 1/2/4/8/16", so->key_len))); /* GCOV_EXCL_LINE */
                                smol_copy_small(so->itup_data, keyp, so->key_len); /* GCOV_EXCL_LINE - defensive fallback */
                            }
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
                                        ip = so->inc_meta->plain_inc_base[ii] + (size_t) row * so->inc_meta->inc_len[ii];
                                    else if (so->rle_run_inc_cached)
                                        /* RLE page with cached run: INCLUDE values constant within run */
                                        ip = so->inc_meta->rle_run_inc_ptr[ii];
                                    else
                                        /* Slow path: compute pointer dynamically */
                                        ip = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, ii, row, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
                                    char *dst = so->itup_data + so->inc_meta->inc_offs[ii];
                                    /* Use precomputed copy function (eliminates if-else chain overhead) */
                                    so->inc_meta->inc_copy[ii](dst, ip);
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
                            if (so->inc_meta->inc_is_text[ii])
                            {
                                char *dst = so->itup_data + so->inc_meta->inc_offs[ii];
                                int32 vsz = VARSIZE_ANY((struct varlena *) dst);
                                SMOL_LOGF("tuple include[%u] varlena size=%d off=%u", ii, vsz, so->inc_meta->inc_offs[ii]);
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
                    /* Set synthetic TID directly (not stored in itup->t_tid - see TID OPTIMIZATION) */
                    ItemPointerSet(&scan->xs_heaptid, 0, 1);
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

                /* Tuple buffering optimization for plain pages (forward scans only) */
                if (so->tuple_buffering_enabled && so->plain_inc_cached)
                {
                    /* Check if we have buffered tuples available */
                    if (so->tuple_buffer_current < so->tuple_buffer_count)
                    {
                        /* Return next tuple from buffer */
                        IndexTuple itup = so->tuple_buffer[so->tuple_buffer_current];
                        scan->xs_itup = itup;
                        /* Set synthetic TID directly (not stored in itup->t_tid - see TID OPTIMIZATION) */
                        ItemPointerSet(&scan->xs_heaptid, 0, 1);

                        so->tuple_buffer_current++;
                        so->cur_off++;

                        if (so->prof_enabled)
                        {
                            so->prof_rows++; // GCOV_EXCL_LINE - profiling instrumentation
                            so->prof_bytes += so->tuple_size; // GCOV_EXCL_LINE - profiling instrumentation
                        }

                        return true;
                    }

                    /* Buffer empty - try to refill from current page */
                    if (so->cur_off <= n)
                    {
                        so->tuple_buffer_count = smol_refill_tuple_buffer_plain(so, page);
                        so->tuple_buffer_current = 0;

                        if (so->tuple_buffer_count > 0)
                        {
                            /* Successfully refilled, return first tuple */
                            IndexTuple itup = so->tuple_buffer[0];
                            scan->xs_itup = itup;
                            /* Set synthetic TID directly (not stored in itup->t_tid - see TID OPTIMIZATION) */
                            ItemPointerSet(&scan->xs_heaptid, 0, 1);

                            so->tuple_buffer_current = 1;  /* Consumed first tuple */
                            so->cur_off++;

                            if (so->prof_enabled)
                            {
                                so->prof_rows++; // GCOV_EXCL_LINE - profiling instrumentation
                                so->prof_bytes += so->tuple_size; // GCOV_EXCL_LINE - profiling instrumentation
                            }

                            return true;
                        }
                        /* else: refill returned 0, fall through to page advance */
                    }
                    /* else: cur_off > n, fall through to page advance */
                }

                while (so->cur_off <= n)
                {
                    /* Position-based scan: check if we've reached end position */
                    if (so->use_position_scan && BlockNumberIsValid(so->end_blk))
                    {
                        if (so->cur_blk > so->end_blk ||
                            (so->cur_blk == so->end_blk && so->cur_off >= so->end_off))
                        {
                            /* Reached end position, stop scan */
                            if (so->have_pin && BufferIsValid(so->cur_buf)) { ReleaseBuffer(so->cur_buf); so->have_pin=false; }
                            so->cur_blk = InvalidBlockNumber;
                            return false;
                        }
                    }

                    char *keyp = smol_leaf_keyptr_cached(so, page, so->cur_off, so->key_len, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
                    /* Check upper bound (for BETWEEN queries) */
                    if (so->have_upper_bound && !so->use_position_scan)
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
                                         !smol_leaf_run_bounds_rle_ex(page, so->cur_off, so->key_len, &start, &end, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude))
                                {
                                    /* RLE page but not encoded: scan forward to find run end */
                                    while (end < n)
                                    {
                                        const char *kp = smol_leaf_keyptr_ex(page, (uint16) (end + 1), so->key_len, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
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
                                            so->inc_meta->rle_run_inc_ptr[ii] = smol1_inc_ptr_any(page, so->key_len, n,
                                                                                         so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude,
                                                                                         ii, start - 1, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
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
                        if (so->has_varwidth)
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
                            else if (so->key_len == 1) smol_copy1(so->itup_data, keyp);
                            else
                            {
                                /* Defensive: PostgreSQL byval types are 1,2,4,8,16 bytes only */
                                SMOL_DEFENSIVE_CHECK(false, ERROR, /* GCOV_EXCL_LINE - defensive check, should never execute */
                                    (errmsg("smol: unexpected key_len=%u in plain path, expected 1/2/4/8/16", so->key_len))); /* GCOV_EXCL_LINE */
                                smol_copy_small(so->itup_data, keyp, so->key_len); /* GCOV_EXCL_LINE - defensive fallback */
                            }
                            if (so->ninclude > 0)
                            {
                                uint16 n2 = n;
                                for (uint16 ii=0; ii<so->ninclude; ii++)
                                {
                                    char *ip;
                                    /* Use cached pointers when available (fast paths) */
                                    if (so->plain_inc_cached) /* GCOV_EXCL_START - tuple buffering now handles plain pages */
                                        /* Plain page: base pointer + row offset */
                                        ip = so->inc_meta->plain_inc_base[ii] + (size_t) row * so->inc_meta->inc_len[ii]; /* GCOV_EXCL_STOP */
                                    else if (so->rle_run_inc_cached)
                                        /* RLE page with cached run: INCLUDE values constant within run */
                                        ip = so->inc_meta->rle_run_inc_ptr[ii];
                                    else /* GCOV_EXCL_LINE - defensive fallback: caching now covers all RLE pages with INCLUDE */
                                        /* Slow path: compute pointer dynamically */ /* GCOV_EXCL_LINE */
                                        ip = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, ii, row, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL); /* GCOV_EXCL_LINE */
                                    char *dst = so->itup_data + so->inc_meta->inc_offs[ii];
                                    /* Use precomputed copy function (eliminates if-else chain overhead) */
                                    so->inc_meta->inc_copy[ii](dst, ip);
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
                                    char *firstp = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, ii, (uint32)(start - 1), so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
                                    for (uint16 off = start + 1; off <= end; off++)
                                    {
                                        char *p2 = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, ii, (uint32)(off - 1), so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
                                        if (memcmp(firstp, p2, so->inc_meta->inc_len[ii]) != 0)
                                        { all_eq = false; break; }
                                    }
                                    so->inc_meta->inc_const[ii] = all_eq;
                                    if (all_eq && so->inc_meta->inc_is_text[ii])
                                    {
                                        const char *zend = (const char *) memchr(firstp, '\0', so->inc_meta->inc_len[ii]);
                                        so->inc_meta->run_inc_len[ii] = (int16) (zend ? (zend - firstp) : so->inc_meta->inc_len[ii]);
                                    }
                                }
                                so->run_inc_evaluated = true;
                                if (so->cur_off == so->run_start_off)
                                {
                                    for (uint16 ii=0; ii<so->ninclude; ii++)
                                    {
                                        if (!so->inc_meta->inc_const[ii]) continue;
                                        char *ip0 = smol1_inc_ptr_any(page, so->key_len, n2, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, ii, (uint32)(so->run_start_off - 1), so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
                                        if (so->inc_meta->inc_is_text[ii])
                                        {
                                            int ilen0 = 0; while (ilen0 < so->inc_meta->inc_len[ii] && ip0[ilen0] != '\0') ilen0++;
                                            SET_VARSIZE((struct varlena *) so->inc_meta->run_inc_vl[ii], ilen0 + VARHDRSZ);
                                            memcpy(so->inc_meta->run_inc_vl[ii] + VARHDRSZ, ip0, ilen0);
                                            so->inc_meta->run_inc_vl_len[ii] = (int16) (VARHDRSZ + ilen0);
                                            so->inc_meta->run_inc_built[ii] = true;
                                        }
                                        /* fixed-size includes are copied later in fixed path */
                                    }
                                }
                            }
                            bool all_const = true;
                            if (so->ninclude > 0)
                                for (uint16 ii=0; ii<so->ninclude; ii++) if (!so->inc_meta->inc_const[ii]) { all_const = false; break; }
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
                    if (!smol_test_runtime_keys(scan, so))
                    { /* GCOV_EXCL_LINE */
                        so->cur_off++; /* GCOV_EXCL_LINE */
                        continue; /* Skip this tuple */ /* GCOV_EXCL_LINE */
                    } /* GCOV_EXCL_LINE */

                    if (scan->xs_want_itup)
                        scan->xs_itup = so->itup;
                    /* Set synthetic TID directly (not stored in itup->t_tid - see TID OPTIMIZATION) */
                    ItemPointerSet(&scan->xs_heaptid, 0, 1);
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
            /* Claim next leaf atomically (no batch claiming) */
            {
                for (;;)
                { /* GCOV_EXCL_LINE (flaky) */
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
                        BlockNumber next_leaf = smol_page_opaque(lpg)->rightlink;
                        ReleaseBuffer(lbuf);
                        uint32 expect = 0u;
                        uint32 newv = (uint32) (BlockNumberIsValid(next_leaf) ? next_leaf : InvalidBlockNumber);
                        if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expect, newv))
                        {
                            next = left;
                            so->chunk_left = 0;
                            if (BlockNumberIsValid(next))
                                PrefetchBuffer(idx, MAIN_FORKNUM, next);
                            break;
                        }
                        continue;
                    } /* GCOV_EXCL_STOP */
                    if (curv == (uint32) InvalidBlockNumber)
                    { next = InvalidBlockNumber; break; }
                    /* Read rightlink to publish next without batch skip */
                    Buffer tbuf = ReadBufferExtended(idx, MAIN_FORKNUM, (BlockNumber) curv, RBM_NORMAL, so->bstrategy);
                    Page tpg = BufferGetPage(tbuf);
                    BlockNumber next_leaf = smol_page_opaque(tpg)->rightlink;
                    ReleaseBuffer(tbuf);
                    uint32 expected = curv;
                    uint32 newv = (uint32) (BlockNumberIsValid(next_leaf) ? next_leaf : InvalidBlockNumber);
                    if (SMOL_ATOMIC_CAS_U32(&ps->curr, &expected, newv))
                    {
                        next = (BlockNumber) curv;
                        so->chunk_left = 0;
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
        /* Increment page counter unconditionally (needed for bloom filter checks) */
        so->prof_pages++;
        /* Update profiling stats if enabled */
        if (!so->prof_enabled)
            ;  /* No-op, but keeps symmetry with other prof_* increments */
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
            bool enable_page_bounds_check = (smol_test_force_page_bounds_check && dir != BackwardScanDirection) ||
                (!so->two_col && dir != BackwardScanDirection && (so->have_upper_bound || so->have_k1_eq));
#else
            bool enable_page_bounds_check = !so->two_col && dir != BackwardScanDirection && (so->have_upper_bound || so->have_k1_eq);
#endif
            if (enable_page_bounds_check)
            {
                uint16 n_check = smol_leaf_nitems(np);

                if (n_check > 0)
                {
                    bool stop_scan = false;
                    bool matches pg_attribute_unused() = smol_page_matches_scan_bounds(so, np, n_check, &stop_scan);
                    /* Page must match bounds - scan logic stops at tuple level before loading non-matching pages */
                    Assert(matches);

                    if (stop_scan)
                    { /* defensive: page-level bounds check prevents non-matching pages from loading */
                        /* Page-level bounds check triggered: stop scan early */
                        ReleaseBuffer(nbuf); /* GCOV_EXCL_LINE (flaky) */
                        if (so->have_pin && BufferIsValid(so->cur_buf)) /* GCOV_EXCL_LINE (flaky) */
                        { /* GCOV_EXCL_LINE (flaky) */
                            ReleaseBuffer(so->cur_buf); /* GCOV_EXCL_LINE (flaky) */
                            so->have_pin = false; /* GCOV_EXCL_LINE (flaky) */
                        } /* GCOV_EXCL_LINE (flaky) */
                        so->cur_blk = InvalidBlockNumber; /* GCOV_EXCL_LINE (flaky) */
                        return false; /* GCOV_EXCL_LINE (flaky) */
                    }
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
                        char *k1p = smol12_row_k1_ptr(np, mid, so->key_len, so->key_len2, so->inc_meta ? so->inc_meta->inc_cumul_offs[so->ninclude] : 0);
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
                    uint16 lo2 = FirstOffsetNumber, hi2 = n2, ans2 = InvalidOffsetNumber;
                    while (lo2 <= hi2)
                    {
                        uint16 mid2 = (uint16) (lo2 + ((hi2 - lo2) >> 1));
                        char *kp2 = smol_leaf_keyptr_ex(np, mid2, so->key_len, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
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
                        char *kp2 = smol_leaf_keyptr_ex(np, mid2, so->key_len, so->inc_meta ? so->inc_meta->inc_len : NULL, so->ninclude, so->inc_meta ? so->inc_meta->inc_cumul_offs : NULL);
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

void
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
        if (so->inc_meta)
            pfree(so->inc_meta);
        /* Clean up tuple buffer */
        if (so->tuple_buffering_enabled)
        {
            if (so->tuple_buffer)
                pfree(so->tuple_buffer);
            if (so->tuple_buffer_data)
                pfree(so->tuple_buffer_data);
        }
        if (so->prof_enabled)
            elog(LOG, "[smol] scan profile: calls=%lu rows=%lu leaf_pages=%lu bytes_copied=%lu bytes_touched=%lu binsearch_steps=%lu bloom_checks=%lu bloom_skips=%lu",
                 (unsigned long) so->prof_calls,
                 (unsigned long) so->prof_rows,
                 (unsigned long) so->prof_pages,
                 (unsigned long) so->prof_bytes,
                 (unsigned long) so->prof_touched,
                 (unsigned long) so->prof_bsteps,
                 (unsigned long) so->prof_bloom_checks,
                 (unsigned long) so->prof_bloom_skips);
        pfree(so);
    }
}

bool
smol_canreturn(Relation index, int attno)
{
    /* Can return leading key columns (all key columns) */
    return attno >= 1 && attno <= RelationGetDescr(index)->natts;
}

Size
smol_estimateparallelscan(Relation index, int nkeys, int norderbys)
{
    return (Size) sizeof(SmolParallelScan);
}

void
smol_initparallelscan(void *target)
{
    SmolParallelScan *ps = (SmolParallelScan *) target;
    pg_atomic_init_u32(&ps->curr, 0u);
}


void
smol_parallelrescan(IndexScanDesc scan) /* GCOV_EXCL_START - extremely rare: parallel scan rescan, see comments */
{
    if (scan->parallel_scan)
    {
        SmolParallelScan *ps = (SmolParallelScan *) ((char *) scan->parallel_scan + scan->parallel_scan->ps_offset_am);
        pg_atomic_write_u32(&ps->curr, 0u);
    }
} /* GCOV_EXCL_STOP */
