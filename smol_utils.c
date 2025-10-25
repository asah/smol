/*
 * smol_utils.c
 *
 * Utility functions for SMOL index access method
 * - Page/metadata access
 * - Tree navigation
 * - Leaf page helpers
 */

#include "smol.h"

/* Forward declaration for recursive function */
static BlockNumber smol_find_prev_leaf_recursive(Relation idx, BlockNumber node, BlockNumber target, uint16 levels, bool *found);

void
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

void
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

Buffer
smol_extend(Relation idx)
{
    instr_time t0, t1;
    Buffer buf = ReadBufferExtended(idx, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
    INSTR_TIME_SET_CURRENT(t0);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    INSTR_TIME_SET_CURRENT(t1);
    if (smol_debug_log && SMOL_WAIT_LOG_MS > 0)
    {
        double ms = (INSTR_TIME_GET_DOUBLE(t1) - INSTR_TIME_GET_DOUBLE(t0)) * 1000.0;
        if (ms > SMOL_WAIT_LOG_MS)
            SMOL_LOGF("slow LockBuffer(new) wait ~%.1f ms on blk=%u", /* GCOV_EXCL_LINE - timing-dependent, requires lock contention */
                      ms, BufferGetBlockNumber(buf));
    }
    return buf;
}

void
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
void
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

BlockNumber
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

        /* Binary search for first child where highkey >= lower_bound */
        OffsetNumber lo = FirstOffsetNumber, hi = maxoff;
        while (lo <= hi)
        {
            OffsetNumber mid = (OffsetNumber) (lo + ((hi - lo) >> 1));
            char *itp = (char *) PageGetItem(page, PageGetItemId(page, mid));
            BlockNumber c;
            memcpy(&c, itp, sizeof(BlockNumber));
            char *keyp = itp + sizeof(BlockNumber);
            if (smol_cmp_keyptr_bound(keyp, key_len, atttypid, lower_bound) >= 0)
            {
                child = c;
                if (mid == FirstOffsetNumber) break;
                hi = (OffsetNumber) (mid - 1);
            }
            else
                lo = (OffsetNumber) (mid + 1);
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
BlockNumber
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

        /* Binary search for first child where highkey >= lower_bound */
        OffsetNumber lo = FirstOffsetNumber, hi = maxoff;
        while (lo <= hi)
        {
            OffsetNumber mid = (OffsetNumber) (lo + ((hi - lo) >> 1));
            char *itp = (char *) PageGetItem(page, PageGetItemId(page, mid));
            BlockNumber c;
            memcpy(&c, itp, sizeof(BlockNumber));
            char *keyp = itp + sizeof(BlockNumber);
            /* Use generic comparator that handles text correctly */
            int cmp = smol_cmp_keyptr_to_bound(so, keyp);
            if (cmp >= 0)
            {
                child = c;
                if (mid == FirstOffsetNumber) break;
                hi = (OffsetNumber) (mid - 1);
            }
            else
                lo = (OffsetNumber) (mid + 1);
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
/* Find leaf containing values around upper bound for backward scans. */
BlockNumber __attribute__((unused))
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


int
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
    /* Fix for "char" type and other types with no collation (collation=0).
     * FunctionCall2Coll expects InvalidOid for non-collatable types, not 0. */
    Oid coll = (collation == 0) ? InvalidOid : collation;
    int32 c = DatumGetInt32(FunctionCall2Coll(cmp, coll, kd, bound));
    return (c > 0) - (c < 0);
}

/* Legacy integer comparator used by two-column/internal paths */
int
smol_cmp_keyptr_bound(const char *keyp, uint16 key_len, Oid atttypid, int64 bound)
{
    if (key_len == 2)
    { int16 v; memcpy(&v, keyp, 2); return (v > bound) - (v < bound); }
    else if (key_len == 4)
    { int32 v; memcpy(&v, keyp, 4); return ((int64)v > bound) - ((int64)v < bound); }
    else
    { int64 v; memcpy(&v, keyp, 8); return (v > bound) - (v < bound); }
}

uint16
smol_leaf_nitems(Page page)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 tag;
    memcpy(&tag, p, sizeof(uint16));
    if (tag == SMOL_TAG_KEY_RLE ||
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
char *
smol_leaf_keyptr_ex(Page page, uint16 idx, uint16 key_len, const uint16 *inc_lens, uint16 ninc, const uint32 *inc_cumul_offs)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 tag;
    memcpy(&tag, p, sizeof(uint16));

    if (!(tag == SMOL_TAG_KEY_RLE || tag == SMOL_TAG_KEY_RLE_V2 || tag == SMOL_TAG_INC_RLE))
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

/* GCOV_EXCL_START - unused wrapper function (all callers use smol_leaf_keyptr_ex directly) */
/* Wrapper for backward compatibility - assumes single-run or no includes */
char *
smol_leaf_keyptr(Page page, uint16 idx, uint16 key_len)
{
    return smol_leaf_keyptr_ex(page, idx, key_len, NULL, 0, NULL);
}
/* GCOV_EXCL_STOP */

bool
smol_key_eq_len(const char *a, const char *b, uint16 key_len)
{
    /* Fixed-length, small keys: branch by common sizes */
    if (key_len == 2)
    { int16 x, y; memcpy(&x, a, 2); memcpy(&y, b, 2); return x == y; }
    if (key_len == 4)
    { int32 x, y; memcpy(&x, a, 4); memcpy(&y, b, 4); return x == y; }
    if (key_len == 8)
    { int64 x, y; memcpy(&x, a, 8); memcpy(&y, b, 8); return x == y; }
    return memcmp(a, b, key_len) == 0;
}

/* GCOV_EXCL_START - unused function (only referenced in comment, never called) */
bool
smol_leaf_is_rle(Page page)
{
    ItemId iid = PageGetItemId(page, FirstOffsetNumber);
    char *p = (char *) PageGetItem(page, iid);
    uint16 tag; memcpy(&tag, p, sizeof(uint16));
    /* RLE formats have tags; plain format uses nitems as first u16 */
    return (tag == SMOL_TAG_KEY_RLE ||
            tag == SMOL_TAG_KEY_RLE_V2 || tag == SMOL_TAG_INC_RLE);
}
/* GCOV_EXCL_STOP */

/* GCOV_EXCL_START - debug-only function (requires PGC_SUSET GUC smol.debug_log) */
void
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
BlockNumber
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
BlockNumber
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
BlockNumber __attribute__((unused))
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
/* GCOV_EXCL_START - debug-only function (requires PGC_SUSET GUC smol.debug_log) */
char *
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
