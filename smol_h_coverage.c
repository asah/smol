/*
 * smol_h_coverage.c
 *
 * Non-inline implementations of functions from smol.h for coverage builds.
 *
 * When SMOL_TEST_COVERAGE is defined, functions that are normally static inline
 * in smol.h are instead declared extern and implemented here. This allows gcov
 * to track their execution and measure code coverage.
 *
 * In production builds (without SMOL_TEST_COVERAGE), these functions remain
 * as static inline in the header for optimal performance.
 */

#include "smol.h"
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"

/* Fast copy helpers - non-inline implementations for coverage tracking */

void smol_copy1(char *dst, const char *src)
{
	*dst = *src;
}

void smol_copy2(char *dst, const char *src)
{
	__builtin_memcpy(dst, src, 2);
}

void smol_copy4(char *dst, const char *src)
{
	__builtin_memcpy(dst, src, 4);
}

void smol_copy8(char *dst, const char *src)
{
	__builtin_memcpy(dst, src, 8);
}

void smol_copy16(char *dst, const char *src)
{
	__builtin_memcpy(dst, src, 16);
}

/* Generic small copy for uncommon fixed lengths (<= 32) */
void
smol_copy_small(char *dst, const char *src, uint16_t len)
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
uint64
smol_norm64(int64 v)
{
	return (uint64) v ^ UINT64_C(0x8000000000000000);
}

/* Bound comparison helpers - non-inline implementations for coverage tracking */

int
smol_cmp_keyptr_to_bound(SmolScanOpaque so, const char *keyp)
{
	if (so->have_bound && (so->atttypid == INT2OID || so->atttypid == INT4OID || so->atttypid == INT8OID))
		return smol_cmp_keyptr_bound(keyp, so->key_len, so->atttypid, (int64)
		                             (so->atttypid == INT2OID ? (int64) DatumGetInt16(so->bound_datum)
		                                                      : so->atttypid == INT4OID ? (int64) DatumGetInt32(so->bound_datum)
		                                                                                : DatumGetInt64(so->bound_datum)));
	if (so->have_bound && (so->atttypid == TEXTOID /* || so->atttypid == VARCHAROID */))
	{
		/* Non-C collation: use PostgreSQL's comparator (can't use memcmp with non-C collations) */
		if (so->use_generic_cmp)
		{
			/* Fall through to generic comparator for non-C collations */
			return smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, so->atttypid, keyp, so->key_len, so->key_byval, so->bound_datum);
		}

		/* C collation: compare 32-byte padded keyp with detoasted bound text (binary) */
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
	return smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, so->atttypid, keyp, so->key_len, so->key_byval, so->bound_datum);
}

int
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
		/* Non-C collation: use PostgreSQL's comparator (can't use memcmp with non-C collations) */
		if (so->use_generic_cmp)
		{
			/* Fall through to generic comparator for non-C collations */
			return smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, so->atttypid, keyp, so->key_len, so->key_byval, so->upper_bound_datum);
		}

		/* C collation: compare with detoasted bound text (binary) */
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
	return smol_cmp_keyptr_bound_generic(&so->cmp_fmgr, so->collation, so->atttypid, keyp, so->key_len, so->key_byval, so->upper_bound_datum);
}
