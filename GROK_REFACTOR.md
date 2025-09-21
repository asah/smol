**GROK Refactor Summary**
- This document summarizes functional improvements in `CLAUDE_GROK_ENHANCED_smol.c` relative to the baseline `smol.c`, and records benchmark results gathered on 20M-row datasets.

**Key Enhancements**
- Parallel chunking: Adds tunable chunk-based page scheduling for parallel scans to reduce atomic contention and improve balance.
  - GUC `smol.parallel_chunk_pages` controls pages per work chunk.
- Read-ahead prefetch: Light `smgrprefetch` in both serial and parallel scans to overlap I/O with compute.
  - GUC `smol.prefetch_distance` controls prefetch depth in serial scans; also used within a worker’s current chunk.
- Lower-bound directory: Writes a compact first/last-key directory to the metapage for fixed-width first-key types (int2/int4/int8).
  - Used by `smol_seek_lower_bound`/`smol_find_lower_bound_block` to binary-search the starting page for range scans.
  - Falls back to page-level probing when the directory isn’t present/applicable.
- Comparator fast-path: Specializes qsort’s first-key compare for int2/int4/int8 to avoid fmgr overhead on hot paths; subsequent keys use generic comparators with collation.
- Planner/AM integration: Implements `amestimateparallelscan`, enables `amcanparallel = true`, honors `amcanbackward`, and keeps IOS semantics (`amcanreturn`).
- Build-time hygiene: Explicit build-time memory context; enforces fixed-width keys and rejects NULLs; AccessExclusive lock during build to avoid write-path races.

**Benchmark Setup**
- Environment: Ubuntu 24.04, PostgreSQL 16.10 (Docker image from `Dockerfile`).
- Script: `bench/smol_bench.sql` (UNLOGGED table, analyze + freeze for IOS; planner nudged toward IOS; parallel enabled).
- Parameters: dtype=int2, rows=20,000,000; thresholds as listed; `enable_seqscan=off`, `enable_bitmapscan=off`, `enable_indexonlyscan=on`.

**Results (20M rows, int2)**
- Selective (b > 5000), par=2
  - BTREE: ~943.8 ms
  - SMOL (enhanced): ~452.4 ms
  - Speedup: ~2.1x (−52%)
  - Output: `results/bench_20M_int2_par2_thr5000.out:1`
- Full scan (no WHERE), par=2
  - BTREE: ~1021.8 ms
  - SMOL (enhanced): ~513.2 ms
  - Speedup: ~2.0x (−50%)
  - Output: `results/bench_20M_int2_par2_fullscan.out:1`
- Selective (b > 5000), par=6
  - BTREE: ~1111.7 ms
  - SMOL (enhanced): ~273.3 ms
  - Speedup: ~4.1x (−75%)
  - Output: `results/bench_20M_int2_par6_thr5000.out:1`

**Notes**
- All runs report Index Only Scan and Heap Fetches: 0.
- Directory-assisted lower-bound + prefetching noticeably helps selective scans; chunking scales well with more workers.
- Feature scope is unchanged: fixed-width, non-NULL key types; no INCLUDE columns; no bitmap scans; read-only AM.

**Reproduce**
- Build Docker image: `docker build -t smol-dev .`
- Run container: `docker run --rm -it -v "$PWD":/workspace -w /workspace smol-dev bash`
- Inside container: `make && make install`
- Run examples (20M int2):
  - Selective par=2: `psql -v dtype=int2 -v rows=20000000 -v par_workers=2 -v thr=5000 -f bench/smol_bench.sql`
  - Full scan par=2: `psql -v dtype=int2 -v rows=20000000 -v par_workers=2 -v nofilter=1 -f bench/smol_bench.sql`
  - Selective par=6: `psql -v dtype=int2 -v rows=20000000 -v par_workers=6 -v thr=5000 -f bench/smol_bench.sql`

**Tunable GUCs**
- `smol.parallel_chunk_pages` — pages per parallel work chunk (load balance vs. overhead).
- `smol.prefetch_distance` — pages to prefetch ahead (serial and within-chunk prefetching).

