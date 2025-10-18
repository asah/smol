# Benchmark Suite Development History

## Version 3: 30-Second Suite (2025-10-17)

**Goal**: Expand from 12.5s → 30s with more comprehensive coverage

**Approach**: Incremental development, measuring runtime after each addition using `time(1)`. NO dynamic scaling - hardcoded 17 workloads.

### What Was Added (8 workloads, +16.7s)

| # | Workload | Time | Purpose |
|---|----------|------|---------|
| 10 | Backward scans | +2.2s | ORDER BY ... DESC optimization |
| 11 | LIMIT queries | +2.2s | Early termination (10, 1000, range) |
| 12 | IN clause | +1.9s | Multiple lookups (5, 50, 500 values) |
| 13 | int2 type | +2.0s | 2-byte integer optimization |
| 14 | int8 type | +2.0s | 8-byte integer handling |
| 15 | date type | +2.0s | Temporal data |
| 16 | Index-only scans | +2.2s | Covering indexes |
| 17 | Partial indexes | +2.2s | Filtered indexes (10% of data) |

### Final Metrics
- **Runtime**: 27-29s (target: 30s ±10%) ✓
- **Workloads**: 17 (up from 9)
- **Query patterns**: 10 (up from 6)
- **Data types**: 6 (up from 3)
- **Consistency**: ±2.5% variance across runs

### New Coverage
- ✅ Backward scans (DESC)
- ✅ LIMIT queries (early termination)
- ✅ IN clauses (multiple point lookups)
- ✅ Additional data types (int2, int8, date)
- ✅ Index-only scans (covering indexes)
- ✅ Partial indexes (filtered)

### Key Findings
- **SMOL wins**: Partial indexes (1.3x faster), INCLUDE columns (1.2x faster)
- **BTREE wins**: Still dominates low selectivity (<10%)
- **Neutral**: Index-only scans, LIMIT queries, parallel scans

### Future Wishlist (Deferred, Would Need 60+ Second Suite)
- Concurrent throughput testing (5-10s)
- Build time at scale (1M, 10M, 50M rows) (10s)
- Worst-case distributions (random UUIDs) (5s)
- Multi-size scaling (100K, 1M, 10M) (15s)
- Memory pressure simulation (tiny shared_buffers) (5s)

**Why deferred**: 30s is perfect for CI. 60s+ would be too slow for regression testing.

---

## Version 2: 13-Second Suite (2025-10-17)

**Goal**: Expand from 4s → 13s with critical missing patterns

### What Was Added (5 workloads, +9s)

1. **TextKeysWorkload** (+3s)
   - UUID/VARCHAR with C collation
   - Tests text32 zero-copy optimization
   - **Result**: SMOL 3.8x faster for UUID keys

2. **SelectivityRangeWorkload** (+4s)
   - Tests 0.1% and 10% selectivity
   - Finds crossover point
   - **Result**: BTREE 5.7x faster at 0.1%, 2.1x faster at 10%

3. **ParallelScalingWorkload** (+2s)
   - Tests with 4 workers
   - **Result**: Neutral (both ~25ms)

4. **IncludeOverheadWorkload** (+2s)
   - Tests 0 vs 4 INCLUDE columns
   - **Result**: SMOL 3.7x smaller with 0, neutral with 4

5. **NullRejectionWorkload** (later removed)
   - Documented NULL limitation
   - **Removed**: Not a performance test, correctness only

### Key Findings
- ✅ UUID/text keys: SMOL 3.8x faster
- ✗ Low selectivity: BTREE 5.7x faster for point queries
- ~ Parallel: Neutral performance

### Methodological Fix Applied
**Problem**: Cache pollution in repeated runs (only first run was truly cold)

**Solution**:
- Cold cache: 1 run only
- Hot cache: Warmup run (discarded) + 5 measured runs
- Documented in METHODOLOGY.md

---

## Version 1: Initial 4-Second Suite (2025-10-17)

**Goal**: Proof-of-concept comprehensive suite

### Original Workloads (5 workloads, 4s)

1. **TimeSeriesWorkload** - RLE compression test
2. **DimensionWorkload** - Small lookup tables
3. **EventStreamWorkload** - Zipfian distribution
4. **SparseWorkload** - Extreme compression (2-10 distinct values)
5. **CompositeWorkload** - Multi-column indexes

### Initial Results
- SMOL consistently 3-4x smaller indexes
- Similar query performance (hot cache)
- RLE effective for low-cardinality data

### Limitations Identified
- Too fast (only 4s) - insufficient coverage
- Missing critical patterns (UUID keys, selectivity ranges)
- No backward scan testing
- No data type variation

---

## Architecture Evolution

### Core Infrastructure (Stable Since v1)
- `bench/lib/db.py` - Database connection, auto-scaling
- `bench/lib/cache.py` - Portable cache control
- `bench/lib/metrics.py` - Result data structures
- `bench/lib/reporting.py` - Report generation, decision trees
- `bench/lib/regression.py` - Baseline comparison
- `bench/workloads/base.py` - Abstract workload class

### Design Principles (Unchanged)
1. **Real-world workloads** - Not synthetic micro-benchmarks
2. **Auto-scaling** - Adapts to shared_buffers
3. **Actionable insights** - Clear SMOL vs BTREE guidance

### Methodology Improvements
- **v1**: Multiple runs, no warmup, cache pollution
- **v2**: Fixed cache methodology (warmup + multiple runs for hot, single run for cold)
- **v3**: Maintained v2 methodology

---

## Coverage Progression

| Aspect | v1 (4s) | v2 (13s) | v3 (30s) |
|--------|---------|----------|----------|
| Workloads | 5 | 10 | 17 |
| Query patterns | 4 | 6 | 10 |
| Data types | 1 (int4) | 3 (int4, uuid, text) | 6 (int2/4/8, uuid, text, date) |
| Scan directions | 1 (forward) | 1 | 2 (forward, backward) |
| Result sizes | Full scans | Full + selective | + LIMIT tests |
| Lookup patterns | Single equality | Single + range | + IN clause |
| Index strategies | Full | Full + INCLUDE | + Partial |
| Heap access | Always | Always | + Index-only |

---

## Regression Testing

### Baseline System (Added v1)
- JSON file stores reference timings
- Detects >15% latency regression
- Detects >10% size regression

**NOTE**: Baseline is **platform-specific** and should NOT be committed to git. See bench/config/baseline.json.example for template.

### How to Use Baseline
```bash
# Run benchmark
python3 bench/runner.py --quick

# Update baseline (if results acceptable)
python3 bench/runner.py --update-baseline bench/results/quick-TIMESTAMP.json

# Future runs will detect regressions against this baseline
```

---

## Files Created

### Documentation
- `bench/README.md` - User documentation
- `bench/METHODOLOGY.md` - Statistical methodology
- `bench/WORKLOAD_CATALOG.md` - Complete workload reference
- `bench/CHANGELOG.md` (this file)

### Infrastructure (v1)
- `bench/lib/*.py` - Core infrastructure (6 files)
- `bench/config/baseline.json.example` - Baseline template

### Workloads
- **v1**: 5 workload classes
- **v2**: +5 workload classes (TextKeys, Selectivity, Parallel, Include, Null)
- **v3**: +6 workload classes (Backward, Limits, InClause, DataTypes, IndexOnly, Partial)
- **Total**: 17 workload implementations

---

## Statistics

### Total Implementation
- **Lines of code**: ~2,500 (infrastructure + workloads)
- **Documentation**: ~30 pages (README, METHODOLOGY, CATALOG, CHANGELOG)
- **Runtime**: 27-29s (perfect for CI)
- **Coverage**: 10 query patterns, 6 data types, 17 workloads

### Performance
- **Execution**: 17 workloads, 35 query executions
- **Consistency**: ±2.5% variance
- **Error rate**: 0% (all tests pass)
- **Value per second**: ~0.6 workloads/second

---

## Future Directions

### Potential Improvements
1. **Concurrent workload** - Multiple parallel clients
2. **Scale testing** - 100K, 1M, 10M, 100M rows
3. **Memory pressure** - Test with tiny shared_buffers (16MB)
4. **Worst-case data** - Adversarial distributions
5. **Write workloads** - Document SMOL's read-only limitation
6. **Cost model validation** - EXPLAIN cost vs actual runtime

### Why Not Implemented
- **Time constraint**: Would push to 60-90s runtime
- **CI concerns**: 30s is acceptable, 60s+ is not
- **Diminishing returns**: Core patterns already covered
- **Infrastructure needs**: Some require postgres restart

---

## Conclusion

The benchmark suite evolved from a 4-second proof-of-concept to a comprehensive 30-second production suite through three iterations:

1. **v1 (4s)**: Established architecture, proved concept
2. **v2 (13s)**: Fixed methodology, added critical patterns
3. **v3 (30s)**: Comprehensive coverage, ready for production

**Current state**: Production-ready suite providing honest, actionable guidance on SMOL vs BTREE trade-offs in <30 seconds.
