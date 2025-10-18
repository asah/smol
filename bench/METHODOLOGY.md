# SMOL Benchmark Methodology

## Statistical Rigor

### Cache State Handling

**Problem:** Naive benchmarking flushes cache once, then runs query N times. Result:
- Run 1: Cold cache ✓
- Runs 2-N: Warm cache ✗ (pollutes results)

**Our Solution:**

#### Cold Cache Tests
```python
# Flush cache
cache.flush_relation(index)

# Single run only (cold state)
result = execute_query()
```

**Why single run?** Flushing before each run is prohibitively slow (adds 1-2s per run). Cold cache variability is typically ±10%, acceptable for decision-making.

#### Hot/Warm Cache Tests
```python
# Warmup run (discarded)
execute_query()  # Loads index into cache

# Measure 5 runs (all hot)
for i in range(5):
    timings.append(execute_query())

# Report percentiles
report(p50, p95, p99)
```

**Why warmup?** First run after cache flush shows cache-loading overhead, not steady-state performance.

**Why 5 runs?** Statistical significance:
- 3 runs: p95 = max (unreliable)
- 5 runs: p95 is 5th value (reasonable)
- 10+ runs: Diminishing returns for benchmark time

### Percentile Metrics

We report **percentiles, not averages:**

| Metric | Why |
|--------|-----|
| **p50 (median)** | Typical performance, robust to outliers |
| **p95** | Tail latency, catches variability |
| **p99** | Worst-case (only with 10+ runs) |
| **avg** | Reported but not primary (sensitive to outliers) |

**Example:**
```
Runs: [10ms, 11ms, 10ms, 11ms, 50ms]  (1 outlier)
Avg:  18.4ms  ← Misleading!
p50:  11ms    ← Actual typical performance
p95:  50ms    ← Captures the outlier
```

### Query Repetition Strategy

| Query Type | Repeats | Reason |
|------------|---------|--------|
| Point lookup | 10 | Fast (ms), need more samples |
| Range scan | 5 | Default, good balance |
| Full scan | 3 | Slow (seconds), fewer needed |
| Aggregation | 5 | Moderate speed |

Overridable per query:
```python
Query(
    id='point_lookup',
    sql='SELECT * FROM t WHERE id = 42',
    repeat=10  # Override default
)
```

### Cache Modes Explained

#### Hot Cache
- **Definition**: Index fully cached, no disk I/O
- **Method**: Warmup run, then measure
- **Measures**: CPU-only performance (scan/decompress overhead)
- **Real-world**: Production after index loads into memory

#### Warm Cache
- **Definition**: Working set in cache, cold pages on disk
- **Method**: CHECKPOINT + DISCARD PLANS (partial eviction)
- **Measures**: Mixed RAM + disk performance
- **Real-world**: Production under mixed workload

#### Cold Cache
- **Definition**: Nothing cached, all disk I/O
- **Method**: Full cache eviction (pg_buffercache if available)
- **Measures**: Worst-case startup performance
- **Real-world**: First query after restart, large index scan

### Baseline Comparison

**Regression detection:**
```python
if current.latency_p50 > baseline.latency_p50 * 1.15:
    alert("WARNING: 15% slower than baseline")

if current.latency_p50 > baseline.latency_p50 * 1.50:
    alert("CRITICAL: 50% slower than baseline")
```

**Why 15% threshold?**
- <10%: Natural variability, not actionable
- 10-15%: Gray area
- >15%: Likely real regression
- >50%: Critical performance bug

**Size regression:**
```python
if current.index_size > baseline.index_size * 1.10:
    alert("WARNING: Index 10% larger")
```

**Why 10% for size?** Index size is deterministic (no natural variability), so even small increases matter.

### Auto-Scaling Row Counts

**Problem:** Fixed row counts (e.g., always 10M) cause:
- CI timeouts (small VMs)
- Wasted time on large servers
- Non-representative of target environment

**Solution:** Scale based on `shared_buffers`:

```python
if shared_buffers < 256 MB:
    rows = [100_000, 500_000]      # CI/small VM
    timeout = 3 min

elif shared_buffers < 2 GB:
    rows = [1_000_000, 5_000_000]  # Developer workstation
    timeout = 15 min

else:
    rows = [20_000_000, 50_000_000]  # Production-like
    timeout = 45 min
```

**Rationale:**
- Small buffers = small datasets (avoid thrashing)
- Large buffers = large datasets (test real scale)
- Timeout scales proportionally

### Data Generation

**Realistic distributions, not uniform random:**

#### Time-Series (Temporal Locality)
```sql
-- Recent data accessed more (90% of queries hit last 30 days)
ts = NOW() - (random() * POWER(0.7, 2) * INTERVAL '365 days')
```

#### Events (Zipfian/Power Law)
```sql
-- Top 1% users = 50% of events
user_id = CASE
    WHEN random() < 0.5 THEN random() * (total_users * 0.01)
    ELSE random() * total_users
END
```

#### Dimension Tables (Small, Fixed Set)
```sql
-- Country codes (real cardinality ~249)
country = ARRAY['US','CN','IN',...][i % 10]
```

### Workload Classification

Each benchmark targets a **workload class**, not a synthetic pattern:

| Class | Real-World Example | SMOL Advantage |
|-------|-------------------|----------------|
| **Time-Series** | APM, monitoring | RLE on repeated metrics |
| **Dimension** | Lookup tables | Entire index fits in cache |
| **Events** | Clickstream logs | Hot key caching |
| **Sparse** | Partial indexes | Extreme compression |
| **Composite** | (date, id) keys | Sequential RLE |

### Limitations & Future Work

**Current limitations:**
1. **Cold cache:** Single run (higher variance)
2. **No I/O breakdown:** Can't separate read vs CPU time
3. **No multi-tenant:** Single workload at a time
4. **PostgreSQL-only:** No cross-DB comparison

**Potential improvements:**
1. Multiple cold runs with cache flush between (slower but more accurate)
2. Use `pg_stat_statements` for detailed I/O breakdown
3. Concurrent workload simulation (reader + writer threads)
4. Add MySQL/SQLite adapters for comparison

### References

- **TPC-H/TPC-C**: Industry-standard database benchmarks
- **YCSB**: Yahoo Cloud Serving Benchmark (workload patterns)
- **Gil Tene on percentiles**: Why p99 > average for latency
- **PostgreSQL pg_buffercache**: Cache eviction methodology
- **RUM conjecture**: Read/Update/Memory trade-offs in index structures
