# SMOL Benchmark Suite v2

Comprehensive, auto-scaling benchmark suite for comparing SMOL vs BTREE performance across real-world workload patterns.

## Quick Start

```bash
# Quick comprehensive test (~30 sec, 17 workloads)
make bench-quick

# Full comprehensive suite (~15-20 min)
make bench-full

# Help
make bench-help
```

## Design Philosophy

This benchmark suite is designed with three core principles:

1. **Real-world workloads**: Not synthetic micro-benchmarks, but patterns that reflect actual production use cases
2. **Auto-scaling**: Tests automatically adapt to available resources (shared_buffers, CPU)
3. **Actionable insights**: Results include clear recommendations on when to use SMOL vs BTREE

## Workload Classes

### 1. Time-Series Analytics
- **Pattern**: Ordered timestamps, low cardinality metrics, range scans
- **Use case**: APM, monitoring, IoT data
- **SMOL advantage**: RLE compression on repeated dimension values

### 2. Dimension Tables
- **Pattern**: Small, mostly-static lookup tables with frequent joins
- **Use case**: Country codes, product categories, user types
- **SMOL advantage**: Entire index fits in cache, smaller footprint

### 3. Event Streams
- **Pattern**: Zipfian distribution, hot keys get 80% of traffic
- **Use case**: User activity logs, clickstream data
- **SMOL advantage**: RLE + duplicate caching for hot keys

### 4. Sparse Indexes
- **Pattern**: Partial index on filtered subset
- **Use case**: Index on active orders, pending tasks
- **SMOL advantage**: Extreme compression (only 2-10 distinct values)

### 5. Composite Keys
- **Pattern**: (date, id) multi-column indexes
- **Use case**: Order history, event logs by date
- **SMOL advantage**: Sequential access pattern, RLE on leading column

## Auto-Scaling

The suite automatically detects your environment and scales tests:

| shared_buffers | Scale | Row Counts | Duration | Use Case |
|----------------|-------|------------|----------|----------|
| < 256 MB | micro | 100K, 500K | 2-3 min | CI/small VM |
| 256 MB - 2 GB | standard | 1M, 5M | 10-15 min | Developer workstation |
| > 2 GB | large | 5M, 20M, 50M | 30-45 min | Production-like |

## Metrics Tracked

For each workload, we measure:

- **Performance**: Latency percentiles (p50, p95, p99)
- **Resources**: Index size, build time, buffer hits/reads
- **Cache behavior**: Hot/warm/cold query performance

## Output

### Console Output
```
╔══════════════════════════════════════════════════════════╗
║  SMOL Benchmark Suite v2 - Developer Workstation         ║
╚══════════════════════════════════════════════════════════╝

Environment:
  PostgreSQL: 18.0
  shared_buffers: 1024 MB
  Test scale: standard

[1/3] Workload: Time-series: 1,000,000 rows, 50 metrics
  BTREE: 180.5 MB, 45.2ms (p50)
  SMOL:  8.2 MB, 32.8ms (p50)  → 1.4x faster, 22x smaller
```

### Decision Tree
```
✓ STRONGLY RECOMMEND SMOL
  Workload: timeseries_1m_ultra_low_card
  Reason: 1.4x faster, 22x smaller

Use SMOL when:
  • Low cardinality data (high RLE compression potential)
  • Memory-constrained environments
  • Read-heavy analytical workloads
```

### Files Generated
- `bench/results/quick-YYYYMMDD-HHMMSS.json` - Raw results
- `bench/results/quick-YYYYMMDD-HHMMSS.md` - Markdown report

## Regression Detection

The suite can compare results against a baseline to detect performance regressions.

**IMPORTANT**: Baselines are **platform-specific** (CPU, memory, disk). Each developer/CI environment needs its own baseline.

### Setup Baseline (First Time)

```bash
# 1. Run benchmark to get initial results
make bench-quick

# 2. Review results to ensure they look reasonable
cat bench/results/quick-TIMESTAMP.json

# 3. Set as baseline (creates bench/config/baseline.json)
python3 bench/runner.py --update-baseline bench/results/quick-TIMESTAMP.json
```

### Using Baseline

Once a baseline exists, future runs will automatically check for regressions:
- **>15% slower**: WARNING or CRITICAL alert
- **>10% larger index**: WARNING alert

```bash
# Just run the benchmark - regression check is automatic
make bench-quick
```

### Notes
- `bench/config/baseline.json` is **gitignored** (platform-specific)
- Without a baseline, the suite runs normally but skips regression checking
- Safe to delete baseline.json anytime to start fresh

## CLI Usage

```bash
# Quick mode (3 workloads)
python3 bench/runner.py --quick

# Full mode (all workloads, all configurations)
python3 bench/runner.py --full

# Update baseline
python3 bench/runner.py --update-baseline RESULTS.json
```

## Architecture

```
bench/
├── runner.py                      # Main orchestrator
├── workloads/                     # Workload implementations (17 classes)
│   ├── base.py                   # Abstract base class
│   ├── timeseries.py
│   ├── dimension.py
│   ├── events.py
│   ├── sparse.py
│   ├── composite.py
│   ├── textkeys.py
│   ├── selectivity.py
│   ├── parallel.py
│   ├── backward.py
│   ├── limits.py
│   ├── inclause.py
│   ├── datatypes.py
│   ├── indexonly.py
│   ├── partial.py
│   └── ...
├── lib/                          # Core utilities
│   ├── db.py                    # Database connection, auto-scaling
│   ├── cache.py                 # Cache control
│   ├── metrics.py               # Result data structures
│   ├── reporting.py             # Report generation, decision trees
│   └── regression.py            # Regression detection
├── config/
│   ├── baseline.json.example    # Template (tracked)
│   └── baseline.json            # Your baseline (gitignored)
├── results/                      # Generated reports (gitignored)
└── archive/                      # Legacy benchmarks
```

## Design Rationale

### Why Not Micro-Benchmarks?

Traditional database benchmarks (TPC, YCSB) use synthetic workloads that don't answer "when should I use SMOL?"

This suite focuses on **workload classes** that represent real production patterns:
- Time-series analytics (not "scan 1M rows")
- Dimension tables (not "random lookups")
- Event streams (not "Zipf distribution benchmark")

### Why Auto-Scaling?

A benchmark that takes 2 hours on a CI VM and 30 seconds on a production server is useless for regression testing. Auto-scaling ensures:
- CI completes in <5 min
- Local dev provides quick feedback
- Full suite validates performance claims

### Why Decision Trees?

Raw numbers (45.2ms vs 32.8ms) don't help developers choose an index type. The decision tree provides:
- Clear recommendations
- Reasoning based on workload characteristics
- Trade-off analysis

## Extending

To add a new workload:

1. Create `bench/workloads/my_workload.py`:
```python
from bench.workloads.base import WorkloadBase

class MyWorkload(WorkloadBase):
    def get_workload_id(self) -> str:
        return "my_workload"

    def get_description(self) -> str:
        return "My custom workload"

    def generate_data(self):
        # Create table and populate
        pass

    def get_queries(self):
        # Return list of Query objects
        return [Query(id='q1', sql='...', description='...')]
```

2. Add to runner in `bench/runner.py`:
```python
from bench.workloads.my_workload import MyWorkload

workloads.append(MyWorkload(self.db, self.cache, config))
```

## Troubleshooting

### "Could not load smol extension"
Ensure SMOL is installed: `make build && make start`

### "Timeout exceeded"
Reduce row counts or increase timeout in config

### "No pg_prewarm extension"
Cold cache tests will use best-effort eviction (CHECKPOINT only)

## Legacy Benchmarks

Old benchmarks are archived in `bench/archive/`:
- `bench_runner.py` - Original Python runner
- `bench.sh` - Shell-based runner
- `quick.sql` - SQL-only tests

These are kept for reference but not actively maintained.
