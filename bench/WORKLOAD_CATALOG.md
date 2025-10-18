# SMOL Benchmark Suite - Complete Workload Catalog

## Quick Reference: All 17 Workloads

### Original Suite (1-9) - 12.5 seconds

| # | ID | Description | Queries | Key Finding |
|---|----|-------------|---------|-------------|
| 1 | timeseries_1000k_50 | Time-series, 50 metrics, hot+cold | 4 | RLE compression 3.1x |
| 2 | textkeys_uuid_1000k | UUID keys, C collation | 2 | SMOL 2.5x faster |
| 3 | composite_1000k | Composite (date, id) | 2 | Multi-column RLE |
| 4 | selectivity_0.1pct_1000k | Point queries (0.1%) | 1 | BTREE 5.7x faster |
| 5 | selectivity_10.0pct_1000k | Range scans (10%) | 1 | BTREE 2.1x faster |
| 6 | parallel_4w_1000k | Parallel 4 workers | 1 | Neutral |
| 7 | include_0cols_1000k | INCLUDE 0 columns | 1 | SMOL 3.7x smaller |
| 8 | include_4cols_1000k | INCLUDE 4 columns | 1 | SMOL 2.0x smaller |
| 9 | timeseries_1000k_ultra_low_card | RLE extreme (10 metrics) | 2 | SMOL 3.1x smaller |

### Expansion (10-17) - +16.7 seconds

| # | ID | Description | Queries | What It Tests |
|---|----|-------------|---------|---------------|
| 10 | backward_1000k | Backward scans (DESC) | 3 | ORDER BY ... DESC efficiency |
| 11 | limit_1000k | LIMIT queries | 3 | Early termination (10, 1000, range) |
| 12 | in_clause_1000k | IN clause | 3 | Multiple lookups (5, 50, 500 values) |
| 13 | dtype_int2_1000k | int2 data type | 2 | 2-byte integer optimization |
| 14 | dtype_int8_1000k | int8 data type | 2 | 8-byte integer handling |
| 15 | dtype_date_1000k | date data type | 2 | Temporal data (1 month, recent) |
| 16 | ios_1000k | Index-only scans | 2 | Covering index (100K, 10K rows) |
| 17 | partial_1000k | Partial indexes | 2 | Filtered index (10% of data) |

## Detailed Workload Specifications

---

### 1. Time-Series (Hot + Cold Cache)

**File**: `bench/workloads/timeseries.py`
**Purpose**: Test I/O efficiency and RLE compression with temporal data

**Data Generation**:
```python
rows = 1_000_000
metrics = 50
# Zipfian distribution (80% of queries hit 20% of metrics)
# Temporal locality (recent data accessed more)
```

**Queries**:
1. `hot_metric` (hot): Select single hot metric (80% probability)
2. `hot_metric` (cold): Same, with cache flushed
3. `metric_range` (hot): Range scan across time
4. `metric_range` (cold): Same, with cache flushed

**Expected**: SMOL wins on index size (3x smaller), similar query speed

---

### 2. Text/UUID Keys

**File**: `bench/workloads/textkeys.py`
**Purpose**: Test text32 zero-copy optimization

**Data Generation**:
```python
key_type = 'uuid'
# Generate UUIDs with C collation
CREATE INDEX ... USING smol(uuid_col)
```

**Queries**:
1. `equality` (hot): Point lookup on UUID
2. `range` (hot): Range scan on UUID

**Expected**: SMOL 3.8x faster (zero-copy avoids varlena overhead)

---

### 3. Composite Keys

**File**: `bench/workloads/composite.py`
**Purpose**: Test multi-column RLE

**Data Generation**:
```python
(order_date, order_id)
# Sequential dates with many orders per day
```

**Queries**:
1. `recent_orders` (hot): Last 7 days
2. `single_day` (hot): All orders for one day

**Expected**: RLE on leading column (date)

---

### 4. Selectivity 0.1%

**File**: `bench/workloads/selectivity.py`
**Purpose**: Find selectivity crossover point (BTREE advantage)

**Data Generation**:
```python
rows = 1_000_000
selectivity = 0.001  # Return 1,000 rows
```

**Queries**:
1. `sel_0.1pct` (hot): WHERE id BETWEEN x AND x+1000

**Expected**: BTREE 5.7x faster (point query advantage)

---

### 5. Selectivity 10%

**File**: `bench/workloads/selectivity.py`
**Purpose**: Large range scan (crossover point)

**Data Generation**:
```python
rows = 1_000_000
selectivity = 0.10  # Return 100,000 rows
```

**Queries**:
1. `sel_10.0pct` (hot): WHERE id BETWEEN x AND x+100000

**Expected**: BTREE 2.1x faster (but gap narrowing)

---

### 6. Parallel Scaling

**File**: `bench/workloads/parallel.py`
**Purpose**: Test parallel scan efficiency

**Data Generation**:
```python
rows = 1_000_000
workers = 4
SET max_parallel_workers_per_gather = 4;
```

**Queries**:
1. `full_scan_4w` (hot): Full table scan with 4 workers

**Expected**: Neutral (both ~25ms with 4 workers)

---

### 7. INCLUDE 0 Columns

**File**: `bench/workloads/includeoverhead.py`
**Purpose**: Baseline overhead without INCLUDE

**Data Generation**:
```python
CREATE INDEX ... (id) -- No INCLUDE
```

**Queries**:
1. `scan_0inc` (hot): SELECT * WHERE id > threshold

**Expected**: SMOL 3.7x smaller index

---

### 8. INCLUDE 4 Columns

**File**: `bench/workloads/includeoverhead.py`
**Purpose**: Columnar format advantage with INCLUDE

**Data Generation**:
```python
CREATE INDEX ... (id) INCLUDE (col1, col2, col3, col4)
```

**Queries**:
1. `scan_4inc` (hot): SELECT * WHERE id > threshold

**Expected**: SMOL 2.0x smaller, similar speed

---

### 9. RLE Extreme

**File**: `bench/workloads/timeseries.py`
**Purpose**: Maximum RLE compression (only 10 distinct values)

**Data Generation**:
```python
metrics = 10  # Very low cardinality
# Same timestamp, varies only metric_id
```

**Queries**:
1. `hot_metric` (hot): Select single metric
2. `metric_range` (hot): Range scan

**Expected**: SMOL 3.1x smaller (extreme RLE benefit)

---

### 10. Backward Scans

**File**: `bench/workloads/backward.py`
**Purpose**: Test ORDER BY ... DESC efficiency

**Data Generation**:
```python
id int4 PRIMARY KEY
# Sequential 1..1M
```

**Queries**:
1. `desc_limit100`: Last 100 rows (DESC)
2. `desc_limit10k`: Last 10K rows (DESC)
3. `desc_range`: Range + DESC

**Expected**: SMOL competitive (has backward scan optimization)

---

### 11. LIMIT Queries

**File**: `bench/workloads/limits.py`
**Purpose**: Test early termination efficiency

**Data Generation**:
```python
category int4  -- i % 100
# Many rows per category
```

**Queries**:
1. `limit10`: LIMIT 10 (tiny)
2. `limit1000`: LIMIT 1000 (medium)
3. `range_limit100`: Range + LIMIT 100

**Expected**: BTREE may win (optimized early termination)

---

### 12. IN Clause

**File**: `bench/workloads/inclause.py`
**Purpose**: Multiple point lookups in single query

**Data Generation**:
```python
id int4 PRIMARY KEY
# Scattered IDs (every 10K, 2K, 200)
```

**Queries**:
1. `in_5`: WHERE id IN (5 values)
2. `in_50`: WHERE id IN (50 values)
3. `in_500`: WHERE id IN (500 values)

**Expected**: BTREE may win for small N (<10), SMOL for large N

---

### 13. int2 Data Type

**File**: `bench/workloads/datatypes.py`
**Purpose**: Test 2-byte integer optimization

**Data Generation**:
```python
id int2 PRIMARY KEY
# Values 1..32767 (int2 range)
```

**Queries**:
1. `range_1pct`: 1% range scan
2. `point`: Single point lookup

**Expected**: SMOL similar compression to int4 (fixed-width)

---

### 14. int8 Data Type

**File**: `bench/workloads/datatypes.py`
**Purpose**: Test 8-byte integer handling

**Data Generation**:
```python
id int8 PRIMARY KEY
# Large values 1..2M
```

**Queries**:
1. `range_1pct`: 1% range scan
2. `point`: Single point lookup

**Expected**: SMOL may be slightly larger (8 bytes vs 4)

---

### 15. date Data Type

**File**: `bench/workloads/datatypes.py`
**Purpose**: Test temporal data optimization

**Data Generation**:
```python
id date PRIMARY KEY
# Sequential dates 2020-01-01 + i days
```

**Queries**:
1. `range_1month`: June 2020 (1 month)
2. `recent`: After 2022-01-01 (backward)

**Expected**: SMOL RLE benefits on sequential dates

---

### 16. Index-Only Scans

**File**: `bench/workloads/indexonly.py`
**Purpose**: Test covering index efficiency (avoid heap)

**Data Generation**:
```python
id int4 PRIMARY KEY
# VACUUM to enable IOS
```

**Queries**:
1. `ios_range`: SELECT id only (100K rows)
2. `ios_small`: SELECT id only (10K rows)

**Expected**: SMOL's columnar format may help

---

### 17. Partial Indexes

**File**: `bench/workloads/partial.py`
**Purpose**: Test filtered index (10% of data)

**Data Generation**:
```python
status text  -- 'active' (10%) or 'inactive' (90%)
CREATE INDEX ... WHERE status = 'active'
```

**Queries**:
1. `active_category`: Filtered equality
2. `active_range`: Filtered range scan

**Expected**: Even smaller SMOL indexes (only 10% indexed)

---

## Query Count by Workload

```
Workload                Queries    Cache Modes    Total Runs
────────────────────────────────────────────────────────────
timeseries (50)         2          hot, cold      4
textkeys_uuid           2          hot            2
composite               2          hot            2
selectivity_0.1pct      1          hot            1
selectivity_10.0pct     1          hot            1
parallel_4w             1          hot            1
include_0cols           1          hot            1
include_4cols           1          hot            1
timeseries (10)         2          hot            2
backward                3          hot            3
limits                  3          hot            3
in_clause               3          hot            3
dtype_int2              2          hot            2
dtype_int8              2          hot            2
dtype_date              2          hot            2
ios                     2          hot            2
partial                 2          hot            2
────────────────────────────────────────────────────────────
TOTAL                   32         -              35 query executions
```

**Note**: Some queries run in both hot and cold cache modes, increasing total executions.

## Coverage Summary

### Query Patterns (10)
1. ✅ Full scans
2. ✅ Range scans
3. ✅ Point lookups
4. ✅ Backward scans (DESC)
5. ✅ Early termination (LIMIT)
6. ✅ Multiple lookups (IN)
7. ✅ Index-only scans
8. ✅ Parallel scans
9. ✅ Cold cache I/O
10. ✅ Filtered indexes (partial)

### Data Types (6)
1. ✅ int2 (2 bytes)
2. ✅ int4 (4 bytes)
3. ✅ int8 (8 bytes)
4. ✅ date (4 bytes)
5. ✅ uuid (16 bytes)
6. ✅ text/varchar (variable)

### Index Features (7)
1. ✅ Single-column indexes
2. ✅ Multi-column indexes
3. ✅ INCLUDE columns (0, 4)
4. ✅ Partial indexes (WHERE clause)
5. ✅ Forward scans
6. ✅ Backward scans
7. ✅ Index-only scans

### Cache Modes (2)
1. ✅ Hot cache (all in shared_buffers)
2. ✅ Cold cache (flushed before query)

### Parallelism (2)
1. ✅ Sequential (single worker)
2. ✅ Parallel (4 workers)

## Total Coverage

- **17 workloads**
- **35 query executions** (some hot+cold)
- **10 query patterns**
- **6 data types**
- **7 index features**
- **~30 seconds runtime**

**Comprehensive SMOL vs BTREE comparison in less time than making coffee!** ☕
