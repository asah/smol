#!/usr/bin/env python3
"""
SMOL Benchmark Runner - Pretty output and comprehensive testing
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Color output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def colorize(text, color):
    return f"{color}{text}{Colors.ENDC}"

def run_sql(sql, db='postgres'):
    """Execute SQL and return output"""
    result = subprocess.run(
        ['psql', '-d', db, '-t', '-A', '-q'],
        input=sql.encode(),
        capture_output=True,
        check=True
    )
    return result.stdout.decode().strip()

def flush_caches(relation=None):
    """
    Flush PostgreSQL shared_buffers and OS page cache

    Args:
        relation: If provided, flush only this specific relation using
                 pg_buffercache_evict_relation() and vmtouch.
                 If None, flush all caches.
    """
    if relation:
        # Targeted eviction for specific relation
        try:
            # Step 1: CHECKPOINT
            run_sql("CHECKPOINT;")

            # Step 2: Evict from PostgreSQL shared_buffers
            result = run_sql(f"SELECT buffers_evicted FROM pg_buffercache_evict_relation('{relation}'::regclass);")
            buffers_evicted = int(result) if result and result.isdigit() else 0

            # Step 3: Evict from OS cache using vmtouch
            import os
            script_dir = os.path.dirname(os.path.abspath(__file__))
            evict_script = os.path.join(script_dir, 'evict_pg_relations.sh')

            if os.path.exists(evict_script):
                subprocess.run([evict_script, relation], capture_output=True, timeout=10)

            return buffers_evicted > 0
        except Exception:
            run_sql("CHECKPOINT;")
            return False
    else:
        # Global cache flush - just CHECKPOINT
        run_sql("CHECKPOINT;")
        return False

def run_sql_timing(sql, db='postgres'):
    """Execute SQL with timing and return elapsed ms"""
    start = time.time()
    run_sql(sql, db)
    elapsed_ms = (time.time() - start) * 1000
    return elapsed_ms

def parse_explain_json(json_str):
    """Parse EXPLAIN (FORMAT JSON) output"""
    data = json.loads(json_str)
    plan = data[0]['Plan']
    exec_time = data[0].get('Execution Time', 0)
    plan_time = data[0].get('Planning Time', 0)

    # Extract buffer stats if present
    buffers = {
        'shared_hit': plan.get('Shared Hit Blocks', 0),
        'shared_read': plan.get('Shared Read Blocks', 0),
        'shared_written': plan.get('Shared Written Blocks', 0),
    }

    return {
        'exec_time': exec_time,
        'plan_time': plan_time,
        'buffers': buffers,
        'rows': plan.get('Actual Rows', 0)
    }

def parse_explain_buffers(explain_output):
    """Extract buffer statistics from EXPLAIN output (text format)"""
    import re

    # Try to extract from EXPLAIN ANALYZE output
    shared_hit = 0
    shared_read = 0

    # Match "Buffers: shared hit=XXXX"
    hit_match = re.search(r'shared hit=(\d+)', explain_output)
    if hit_match:
        shared_hit = int(hit_match.group(1))

    # Match "Buffers: shared read=XXXX"
    read_match = re.search(r'shared read=(\d+)', explain_output)
    if read_match:
        shared_read = int(read_match.group(1))

    return {'shared_hit': shared_hit, 'shared_read': shared_read}

class Benchmark:
    def __init__(self, mode='quick', repeats=1):
        self.mode = mode
        self.repeats = repeats
        self.results = []
        self.timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        self.results_dir = Path('results')
        self.results_dir.mkdir(exist_ok=True)

    def print_header(self, text):
        print(f"\n{colorize('═' * 80, Colors.HEADER)}")
        print(f"{colorize(text, Colors.BOLD + Colors.HEADER)}")
        print(f"{colorize('═' * 80, Colors.HEADER)}")

    def print_subheader(self, text):
        print(f"\n{colorize(text, Colors.OKCYAN + Colors.BOLD)}")
        print(f"{colorize('─' * len(text), Colors.OKCYAN)}")

    def run_case(self, case_id, engine, rows, duplicates='unique', selectivity=0.5,
                 includes=2, warm=True, workers=0):
        """Run a single benchmark case"""

        # Create table (sanitize case_id for SQL identifiers)
        safe_case_id = case_id.replace('.', '_').replace('-', '_')
        table_name = f"bench_{safe_case_id}"
        idx_name = f"{table_name}_{engine}"

        print(f"  {colorize('▸', Colors.OKBLUE)} {case_id:8s} {engine:5s} "
              f"rows={rows:,} dup={duplicates:6s} sel={selectivity:5.2f} "
              f"{'warm' if warm else 'cold':4s}", end='', flush=True)

        # Drop existing
        run_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

        # Create and populate table
        # Sort by key for RLE clustering when testing compression
        order_clause = "ORDER BY 1, 2, 3" if duplicates in ['dup50', 'dup100', 'dup10k', 'dup1k'] else ""
        create_sql = f"""
        CREATE UNLOGGED TABLE {table_name}(k int4, inc1 int4, inc2 int4);
        INSERT INTO {table_name}
        SELECT
            CASE WHEN '{duplicates}' = 'unique' THEN i
                 WHEN '{duplicates}' = 'zipf' THEN (i % ({rows}/100))::int4
                 WHEN '{duplicates}' = 'dup10k' THEN (i % 10000)::int4
                 WHEN '{duplicates}' = 'dup100' THEN (i % 100)::int4
                 WHEN '{duplicates}' = 'dup50' THEN (i % 50)::int4
                 ELSE (i % 1000)::int4
            END,
            (i % 10000)::int4,
            (i % 10000)::int4
        FROM generate_series(1, {rows}) i
        {order_clause};
        ALTER TABLE {table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {table_name};
        """
        run_sql(create_sql)

        # Build index
        inc_clause = f" INCLUDE (inc1, inc2)" if includes > 0 else ""
        create_idx_sql = f"CREATE INDEX {idx_name} ON {table_name} USING {engine}(k){inc_clause};"
        build_ms = run_sql_timing(create_idx_sql)

        # Get index size
        size_result = run_sql(f"SELECT pg_relation_size('{idx_name}');")
        if not size_result or not size_result.strip():
            print(f"\n{colorize('✗ Error:', Colors.FAIL)} Index '{idx_name}' does not exist or query failed")
            return None
        size_bytes = int(size_result)
        size_mb = size_bytes / (1024 * 1024)

        # Configure query
        run_sql(f"SET max_parallel_workers_per_gather = {workers};")
        run_sql("SET enable_seqscan = off;")
        run_sql("SET enable_bitmapscan = off;")
        run_sql("SET enable_indexonlyscan = on;")

        # Warm cache if requested, otherwise flush caches for cold test
        threshold = int(rows * (1 - selectivity))
        if warm:
            run_sql(f"SELECT count(*) FROM {table_name} WHERE k >= {threshold};")
        else:
            # Cold test: flush caches
            cache_flushed = flush_caches()
            if not cache_flushed:
                print(f"\n{colorize('⚠', Colors.WARNING)} Could not flush OS cache (need sudo), results may show warm cache")

        # Run query with EXPLAIN
        query_sql = f"""
        EXPLAIN (ANALYZE, TIMING ON, BUFFERS ON, FORMAT JSON)
        SELECT {('inc1, inc2, ' if includes > 0 else '')} count(*)
        FROM {table_name}
        WHERE k >= {threshold};
        """

        try:
            explain_json = run_sql(query_sql)
            if not explain_json or not explain_json.strip():
                raise ValueError("Empty EXPLAIN output")
            stats = parse_explain_json(explain_json)
        except (json.JSONDecodeError, ValueError) as e:
            # Fallback: run simple timing
            stats = {
                'exec_time': run_sql_timing(
                    f"SELECT {('inc1, inc2, ' if includes > 0 else '')} count(*) FROM {table_name} WHERE k >= {threshold};"
                ),
                'plan_time': 0,
                'buffers': {'shared_hit': 0, 'shared_read': 0, 'shared_written': 0},
                'rows': 0
            }

        # Calculate speedup if this is a comparison pair
        speedup_str = ""
        if len(self.results) > 0 and self.results[-1]['case_id'] == case_id:
            prev = self.results[-1]
            speedup = prev['exec_time'] / stats['exec_time'] if stats['exec_time'] > 0 else 0
            if speedup > 1.1:
                speedup_str = colorize(f" ({speedup:.1f}x faster)", Colors.OKGREEN)
            elif speedup < 0.9:
                speedup_str = colorize(f" ({1/speedup:.1f}x slower)", Colors.FAIL)

        print(f" → {stats['exec_time']:7.1f}ms  {size_mb:6.1f}MB{speedup_str}")

        # Record result
        self.results.append({
            'case_id': case_id,
            'engine': engine,
            'rows': rows,
            'duplicates': duplicates,
            'selectivity': selectivity,
            'includes': includes,
            'warm': warm,
            'workers': workers,
            'build_ms': build_ms,
            'size_mb': size_mb,
            'exec_time': stats['exec_time'],
            'plan_time': stats['plan_time'],
            'shared_read': stats['buffers']['shared_read'],
            'shared_hit': stats['buffers']['shared_hit'],
        })

        # Cleanup
        run_sql(f"DROP TABLE {table_name} CASCADE;")

    def run_case_type(self, case_id, engine, dtype, rows, distinct_values, warm):
        """Run benchmark for a specific integer type with RLE-friendly data"""
        safe_case_id = case_id.replace('.', '_').replace('-', '_')
        table_name = f"bench_{safe_case_id}"
        idx_name = f"{table_name}_{engine}"

        print(f"  {colorize('▸', Colors.OKBLUE)} {case_id:8s} {engine:5s} "
              f"dtype={dtype} rows={rows:,} distinct={distinct_values} "
              f"{'warm' if warm else 'cold':4s}", end='', flush=True)

        # Drop existing
        run_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

        # Create and populate table with duplicates (sorted for RLE)
        create_sql = f"""
        CREATE UNLOGGED TABLE {table_name}(k {dtype});
        INSERT INTO {table_name}
        SELECT (i % {distinct_values})::{dtype}
        FROM generate_series(1, {rows}) i
        ORDER BY 1;
        ALTER TABLE {table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {table_name};
        """
        run_sql(create_sql)

        # Build index
        create_idx_sql = f"CREATE INDEX {idx_name} ON {table_name} USING {engine}(k);"
        build_ms = run_sql_timing(create_idx_sql)

        # Get index size
        size_result = run_sql(f"SELECT pg_relation_size('{idx_name}');")
        if not size_result or not size_result.strip():
            print(f"\n{colorize('✗ Error:', Colors.FAIL)} Index '{idx_name}' does not exist")
            return None
        size_bytes = int(size_result)
        size_mb = size_bytes / (1024 * 1024)

        # Configure query
        run_sql(f"SET max_parallel_workers_per_gather = 0;")
        run_sql("SET enable_seqscan = off;")
        run_sql("SET enable_bitmapscan = off;")
        run_sql("SET enable_indexonlyscan = on;")

        # Warm cache if requested
        threshold = distinct_values // 2
        if warm:
            run_sql(f"SELECT count(*) FROM {table_name} WHERE k >= {threshold};")

        # Run query with EXPLAIN
        query_sql = f"""
        EXPLAIN (ANALYZE, TIMING ON, BUFFERS ON, FORMAT JSON)
        SELECT count(*)
        FROM {table_name}
        WHERE k >= {threshold};
        """

        try:
            explain_json = run_sql(query_sql)
            stats = parse_explain_json(explain_json)
        except (json.JSONDecodeError, ValueError):
            stats = {
                'exec_time': run_sql_timing(f"SELECT count(*) FROM {table_name} WHERE k >= {threshold};"),
                'plan_time': 0,
                'buffers': {'shared_hit': 0, 'shared_read': 0, 'shared_written': 0},
                'rows': 0
            }

        # Calculate speedup
        speedup_str = ""
        if len(self.results) > 0 and self.results[-1]['case_id'] == case_id:
            prev = self.results[-1]
            speedup = prev['exec_time'] / stats['exec_time'] if stats['exec_time'] > 0 else 0
            if speedup > 1.1:
                speedup_str = colorize(f" ({speedup:.1f}x faster)", Colors.OKGREEN)
            elif speedup < 0.9:
                speedup_str = colorize(f" ({1/speedup:.1f}x slower)", Colors.FAIL)

        print(f" → {stats['exec_time']:7.1f}ms  {size_mb:6.2f}MB{speedup_str}")

        # Record result
        self.results.append({
            'case_id': case_id,
            'engine': engine,
            'rows': rows,
            'duplicates': f'{distinct_values}_distinct',
            'selectivity': 0.5,
            'includes': 0,
            'warm': warm,
            'workers': 0,
            'build_ms': build_ms,
            'size_mb': size_mb,
            'exec_time': stats['exec_time'],
            'plan_time': stats['plan_time'],
            'shared_read': stats['buffers']['shared_read'],
            'shared_hit': stats['buffers']['shared_hit'],
        })

        # Cleanup
        run_sql(f"DROP TABLE {table_name} CASCADE;")

    def run_case_text(self, case_id, engine, rows, distinct_values, warm):
        """Run benchmark for text type with RLE-friendly data"""
        safe_case_id = case_id.replace('.', '_').replace('-', '_')
        table_name = f"bench_{safe_case_id}"
        idx_name = f"{table_name}_{engine}"

        print(f"  {colorize('▸', Colors.OKBLUE)} {case_id:8s} {engine:5s} "
              f"dtype=text rows={rows:,} distinct={distinct_values} "
              f"{'warm' if warm else 'cold':4s}", end='', flush=True)

        # Drop existing
        run_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

        # Generate distinct text values
        text_values = ['admin', 'client', 'guest', 'user', 'manager',
                       'operator', 'viewer', 'editor', 'owner', 'member'][:distinct_values]

        # Create and populate table with duplicates (sorted for RLE)
        create_sql = f"""
        CREATE UNLOGGED TABLE {table_name}(k text COLLATE "C");
        INSERT INTO {table_name}
        SELECT CASE (i % {distinct_values})
        """
        for i, val in enumerate(text_values):
            create_sql += f"  WHEN {i} THEN '{val}'\n"
        create_sql += f"""  ELSE 'unknown'
        END
        FROM generate_series(1, {rows}) i
        ORDER BY 1;
        ALTER TABLE {table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {table_name};
        """
        run_sql(create_sql)

        # Build index
        create_idx_sql = f"CREATE INDEX {idx_name} ON {table_name} USING {engine}(k);"
        build_ms = run_sql_timing(create_idx_sql)

        # Get index size
        size_result = run_sql(f"SELECT pg_relation_size('{idx_name}');")
        if not size_result or not size_result.strip():
            print(f"\n{colorize('✗ Error:', Colors.FAIL)} Index '{idx_name}' does not exist")
            return None
        size_bytes = int(size_result)
        size_mb = size_bytes / (1024 * 1024)

        # Configure query
        run_sql(f"SET max_parallel_workers_per_gather = 0;")
        run_sql("SET enable_seqscan = off;")
        run_sql("SET enable_bitmapscan = off;")
        run_sql("SET enable_indexonlyscan = on;")

        # Warm cache if requested
        threshold_val = text_values[distinct_values // 2] if distinct_values > 1 else text_values[0]
        if warm:
            run_sql(f"SELECT count(*) FROM {table_name} WHERE k >= '{threshold_val}';")

        # Run query with EXPLAIN
        query_sql = f"""
        EXPLAIN (ANALYZE, TIMING ON, BUFFERS ON, FORMAT JSON)
        SELECT count(*)
        FROM {table_name}
        WHERE k >= '{threshold_val}';
        """

        try:
            explain_json = run_sql(query_sql)
            stats = parse_explain_json(explain_json)
        except (json.JSONDecodeError, ValueError):
            stats = {
                'exec_time': run_sql_timing(f"SELECT count(*) FROM {table_name} WHERE k >= '{threshold_val}';"),
                'plan_time': 0,
                'buffers': {'shared_hit': 0, 'shared_read': 0, 'shared_written': 0},
                'rows': 0
            }

        # Calculate speedup
        speedup_str = ""
        if len(self.results) > 0 and self.results[-1]['case_id'] == case_id:
            prev = self.results[-1]
            speedup = prev['exec_time'] / stats['exec_time'] if stats['exec_time'] > 0 else 0
            if speedup > 1.1:
                speedup_str = colorize(f" ({speedup:.1f}x faster)", Colors.OKGREEN)
            elif speedup < 0.9:
                speedup_str = colorize(f" ({1/speedup:.1f}x slower)", Colors.FAIL)

        print(f" → {stats['exec_time']:7.1f}ms  {size_mb:6.2f}MB{speedup_str}")

        # Record result
        self.results.append({
            'case_id': case_id,
            'engine': engine,
            'rows': rows,
            'duplicates': f'{distinct_values}_distinct',
            'selectivity': 0.5,
            'includes': 0,
            'warm': warm,
            'workers': 0,
            'build_ms': build_ms,
            'size_mb': size_mb,
            'exec_time': stats['exec_time'],
            'plan_time': stats['plan_time'],
            'shared_read': stats['buffers']['shared_read'],
            'shared_hit': stats['buffers']['shared_hit'],
        })

        # Cleanup
        run_sql(f"DROP TABLE {table_name} CASCADE;")

    def run_case_uuid(self, case_id, engine, rows, distinct_values, warm):
        """Run benchmark for UUID type with RLE-friendly data"""
        safe_case_id = case_id.replace('.', '_').replace('-', '_')
        table_name = f"bench_{safe_case_id}"
        idx_name = f"{table_name}_{engine}"

        print(f"  {colorize('▸', Colors.OKBLUE)} {case_id:8s} {engine:5s} "
              f"dtype=uuid rows={rows:,} distinct={distinct_values} "
              f"{'warm' if warm else 'cold':4s}", end='', flush=True)

        # Drop existing
        run_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

        # Create and populate table with duplicates (sorted for RLE)
        create_sql = f"""
        CREATE UNLOGGED TABLE {table_name}(k uuid);
        INSERT INTO {table_name}
        SELECT ('00000000-0000-0000-0000-' || lpad((i % {distinct_values})::text, 12, '0'))::uuid
        FROM generate_series(1, {rows}) i
        ORDER BY 1;
        ALTER TABLE {table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {table_name};
        """
        run_sql(create_sql)

        # Build index
        create_idx_sql = f"CREATE INDEX {idx_name} ON {table_name} USING {engine}(k);"
        build_ms = run_sql_timing(create_idx_sql)

        # Get index size
        size_result = run_sql(f"SELECT pg_relation_size('{idx_name}');")
        if not size_result or not size_result.strip():
            print(f"\n{colorize('✗ Error:', Colors.FAIL)} Index '{idx_name}' does not exist")
            return None
        size_bytes = int(size_result)
        size_mb = size_bytes / (1024 * 1024)

        # Configure query
        run_sql(f"SET max_parallel_workers_per_gather = 0;")
        run_sql("SET enable_seqscan = off;")
        run_sql("SET enable_bitmapscan = off;")
        run_sql("SET enable_indexonlyscan = on;")

        # Warm cache if requested
        threshold = distinct_values // 2
        threshold_uuid = f"'00000000-0000-0000-0000-{threshold:012d}'::uuid"
        if warm:
            run_sql(f"SELECT count(*) FROM {table_name} WHERE k >= {threshold_uuid};")

        # Run query with EXPLAIN
        query_sql = f"""
        EXPLAIN (ANALYZE, TIMING ON, BUFFERS ON, FORMAT JSON)
        SELECT count(*)
        FROM {table_name}
        WHERE k >= {threshold_uuid};
        """

        try:
            explain_json = run_sql(query_sql)
            stats = parse_explain_json(explain_json)
        except (json.JSONDecodeError, ValueError):
            stats = {
                'exec_time': run_sql_timing(f"SELECT count(*) FROM {table_name} WHERE k >= {threshold_uuid};"),
                'plan_time': 0,
                'buffers': {'shared_hit': 0, 'shared_read': 0, 'shared_written': 0},
                'rows': 0
            }

        # Calculate speedup
        speedup_str = ""
        if len(self.results) > 0 and self.results[-1]['case_id'] == case_id:
            prev = self.results[-1]
            speedup = prev['exec_time'] / stats['exec_time'] if stats['exec_time'] > 0 else 0
            if speedup > 1.1:
                speedup_str = colorize(f" ({speedup:.1f}x faster)", Colors.OKGREEN)
            elif speedup < 0.9:
                speedup_str = colorize(f" ({1/speedup:.1f}x slower)", Colors.FAIL)

        print(f" → {stats['exec_time']:7.1f}ms  {size_mb:6.2f}MB{speedup_str}")

        # Record result
        self.results.append({
            'case_id': case_id,
            'engine': engine,
            'rows': rows,
            'duplicates': f'{distinct_values}_distinct',
            'selectivity': 0.5,
            'includes': 0,
            'warm': warm,
            'workers': 0,
            'build_ms': build_ms,
            'size_mb': size_mb,
            'exec_time': stats['exec_time'],
            'plan_time': stats['plan_time'],
            'shared_read': stats['buffers']['shared_read'],
            'shared_hit': stats['buffers']['shared_hit'],
        })

        # Cleanup
        run_sql(f"DROP TABLE {table_name} CASCADE;")

    def run_quick(self):
        """Quick benchmark suite - basic comparison with multiple datatypes"""
        self.print_header("QUICK BENCHMARK SUITE - Multi-Datatype Tests")

        self.print_subheader("Test 1: int4 - Unique Keys, No INCLUDE (1M rows)")
        self.run_case('q1', 'btree', 1000000, 'unique', 0.5, 0, True)
        self.run_case('q1', 'smol', 1000000, 'unique', 0.5, 0, True)

        self.print_subheader("Test 2: int4 - Duplicate Keys with INCLUDE (1M rows, 1000 distinct)")
        self.run_case('q2', 'btree', 1000000, 'dup1k', 0.5, 2, True)
        self.run_case('q2', 'smol', 1000000, 'dup1k', 0.5, 2, True)

        self.print_subheader("Test 3: int4 - High Selectivity (return 50% of rows)")
        self.run_case('q3', 'btree', 1000000, 'unique', 0.5, 2, True)
        self.run_case('q3', 'smol', 1000000, 'unique', 0.5, 2, True)

        self.print_subheader("Test 4: int2 - RLE Compression (1M rows, 50 distinct)")
        self.run_case_type('q4', 'btree', 'int2', 1000000, 50, True)
        self.run_case_type('q4', 'smol', 'int2', 1000000, 50, True)

        self.print_subheader("Test 5: int8 - RLE Compression (1M rows, 100 distinct)")
        self.run_case_type('q5', 'btree', 'int8', 1000000, 100, True)
        self.run_case_type('q5', 'smol', 'int8', 1000000, 100, True)

        self.print_subheader("Test 6: text - RLE Compression (1M rows, 10 distinct strings)")
        self.run_case_text('q6', 'btree', 1000000, 10, True)
        self.run_case_text('q6', 'smol', 1000000, 10, True)

        self.print_subheader("Test 7: UUID - RLE Compression (1M rows, 20 distinct UUIDs)")
        self.run_case_uuid('q7', 'btree', 1000000, 20, True)
        self.run_case_uuid('q7', 'smol', 1000000, 20, True)

    def run_full(self):
        """Full comprehensive benchmark"""
        self.print_header("FULL BENCHMARK SUITE")

        # Test different row counts
        for rows in [100000, 1000000, 5000000]:
            self.print_subheader(f"Dataset: {rows:,} rows")
            self.run_case(f'r{rows}', 'btree', rows, 'unique', 0.1, 2, True)
            self.run_case(f'r{rows}', 'smol', rows, 'unique', 0.1, 2, True)

        # Test different selectivities
        self.print_subheader("Selectivity Tests (1M rows, unique keys)")
        for sel in [0.001, 0.01, 0.1, 0.5, 1.0]:
            result_btree = self.run_case(f's{sel}', 'btree', 1000000, 'unique', sel, 2, True)
            result_smol = self.run_case(f's{sel}', 'smol', 1000000, 'unique', sel, 2, True)
            if result_btree is None or result_smol is None:
                print(f"{colorize(f'⚠ Skipping selectivity test for sel={sel}', Colors.WARNING)}")
                continue

        # Test duplicate patterns
        self.print_subheader("Duplicate Pattern Tests (1M rows)")
        for dup in ['unique', 'dup1k', 'zipf']:
            self.run_case(f'd{dup}', 'btree', 1000000, dup, 0.5, 2, True)
            self.run_case(f'd{dup}', 'smol', 1000000, dup, 0.5, 2, True)

    def run_thrash(self):
        """Thrashing test - demonstrates cache efficiency with cold queries"""
        self.print_header("THRASHING TEST (demonstrates cache efficiency with COLD queries)")

        # Check shared_buffers setting
        shared_buffers = run_sql("SHOW shared_buffers;")
        print(f"\n{colorize('ℹ', Colors.WARNING)}  Current shared_buffers: {shared_buffers}")
        print(f"{colorize('ℹ', Colors.WARNING)}  This test uses large indexes and COLD (different) queries")
        print(f"{colorize('ℹ', Colors.WARNING)}  BTREE: ~600MB (9.4x larger than cache)")
        print(f"{colorize('ℹ', Colors.WARNING)}  SMOL: ~5MB (easily fits in cache)")
        print(f"{colorize('ℹ', Colors.WARNING)}  Expected: SMOL faster because whole index fits in cache\n")

        # 20M rows with 50 distinct keys = 400K rows per key (extreme RLE compression for SMOL)
        # BTREE: ~600MB index (9.4x larger than 64MB cache → cannot fit)
        # SMOL with RLE: ~5MB (easily fits in 64MB shared_buffers)
        # Use 5 different queries accessing different ranges - BTREE must constantly evict/refetch
        self.print_subheader("20M rows, 50 distinct keys - 5 different cold queries")

        # Build dataset and indexes
        table_name = "bench_thrash"
        run_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

        print(f"Creating table with 20M rows...")
        create_sql = f"""
        CREATE UNLOGGED TABLE {table_name}(k int4, inc1 int4, inc2 int4);
        INSERT INTO {table_name}
        SELECT (i % 50)::int4, (i % 10000)::int4, (i % 10000)::int4
        FROM generate_series(1, 20000000) i
        ORDER BY 1, 2, 3;
        ALTER TABLE {table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {table_name};
        """
        run_sql(create_sql)

        # 5 different queries accessing different key ranges (cold queries)
        # This forces BTREE to evict/refetch pages since working set >> cache
        queries = [
            f"SELECT inc1, inc2, count(*) FROM {table_name} WHERE k BETWEEN 0 AND 9 GROUP BY 1, 2;",
            f"SELECT inc1, inc2, count(*) FROM {table_name} WHERE k BETWEEN 10 AND 19 GROUP BY 1, 2;",
            f"SELECT inc1, inc2, count(*) FROM {table_name} WHERE k BETWEEN 20 AND 29 GROUP BY 1, 2;",
            f"SELECT inc1, inc2, count(*) FROM {table_name} WHERE k BETWEEN 30 AND 39 GROUP BY 1, 2;",
            f"SELECT inc1, inc2, count(*) FROM {table_name} WHERE k BETWEEN 40 AND 49 GROUP BY 1, 2;",
        ]
        query_settings = "SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET max_parallel_workers_per_gather=0;"

        # Build BTREE
        print(f"Building BTREE index...")
        run_sql(f"CREATE INDEX {table_name}_btree ON {table_name}(k) INCLUDE (inc1, inc2);")
        btree_size = int(run_sql(f"SELECT pg_relation_size('{table_name}_btree');"))
        btree_idx_name = f"{table_name}_btree"

        # Test BTREE with 5 cold queries
        print(f"\n{colorize('Testing BTREE:', Colors.BOLD)} 5 cold queries accessing different ranges")
        flush_caches(btree_idx_name)

        btree_total_ms = 0
        for i, query in enumerate(queries):
            start = time.time()
            run_sql(query_settings + query)
            elapsed = (time.time() - start) * 1000
            btree_total_ms += elapsed
            print(f"  Query {i+1}: {elapsed:.1f}ms")

        # Get buffer stats from last query
        btree_explain = run_sql(query_settings + 'EXPLAIN (buffers, analyze) ' + queries[0])
        btree_buffers = parse_explain_buffers(btree_explain)

        print(f"  BTREE total: {colorize(f'{btree_total_ms:.1f}ms', Colors.WARNING)} (avg {btree_total_ms/len(queries):.1f}ms/query)")
        print(f"  BTREE size: {btree_size/(1024*1024):.1f}MB")

        # Build SMOL
        run_sql(f"DROP INDEX {table_name}_btree;")
        print(f"\n{colorize('Building SMOL index...', Colors.BOLD)}")
        run_sql(f"CREATE INDEX {table_name}_smol ON {table_name} USING smol(k) INCLUDE (inc1, inc2);")
        smol_size = int(run_sql(f"SELECT pg_relation_size('{table_name}_smol');"))
        smol_idx_name = f"{table_name}_smol"

        # Test SMOL with 5 cold queries
        print(f"\n{colorize('Testing SMOL:', Colors.BOLD)} 5 cold queries accessing different ranges")
        flush_caches(smol_idx_name)

        smol_total_ms = 0
        for i, query in enumerate(queries):
            start = time.time()
            run_sql(query_settings + query)
            elapsed = (time.time() - start) * 1000
            smol_total_ms += elapsed
            print(f"  Query {i+1}: {elapsed:.1f}ms")

        # Get buffer stats from last query
        smol_explain = run_sql(query_settings + 'EXPLAIN (buffers, analyze) ' + queries[0])
        smol_buffers = parse_explain_buffers(smol_explain)

        print(f"  SMOL total: {colorize(f'{smol_total_ms:.1f}ms', Colors.OKGREEN)} (avg {smol_total_ms/len(queries):.1f}ms/query)")
        print(f"  SMOL size: {smol_size/(1024*1024):.1f}MB")
        print(f"  SMOL inspect: {run_sql(f'SELECT smol_inspect(\'{smol_idx_name}\')')}")

        # Calculate speedup
        speedup = btree_total_ms / smol_total_ms if smol_total_ms > 0 else 0
        compression = btree_size / smol_size if smol_size > 0 else 0

        speedup_color = Colors.OKGREEN if speedup > 1 else Colors.FAIL

        print(f"\n{colorize('Result:', Colors.BOLD)}")
        print(f"  Speedup: {colorize(f'{speedup:.2f}x', speedup_color)}")
        print(f"  Compression: {colorize(f'{compression:.1f}x', Colors.OKBLUE)}")
        print(f"  Performance gain: {colorize(f'{btree_total_ms - smol_total_ms:.0f}ms saved', Colors.OKGREEN if speedup > 1 else Colors.FAIL)} ({((speedup - 1) * 100):.0f}% faster)")

        # Display cache efficiency statistics
        if btree_buffers['shared_hit'] > 0 and smol_buffers['shared_hit'] > 0:
            buffer_ratio = btree_buffers['shared_hit'] / smol_buffers['shared_hit'] if smol_buffers['shared_hit'] > 0 else 0
            btree_pages_mb = btree_buffers['shared_hit'] * 8 / 1024  # 8KB pages to MB
            smol_pages_mb = smol_buffers['shared_hit'] * 8 / 1024

            print(f"\n{colorize('Cache Efficiency (per query):', Colors.BOLD)}")
            btree_hit_str = f"{btree_buffers['shared_hit']:,}"
            smol_hit_str = f"{smol_buffers['shared_hit']:,}"
            print(f"  BTREE: {colorize(btree_hit_str, Colors.WARNING)} buffer hits ({btree_pages_mb:.1f}MB of pages accessed)")
            print(f"  SMOL:  {colorize(smol_hit_str, Colors.OKGREEN)} buffer hits ({smol_pages_mb:.1f}MB of pages accessed)")
            ratio_str = f'→ SMOL uses {buffer_ratio:.0f}x fewer cache pages per query!'
            print(f"  {colorize(ratio_str, Colors.OKBLUE)}")

        # Record results
        self.results.append({
            'case_id': 'thrash',
            'engine': 'btree',
            'rows': 20000000,
            'duplicates': 'dup50',
            'selectivity': 0.2,
            'includes': 2,
            'warm': False,
            'workers': 0,
            'build_ms': 0,
            'size_mb': btree_size / (1024*1024),
            'exec_time': btree_total_ms / len(queries),  # avg per query
            'plan_time': 0,
            'shared_read': 0,
            'shared_hit': 0,
        })

        self.results.append({
            'case_id': 'thrash',
            'engine': 'smol',
            'rows': 20000000,
            'duplicates': 'dup50',
            'selectivity': 0.2,
            'includes': 2,
            'warm': False,
            'workers': 0,
            'build_ms': 0,
            'size_mb': smol_size / (1024*1024),
            'exec_time': smol_total_ms / len(queries),  # avg per query
            'plan_time': 0,
            'shared_read': 0,
            'shared_hit': 0,
        })

    def run_thrash_repeated(self):
        """Repeated query test - demonstrates cache warming benefits"""
        self.print_header("THRASHING TEST - REPEATED QUERIES (demonstrates cache warming)")

        # Check shared_buffers setting
        shared_buffers = run_sql("SHOW shared_buffers;")
        print(f"\n{colorize('ℹ', Colors.WARNING)}  Current shared_buffers: {shared_buffers}")
        print(f"{colorize('ℹ', Colors.WARNING)}  This test runs the SAME query 10 times to show cache effects")
        print(f"{colorize('ℹ', Colors.WARNING)}  Expected: SMOL warms up faster (fits in cache), BTREE stays slow (thrashes)\n")

        # Same setup as regular thrash
        self.print_subheader("5M rows, 50 distinct keys (100K rows per key, extreme RLE compression)")

        # Build dataset and indexes
        table_name = "bench_thrash_rep"
        run_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

        create_sql = f"""
        CREATE UNLOGGED TABLE {table_name}(k int4, inc1 int4, inc2 int4);
        INSERT INTO {table_name}
        SELECT (i % 50)::int4, (i % 10000)::int4, (i % 10000)::int4
        FROM generate_series(1, 5000000) i
        ORDER BY 1, 2, 3;
        ALTER TABLE {table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {table_name};
        """
        runs = 10  # 10 iterations to show cache warming
        query_settings = f"SET enable_seqscan=off; SET enable_bitmapscan=off; SET enable_indexonlyscan=on; SET max_parallel_workers_per_gather=0;"
        query = f"SELECT inc1, inc2, count(*) FROM {table_name} WHERE k <= 20 GROUP BY 1, 2;"

        print(f"creating table {table_name}...")
        run_sql(create_sql)
        print(f"testing with query {query}\n")

        # Build BTREE
        run_sql(f"CREATE INDEX {table_name}_btree ON {table_name}(k) INCLUDE (inc1, inc2);")
        btree_size = int(run_sql(f"SELECT pg_relation_size('{table_name}_btree');"))

        # Test BTREE with repeated queries
        print(f"{colorize('Testing BTREE:', Colors.BOLD)} {runs} iterations of same query (cold start)")
        flush_caches(f"{table_name}_btree")

        btree_times = []
        for i in range(runs):
            start = time.time()
            run_sql(query_settings + query)
            elapsed = (time.time() - start) * 1000
            btree_times.append(elapsed)

        btree_avg = sum(btree_times) / len(btree_times)
        btree_first = btree_times[0]
        btree_warm_avg = sum(btree_times[1:]) / len(btree_times[1:])

        print(f"  BTREE times: [{', '.join([f'{t:.0f}ms' for t in btree_times])}]")
        print(f"  First (cold): {colorize(f'{btree_first:.1f}ms', Colors.WARNING)}")
        print(f"  Avg 2-10 (warm): {colorize(f'{btree_warm_avg:.1f}ms', Colors.WARNING)}")

        # Build SMOL
        run_sql(f"DROP INDEX {table_name}_btree;")
        run_sql(f"CREATE INDEX {table_name}_smol ON {table_name} USING smol(k) INCLUDE (inc1, inc2);")
        smol_size = int(run_sql(f"SELECT pg_relation_size('{table_name}_smol');"))

        # Test SMOL with repeated queries
        print(f"\n{colorize('Testing SMOL:', Colors.BOLD)} {runs} iterations of same query (cold start)")
        flush_caches(f"{table_name}_smol")

        smol_times = []
        for i in range(runs):
            start = time.time()
            run_sql(query_settings + query)
            elapsed = (time.time() - start) * 1000
            smol_times.append(elapsed)

        smol_avg = sum(smol_times) / len(smol_times)
        smol_first = smol_times[0]
        smol_warm_avg = sum(smol_times[1:]) / len(smol_times[1:])

        print(f"  SMOL times: [{', '.join([f'{t:.0f}ms' for t in smol_times])}]")
        print(f"  First (cold): {colorize(f'{smol_first:.1f}ms', Colors.WARNING)}")
        print(f"  Avg 2-10 (warm): {colorize(f'{smol_warm_avg:.1f}ms', Colors.OKGREEN)}")

        # Calculate metrics
        cold_speedup = btree_first / smol_first if smol_first > 0 else 0
        warm_speedup = btree_warm_avg / smol_warm_avg if smol_warm_avg > 0 else 0
        compression = btree_size / smol_size if smol_size > 0 else 0

        cold_color = Colors.OKGREEN if cold_speedup > 1 else Colors.FAIL
        warm_color = Colors.OKGREEN if warm_speedup > 1 else Colors.FAIL

        print(f"\n{colorize('Result:', Colors.BOLD)}")
        print(f"  Cold (iteration 1): BTREE {btree_first:.1f}ms, SMOL {smol_first:.1f}ms → {colorize(f'{cold_speedup:.2f}x', cold_color)}")
        print(f"  Warm (avg 2-10): BTREE {btree_warm_avg:.1f}ms, SMOL {smol_warm_avg:.1f}ms → {colorize(f'{warm_speedup:.2f}x', warm_color)}")
        print(f"  Compression: {colorize(f'{compression:.1f}x', Colors.OKBLUE)}")
        print(f"\n  {colorize('Cache warming effect:', Colors.BOLD)}")
        print(f"  BTREE: {btree_first:.1f}ms → {btree_warm_avg:.1f}ms ({((btree_warm_avg/btree_first)*100):.0f}% of cold)")
        print(f"  SMOL:  {smol_first:.1f}ms → {smol_warm_avg:.1f}ms ({((smol_warm_avg/smol_first)*100):.0f}% of cold)")

        # Record results
        self.results.append({
            'case_id': 'thrash_rep_cold',
            'engine': 'btree',
            'rows': 5000000,
            'duplicates': 'dup50',
            'selectivity': 0.42,
            'includes': 2,
            'warm': False,
            'workers': 0,
            'build_ms': 0,
            'size_mb': btree_size / (1024*1024),
            'exec_time': btree_first,
            'plan_time': 0,
            'shared_read': 0,
            'shared_hit': 0,
        })

        self.results.append({
            'case_id': 'thrash_rep_cold',
            'engine': 'smol',
            'rows': 5000000,
            'duplicates': 'dup50',
            'selectivity': 0.42,
            'includes': 2,
            'warm': False,
            'workers': 0,
            'build_ms': 0,
            'size_mb': smol_size / (1024*1024),
            'exec_time': smol_first,
            'plan_time': 0,
            'shared_read': 0,
            'shared_hit': 0,
        })

        self.results.append({
            'case_id': 'thrash_rep_warm',
            'engine': 'btree',
            'rows': 5000000,
            'duplicates': 'dup50',
            'selectivity': 0.42,
            'includes': 2,
            'warm': True,
            'workers': 0,
            'build_ms': 0,
            'size_mb': btree_size / (1024*1024),
            'exec_time': btree_warm_avg,
            'plan_time': 0,
            'shared_read': 0,
            'shared_hit': 0,
        })

        self.results.append({
            'case_id': 'thrash_rep_warm',
            'engine': 'smol',
            'rows': 5000000,
            'duplicates': 'dup50',
            'selectivity': 0.42,
            'includes': 2,
            'warm': True,
            'workers': 0,
            'build_ms': 0,
            'size_mb': smol_size / (1024*1024),
            'exec_time': smol_warm_avg,
            'plan_time': 0,
            'shared_read': 0,
            'shared_hit': 0,
        })

    def save_results(self):
        """Save results to CSV and generate report"""
        csv_file = self.results_dir / f"{self.mode}-{self.timestamp}.csv"

        with open(csv_file, 'w', newline='') as f:
            if self.results:
                writer = csv.DictWriter(f, fieldnames=self.results[0].keys())
                writer.writeheader()
                writer.writerows(self.results)

        print(f"\n{colorize('✓', Colors.OKGREEN)} Results saved to: {colorize(str(csv_file), Colors.BOLD)}")

        # Generate summary
        self.print_summary()

    def print_summary(self):
        """Print summary statistics"""
        self.print_header("BENCHMARK SUMMARY")

        # Group by case_id
        by_case = defaultdict(list)
        for r in self.results:
            by_case[r['case_id']].append(r)

        print(f"\n{'Case':<10} {'Index':<8} {'Exec Time':<12} {'Size':<10} {'Speedup':<10}")
        print(f"{colorize('─' * 60, Colors.OKCYAN)}")

        for case_id in sorted(by_case.keys()):
            engines = by_case[case_id]

            # Find btree and smol results
            btree = next((e for e in engines if e['engine'] == 'btree'), None)
            smol = next((e for e in engines if e['engine'] == 'smol'), None)

            if btree and smol:
                speedup = btree['exec_time'] / smol['exec_time'] if smol['exec_time'] > 0 else 0
                compression = btree['size_mb'] / smol['size_mb'] if smol['size_mb'] > 0 else 0

                speedup_color = Colors.OKGREEN if speedup > 1 else Colors.FAIL

                print(f"{case_id:<10} {'BTREE':<8} {btree['exec_time']:>8.1f} ms  {btree['size_mb']:>6.1f} MB")
                print(f"{'':<10} {'SMOL':<8} {smol['exec_time']:>8.1f} ms  {smol['size_mb']:>6.1f} MB  "
                      f"{colorize(f'{speedup:.2f}x', speedup_color)} {colorize(f'{compression:.1f}x smaller', Colors.OKBLUE)}")
                print()

def main():
    parser = argparse.ArgumentParser(description='SMOL Benchmark Runner')
    parser.add_argument('--quick', action='store_true', help='Run quick benchmark suite')
    parser.add_argument('--full', action='store_true', help='Run full comprehensive suite')
    parser.add_argument('--thrash', action='store_true', help='Run thrashing test')
    parser.add_argument('--thrash-repeated', action='store_true', help='Run repeated query thrashing test (demonstrates cache warming)')
    parser.add_argument('--repeats', type=int, default=1, help='Number of repetitions')

    args = parser.parse_args()

    # Determine mode
    if args.full:
        mode = 'full'
    elif args.thrash:
        mode = 'thrash'
    elif args.thrash_repeated:
        mode = 'thrash_repeated'
    else:
        mode = 'quick'

    bench = Benchmark(mode=mode, repeats=args.repeats)

    try:
        if mode == 'quick':
            bench.run_quick()
        elif mode == 'full':
            bench.run_full()
        elif mode == 'thrash':
            bench.run_thrash()
        elif mode == 'thrash_repeated':
            bench.run_thrash_repeated()

        bench.save_results()

    except Exception as e:
        print(f"\n{colorize('✗ Error:', Colors.FAIL)} {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
