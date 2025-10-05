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

        # Create table
        table_name = f"bench_{case_id}"
        idx_name = f"{table_name}_{engine}"

        print(f"  {colorize('▸', Colors.OKBLUE)} {case_id:8s} {engine:5s} "
              f"rows={rows:,} dup={duplicates:6s} sel={selectivity:5.2f} "
              f"{'warm' if warm else 'cold':4s}", end='', flush=True)

        # Drop existing
        run_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE;")

        # Create and populate table
        create_sql = f"""
        CREATE UNLOGGED TABLE {table_name}(k int4, inc1 int4, inc2 int4);
        INSERT INTO {table_name}
        SELECT
            CASE WHEN '{duplicates}' = 'unique' THEN i
                 WHEN '{duplicates}' = 'zipf' THEN (i % ({rows}/100))::int4
                 ELSE (i % 1000)::int4
            END,
            (i % 10000)::int4,
            (i % 10000)::int4
        FROM generate_series(1, {rows}) i;
        ALTER TABLE {table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {table_name};
        """
        run_sql(create_sql)

        # Build index
        inc_clause = f" INCLUDE (inc1, inc2)" if includes > 0 else ""
        create_idx_sql = f"CREATE INDEX {idx_name} ON {table_name} USING {engine}(k){inc_clause};"
        build_ms = run_sql_timing(create_idx_sql)

        # Get index size
        size_bytes = int(run_sql(f"SELECT pg_relation_size('{idx_name}');"))
        size_mb = size_bytes / (1024 * 1024)

        # Configure query
        run_sql(f"SET max_parallel_workers_per_gather = {workers};")
        run_sql("SET enable_seqscan = off;")
        run_sql("SET enable_bitmapscan = off;")
        run_sql("SET enable_indexonlyscan = on;")

        # Warm cache if requested
        threshold = int(rows * (1 - selectivity))
        if warm:
            run_sql(f"SELECT count(*) FROM {table_name} WHERE k >= {threshold};")

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

    def run_quick(self):
        """Quick benchmark suite - basic comparison"""
        self.print_header("QUICK BENCHMARK SUITE")

        self.print_subheader("Test 1: Unique Keys, No INCLUDE (1M rows)")
        self.run_case('q1', 'btree', 1000000, 'unique', 0.5, 0, True)
        self.run_case('q1', 'smol', 1000000, 'unique', 0.5, 0, True)

        self.print_subheader("Test 2: Duplicate Keys with INCLUDE (1M rows, 1000 distinct)")
        self.run_case('q2', 'btree', 1000000, 'dup1k', 0.5, 2, True)
        self.run_case('q2', 'smol', 1000000, 'dup1k', 0.5, 2, True)

        self.print_subheader("Test 3: High Selectivity (return 50% of rows)")
        self.run_case('q3', 'btree', 1000000, 'unique', 0.5, 2, True)
        self.run_case('q3', 'smol', 1000000, 'unique', 0.5, 2, True)

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
            self.run_case(f's{sel}', 'btree', 1000000, 'unique', sel, 2, True)
            self.run_case(f's{sel}', 'smol', 1000000, 'unique', sel, 2, True)

        # Test duplicate patterns
        self.print_subheader("Duplicate Pattern Tests (1M rows)")
        for dup in ['unique', 'dup1k', 'zipf']:
            self.run_case(f'd{dup}', 'btree', 1000000, dup, 0.5, 2, True)
            self.run_case(f'd{dup}', 'smol', 1000000, dup, 0.5, 2, True)

    def run_thrash(self):
        """Thrashing test - demonstrates cache efficiency"""
        self.print_header("THRASHING TEST (demonstrates cache efficiency)")

        print(f"\n{colorize('ℹ', Colors.WARNING)}  This test uses large datasets that don't fit in shared_buffers")
        print(f"{colorize('ℹ', Colors.WARNING)}  Expected: SMOL has fewer buffer reads due to smaller index size\n")

        # Large dataset with many duplicates
        self.print_subheader("20M rows, 10K distinct keys (high compression opportunity)")
        self.run_case('t1', 'btree', 20000000, 'dup10k', 0.1, 2, False)
        self.run_case('t1', 'smol', 20000000, 'dup10k', 0.1, 2, False)

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
    parser.add_argument('--repeats', type=int, default=1, help='Number of repetitions')

    args = parser.parse_args()

    # Determine mode
    if args.full:
        mode = 'full'
    elif args.thrash:
        mode = 'thrash'
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

        bench.save_results()

    except Exception as e:
        print(f"\n{colorize('✗ Error:', Colors.FAIL)} {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
