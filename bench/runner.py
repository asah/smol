#!/usr/bin/env python3
"""
SMOL Benchmark Suite v2 - Main Runner
Comprehensive, auto-scaling benchmark suite for SMOL vs BTREE comparison
"""

import argparse
import sys
import os
from datetime import datetime

# Add bench directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench.lib.db import DatabaseConnection, PostgresEnvironment
from bench.lib.cache import CacheController
from bench.lib.reporting import ReportGenerator
from bench.lib.regression import RegressionDetector

from bench.workloads.timeseries import TimeSeriesWorkload
from bench.workloads.dimension import DimensionWorkload
from bench.workloads.events import EventStreamWorkload
from bench.workloads.sparse import SparseWorkload
from bench.workloads.composite import CompositeWorkload
from bench.workloads.textkeys import TextKeysWorkload
from bench.workloads.selectivity import SelectivityRangeWorkload
from bench.workloads.parallel import ParallelScalingWorkload
from bench.workloads.nulltest import NullRejectionWorkload
from bench.workloads.includeoverhead import IncludeOverheadWorkload
from bench.workloads.backward import BackwardScanWorkload
from bench.workloads.limits import LimitWorkload
from bench.workloads.inclause import InClauseWorkload
from bench.workloads.datatypes import DataTypesWorkload
from bench.workloads.indexonly import IndexOnlyScanWorkload
from bench.workloads.partial import PartialIndexWorkload


class BenchmarkRunner:
    """Main benchmark orchestrator"""

    def __init__(self, mode: str = 'quick'):
        self.mode = mode
        self.db = DatabaseConnection()
        self.env = PostgresEnvironment(self.db)
        self.cache = CacheController(self.db)
        self.results = []
        self.timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

    def print_header(self):
        """Print banner"""
        scale_params = self.env.get_scale_params()

        print("╔══════════════════════════════════════════════════════════╗")
        print(f"║  SMOL Benchmark Suite v2 - {scale_params['name']:^30}  ║")
        print("╚══════════════════════════════════════════════════════════╝\n")
        print(f"Environment:")
        print(f"  PostgreSQL: {self.env.pg_version}")
        print(f"  shared_buffers: {self.env.shared_buffers_mb:.0f} MB")
        print(f"  Test scale: {self.env.get_test_scale()}")
        print(f"  Max workers: {self.env.max_workers}")
        print(f"  Timeout: {scale_params['timeout_minutes']} minutes\n")

    def get_workloads(self):
        """Select workloads based on mode and scale"""
        scale_params = self.env.get_scale_params()
        workloads = []

        if self.mode == 'quick':
            # Quick mode: Comprehensive 30-second suite (18 workloads)
            rows = 1_000_000
            workers = min(4, self.env.max_workers)

            workloads = [
                # 1. Time-series with cold cache (3s) - I/O advantage
                TimeSeriesWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'metrics': 50, 'cache_modes': ['hot', 'cold'], 'parallelism': 0}
                ),

                # 2. Text/UUID keys (3s) - text32 zero-copy
                TextKeysWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'key_type': 'uuid', 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 3. UTF-8 text keys (2s) - ICU collation with strxfrm
                TextKeysWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'key_type': 'text_utf8', 'collation': 'en-US-x-icu', 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 4. Composite keys (3s) - multi-column RLE
                CompositeWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 5. Selectivity 0.1% (2s) - point query baseline
                SelectivityRangeWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'selectivity': 0.001, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 6. Selectivity 10% (2s) - large range scan
                SelectivityRangeWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'selectivity': 0.10, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 7. Parallel scaling 4 workers (2s) - parallelization advantage
                ParallelScalingWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'cache_modes': ['hot'], 'parallelism': workers}
                ),

                # 8. INCLUDE overhead 0 cols (1s) - baseline
                IncludeOverheadWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'n_includes': 0, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 9. INCLUDE overhead 4 cols (1s) - columnar advantage
                IncludeOverheadWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'n_includes': 4, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 10. RLE extreme (2s) - maximum compression
                TimeSeriesWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'metrics': 10, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 11. Backward scans (2s) - DESC optimization
                BackwardScanWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 12. LIMIT queries (2s) - early termination
                LimitWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 13. IN clause (2s) - multiple point lookups
                InClauseWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 14. Data types: int2 (2s) - small integer optimization
                DataTypesWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'data_type': 'int2', 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 15. Data types: int8 (2s) - large integer
                DataTypesWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'data_type': 'int8', 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 16. Data types: date (2s) - temporal data
                DataTypesWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'data_type': 'date', 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 17. Index-only scans (2s) - covering index
                IndexOnlyScanWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'cache_modes': ['hot'], 'parallelism': 0}
                ),

                # 18. Partial indexes (2s) - filtered index
                PartialIndexWorkload(
                    self.db, self.cache,
                    {'rows': rows, 'cache_modes': ['hot'], 'parallelism': 0}
                ),
            ]

        elif self.mode == 'full':
            # Full mode: All workloads with multiple configurations
            for rows in scale_params['rows']:
                cache_modes = scale_params['cache_modes']

                workloads.extend([
                    TimeSeriesWorkload(
                        self.db, self.cache,
                        {'rows': rows, 'metrics': 50, 'cache_modes': cache_modes, 'parallelism': 0}
                    ),
                    DimensionWorkload(
                        self.db, self.cache,
                        {'rows': min(100000, rows), 'cache_modes': cache_modes, 'parallelism': 0}
                    ),
                    EventStreamWorkload(
                        self.db, self.cache,
                        {'rows': rows, 'distinct_users': rows//100, 'cache_modes': cache_modes, 'parallelism': 0}
                    ),
                    SparseWorkload(
                        self.db, self.cache,
                        {'rows': rows, 'cache_modes': cache_modes, 'parallelism': 0}
                    ),
                    CompositeWorkload(
                        self.db, self.cache,
                        {'rows': rows, 'cache_modes': cache_modes, 'parallelism': 0}
                    ),
                ])

        return workloads

    def run(self):
        """Execute benchmark suite"""
        self.print_header()

        # Ensure smol extension is available
        try:
            self.db.execute("CREATE EXTENSION IF NOT EXISTS smol;")
        except Exception as e:
            print(f"ERROR: Could not load smol extension: {e}")
            sys.exit(1)

        # Get workloads
        workloads = self.get_workloads()
        print(f"Running {len(workloads)} workload(s)...\n")

        # Run each workload
        for i, workload in enumerate(workloads, 1):
            print(f"\n[{i}/{len(workloads)}]", end=' ')
            try:
                results = workload.run()
                self.results.extend(results)
            except Exception as e:
                print(f"ERROR in workload: {e}")
                import traceback
                traceback.print_exc()
                continue

        # Generate reports
        self.generate_reports()

        # Check regressions
        self.check_regressions()

    def generate_reports(self):
        """Generate and save reports"""
        if not self.results:
            print("\nNo results to report")
            return

        print(f"\n{'='*70}")
        print("GENERATING REPORTS")
        print(f"{'='*70}")

        # Create results directory
        os.makedirs('bench/results', exist_ok=True)

        # Save JSON results
        reporter = ReportGenerator(self.results)
        json_path = f'bench/results/{self.mode}-{self.timestamp}.json'
        reporter.save_results(json_path)
        print(f"\nJSON results: {json_path}")

        # Save markdown report
        md_path = f'bench/results/{self.mode}-{self.timestamp}.md'
        with open(md_path, 'w') as f:
            f.write(reporter.generate_markdown())
        print(f"Markdown report: {md_path}")

        # Print decision tree
        print("\n" + reporter.generate_decision_tree())

    def check_regressions(self):
        """Check for regressions against baseline"""
        detector = RegressionDetector()
        regressions = detector.check(self.results)

        if regressions:
            detector.print_regressions(regressions)
        else:
            print("\n✓ No regressions detected")

        # Offer to update baseline
        if self.mode == 'full' and not regressions:
            print("\nTo update baseline with these results:")
            print(f"  python3 bench/runner.py --update-baseline bench/results/{self.mode}-{self.timestamp}.json")


def main():
    parser = argparse.ArgumentParser(
        description='SMOL Benchmark Suite v2 - Comprehensive SMOL vs BTREE comparison'
    )
    parser.add_argument(
        '--quick',
        action='store_true',
        help='Run quick benchmark suite (~30 sec, 18 workloads, for CI/regression)'
    )
    parser.add_argument(
        '--full',
        action='store_true',
        help='Run full comprehensive suite (15-20 min)'
    )
    parser.add_argument(
        '--update-baseline',
        metavar='RESULTS_JSON',
        help='Update baseline from results JSON file'
    )

    args = parser.parse_args()

    # Handle baseline update
    if args.update_baseline:
        import json
        from bench.lib.metrics import WorkloadResult

        with open(args.update_baseline, 'r') as f:
            data = json.load(f)

        results = []
        for r in data['results']:
            results.append(WorkloadResult(**r))

        detector = RegressionDetector()
        detector.update_baseline(results)
        print(f"✓ Baseline updated from {args.update_baseline}")
        sys.exit(0)

    # Determine mode
    if args.full:
        mode = 'full'
    else:
        mode = 'quick'

    # Run benchmark
    runner = BenchmarkRunner(mode=mode)
    try:
        runner.run()
    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
