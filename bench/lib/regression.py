"""Regression detection for benchmark results"""

import os
import json
from typing import List, Dict, Optional
from .metrics import WorkloadResult


class RegressionDetector:
    """Detects performance regressions by comparing against baseline"""

    def __init__(self, baseline_path: str = 'bench/baseline.json'):
        self.baseline_path = baseline_path
        self.baseline: Optional[Dict] = None
        self._load_baseline()

    def _load_baseline(self):
        """Load baseline results from file"""
        if os.path.exists(self.baseline_path):
            try:
                with open(self.baseline_path, 'r') as f:
                    self.baseline = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load baseline from {self.baseline_path}: {e}")
                self.baseline = None
        else:
            self.baseline = None

    def check(self, results: List[WorkloadResult]) -> List[Dict]:
        """
        Check for regressions against baseline

        Returns list of regressions found, each as:
        {
            'workload': str,
            'metric': str,
            'baseline': float,
            'current': float,
            'regression_pct': float
        }
        """
        if not self.baseline:
            return []

        regressions = []
        threshold = 15.0  # 15% regression threshold

        # Group results by workload_id and engine
        workload_stats = {}
        for result in results:
            key = (result.workload_id, result.engine)
            if key not in workload_stats:
                workload_stats[key] = []
            workload_stats[key].append(result)

        # Check each SMOL workload
        for (workload_id, engine), workload_results in workload_stats.items():
            if engine != 'smol':
                continue

            baseline_result = self._find_baseline_result(workload_id)
            if not baseline_result:
                continue

            # Calculate average latency
            avg_latency = sum(r.latency_avg_ms for r in workload_results) / len(workload_results)

            # Check latency regression
            if 'latency_avg_ms' in baseline_result:
                baseline_latency = baseline_result['latency_avg_ms']

                if avg_latency > baseline_latency * (1 + threshold / 100):
                    regression_pct = ((avg_latency - baseline_latency) / baseline_latency) * 100
                    regressions.append({
                        'workload': workload_id,
                        'metric': 'latency_avg_ms',
                        'baseline': baseline_latency,
                        'current': avg_latency,
                        'regression_pct': regression_pct
                    })

            # Check index size regression
            if workload_results and 'index_size_mb' in baseline_result:
                current_size = workload_results[0].index_size_mb
                baseline_size = baseline_result['index_size_mb']

                if current_size > baseline_size * (1 + threshold / 100):
                    regression_pct = ((current_size - baseline_size) / baseline_size) * 100
                    regressions.append({
                        'workload': workload_id,
                        'metric': 'index_size_mb',
                        'baseline': baseline_size,
                        'current': current_size,
                        'regression_pct': regression_pct
                    })

        return regressions

    def _find_baseline_result(self, workload_id: str) -> Optional[Dict]:
        """Find baseline result for given workload"""
        if not self.baseline or 'results' not in self.baseline:
            return None

        for result in self.baseline['results']:
            if result.get('workload_id') == workload_id:
                return result

        return None

    def print_regressions(self, regressions: List[Dict]):
        """Print regression report"""
        print("\n" + "=" * 80)
        print("⚠️  PERFORMANCE REGRESSIONS DETECTED")
        print("=" * 80)

        for reg in regressions:
            print(f"\nWorkload: {reg['workload']}")
            print(f"  Metric:     {reg['metric']}")
            print(f"  Baseline:   {reg['baseline']:.2f}")
            print(f"  Current:    {reg['current']:.2f}")
            print(f"  Regression: {reg['regression_pct']:.1f}%")

        print("\n" + "=" * 80)

    def update_baseline(self, results: List[WorkloadResult]):
        """Update baseline with new results"""
        # Group results by workload_id and engine to calculate aggregates
        workload_stats = {}
        for result in results:
            key = (result.workload_id, result.engine)
            if key not in workload_stats:
                workload_stats[key] = []
            workload_stats[key].append(result)

        baseline_data = {
            'results': []
        }

        # Create baseline entries for each SMOL workload
        for (workload_id, engine), workload_results in workload_stats.items():
            if engine != 'smol':
                continue

            # Calculate aggregate statistics
            avg_latency = sum(r.latency_avg_ms for r in workload_results) / len(workload_results)
            avg_size = workload_results[0].index_size_mb  # Index size should be same across queries

            result_dict = {
                'workload_id': workload_id,
                'latency_avg_ms': avg_latency,
                'index_size_mb': avg_size,
                'build_time_ms': workload_results[0].build_time_ms
            }

            baseline_data['results'].append(result_dict)

        # Ensure directory exists
        os.makedirs(os.path.dirname(self.baseline_path), exist_ok=True)

        # Write baseline
        with open(self.baseline_path, 'w') as f:
            json.dump(baseline_data, f, indent=2)
