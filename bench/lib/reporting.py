"""Report generation and decision tree output"""

from typing import List, Dict
from collections import defaultdict


class ReportGenerator:
    """Generate benchmark reports and recommendations"""

    def __init__(self, results: List):
        self.results = results

    def generate_markdown(self) -> str:
        """Generate markdown report"""
        lines = []

        lines.append("# SMOL vs BTREE Benchmark Results\n")
        lines.append(f"Generated: {self.results[0].timestamp if self.results else 'N/A'}\n")
        lines.append("## Summary\n")

        # Group by workload
        by_workload = defaultdict(list)
        for r in self.results:
            by_workload[r.workload_id].append(r)

        for workload_id, workload_results in sorted(by_workload.items()):
            lines.append(f"\n### {workload_id}\n")

            # Group by query and cache mode
            by_query_cache = defaultdict(lambda: {'btree': None, 'smol': None})
            for r in workload_results:
                key = f"{r.query_id}_{r.cache_mode}"
                by_query_cache[key][r.engine] = r

            for key, engines in sorted(by_query_cache.items()):
                btree = engines['btree']
                smol = engines['smol']

                if btree and smol:
                    query_id = btree.query_id
                    cache_mode = btree.cache_mode
                    speedup = btree.latency_p50_ms / smol.latency_p50_ms if smol.latency_p50_ms > 0 else 0
                    compression = btree.index_size_mb / smol.index_size_mb if smol.index_size_mb > 0 else 0

                    lines.append(f"\n**Query: {query_id} ({cache_mode})**\n")
                    lines.append(f"- BTREE: {btree.latency_p50_ms:.1f}ms (p50), {btree.index_size_mb:.1f}MB")
                    lines.append(f"- SMOL:  {smol.latency_p50_ms:.1f}ms (p50), {smol.index_size_mb:.1f}MB")
                    lines.append(f"- Speedup: {speedup:.2f}x, Compression: {compression:.1f}x\n")

        return '\n'.join(lines)

    def generate_decision_tree(self) -> str:
        """Generate actionable recommendations"""
        lines = []

        lines.append("╔══════════════════════════════════════════════════════════╗")
        lines.append("║  SMOL vs BTREE - RECOMMENDATIONS                        ║")
        lines.append("╚══════════════════════════════════════════════════════════╝\n")

        # Analyze results to determine when SMOL wins
        recommendations = []

        by_workload = defaultdict(list)
        for r in self.results:
            by_workload[r.workload_id].append(r)

        for workload_id, workload_results in sorted(by_workload.items()):
            # Find btree and smol results for same query/cache
            btree_results = [r for r in workload_results if r.engine == 'btree']
            smol_results = [r for r in workload_results if r.engine == 'smol']

            if not btree_results or not smol_results:
                continue

            btree = btree_results[0]
            smol = smol_results[0]

            speedup = btree.latency_p50_ms / smol.latency_p50_ms if smol.latency_p50_ms > 0 else 0
            compression = btree.index_size_mb / smol.index_size_mb if smol.index_size_mb > 0 else 0

            if speedup > 1.5 and compression > 2:
                rec = "✓ STRONGLY RECOMMEND SMOL"
                reason = f"{speedup:.1f}x faster, {compression:.1f}x smaller"
            elif speedup > 1.2:
                rec = "✓ RECOMMEND SMOL"
                reason = f"{speedup:.1f}x faster"
            elif compression > 3:
                rec = "✓ RECOMMEND SMOL (size)"
                reason = f"{compression:.1f}x smaller, similar performance"
            elif speedup < 0.9:
                rec = "✗ USE BTREE"
                reason = f"SMOL is {1/speedup:.1f}x slower"
            else:
                rec = "~ NEUTRAL (tie)"
                reason = "Similar performance and size"

            lines.append(f"{rec}")
            lines.append(f"  Workload: {workload_id}")
            lines.append(f"  Reason: {reason}\n")

        lines.append("\nGeneral Guidelines:\n")
        lines.append("Use SMOL when:")
        lines.append("  • Low cardinality data (high RLE compression potential)")
        lines.append("  • Memory-constrained environments (smaller = better cache hit)")
        lines.append("  • Read-heavy analytical workloads")
        lines.append("  • Multi-column indexes with selective filters\n")

        lines.append("Use BTREE when:")
        lines.append("  • Write-heavy workloads (SMOL is read-only)")
        lines.append("  • Ultra-low latency requirements (<1ms)")
        lines.append("  • Variable-width keys")

        return '\n'.join(lines)

    def save_results(self, filepath: str):
        """Save results to JSON file"""
        import json

        data = {
            'results': [r.to_dict() for r in self.results],
            'timestamp': self.results[0].timestamp if self.results else None
        }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
