"""Metrics and result data structures"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime


@dataclass
class Query:
    """Query definition"""
    id: str
    sql: str
    description: str
    repeat: int = 1


@dataclass
class WorkloadResult:
    """Result from a single workload execution"""
    workload_id: str
    engine: str  # 'btree' or 'smol'
    query_id: str
    cache_mode: str  # 'hot', 'warm', 'cold'

    # Performance metrics
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latency_avg_ms: float
    latency_min_ms: float
    latency_max_ms: float

    # Resource metrics
    index_size_mb: float
    build_time_ms: float
    shared_hit: int
    shared_read: int

    # Query details
    rows_scanned: int
    rows_returned: int

    # Metadata
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            'workload_id': self.workload_id,
            'engine': self.engine,
            'query_id': self.query_id,
            'cache_mode': self.cache_mode,
            'latency_p50_ms': self.latency_p50_ms,
            'latency_p95_ms': self.latency_p95_ms,
            'latency_p99_ms': self.latency_p99_ms,
            'latency_avg_ms': self.latency_avg_ms,
            'latency_min_ms': self.latency_min_ms,
            'latency_max_ms': self.latency_max_ms,
            'index_size_mb': self.index_size_mb,
            'build_time_ms': self.build_time_ms,
            'shared_hit': self.shared_hit,
            'shared_read': self.shared_read,
            'rows_scanned': self.rows_scanned,
            'rows_returned': self.rows_returned,
            'timestamp': self.timestamp,
            'config': self.config
        }


def calculate_percentiles(values: List[float]) -> dict:
    """Calculate percentile metrics from list of values"""
    if not values:
        return {
            'p50': 0, 'p95': 0, 'p99': 0,
            'avg': 0, 'min': 0, 'max': 0
        }

    sorted_vals = sorted(values)
    n = len(sorted_vals)

    return {
        'p50': sorted_vals[int(n * 0.50)] if n > 0 else 0,
        'p95': sorted_vals[int(n * 0.95)] if n > 1 else sorted_vals[-1],
        'p99': sorted_vals[int(n * 0.99)] if n > 2 else sorted_vals[-1],
        'avg': sum(values) / n if n > 0 else 0,
        'min': min(values) if values else 0,
        'max': max(values) if values else 0
    }
