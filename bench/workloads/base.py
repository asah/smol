"""Abstract base class for workload implementations"""

from abc import ABC, abstractmethod
from typing import List, Dict
import time
from bench.lib.metrics import WorkloadResult, Query, calculate_percentiles


class WorkloadBase(ABC):
    """Abstract base class for workload implementations"""

    def __init__(self, db, cache_controller, config: dict):
        self.db = db
        self.cache = cache_controller
        self.config = config
        self.table_name = f"bench_{self.get_workload_id().replace('-', '_').replace('.', '_')}"

    @abstractmethod
    def get_workload_id(self) -> str:
        """Unique identifier (e.g., 'timeseries_1m_low_card')"""
        pass

    @abstractmethod
    def get_description(self) -> str:
        """Human-readable description"""
        pass

    @abstractmethod
    def generate_data(self) -> None:
        """Create and populate table with realistic data"""
        pass

    @abstractmethod
    def get_queries(self) -> List[Query]:
        """Return list of queries to benchmark"""
        pass

    def run(self) -> List[WorkloadResult]:
        """Execute full workload: data gen -> build indexes -> run queries"""
        results = []

        print(f"\n{'='*70}")
        print(f"Workload: {self.get_description()}")
        print(f"{'='*70}")

        # Generate data
        print(f"Generating data...", end='', flush=True)
        self.generate_data()
        print(" done")

        # Test both BTREE and SMOL
        for engine in ['btree', 'smol']:
            print(f"\nTesting {engine.upper()}:")

            # Build index
            print(f"  Building index...", end='', flush=True)
            build_time = self.build_index(engine)
            index_size = self.get_index_size(engine)
            print(f" {build_time:.0f}ms, {index_size:.1f}MB")

            # Run queries
            for query in self.get_queries():
                for cache_mode in self.config.get('cache_modes', ['hot']):
                    result = self.execute_query(
                        query, engine, cache_mode,
                        build_time_ms=build_time,
                        index_size_mb=index_size
                    )
                    results.append(result)

                    # Print quick result
                    print(f"    {query.id} ({cache_mode}): {result.latency_p50_ms:.1f}ms")

        # Cleanup
        self.cleanup()

        return results

    def get_actual_table_name(self) -> str:
        """Get the actual table name (override with get_table_name() if defined)"""
        if hasattr(self, 'get_table_name'):
            return self.get_table_name()
        return self.table_name

    def build_index(self, engine: str) -> float:
        """Build index and return build time in ms"""
        table = self.get_actual_table_name()
        idx_name = f"idx_{table}_{engine}"

        # Drop if exists
        self.db.execute(f"DROP INDEX IF EXISTS {idx_name};")

        # Build index with INCLUDE columns if configured
        include_clause = ""
        if self.config.get('include_columns'):
            cols = ', '.join(self.config['include_columns'])
            include_clause = f" INCLUDE ({cols})"

        # Get index columns
        index_cols = self.get_index_columns()
        if hasattr(self, 'get_index_column'):
            index_cols = self.get_index_column()

        sql = f"CREATE INDEX {idx_name} ON {table} USING {engine}({index_cols}){include_clause};"

        return self.db.execute_timed(sql)

    def get_index_columns(self) -> str:
        """Get index column definition (override for multi-column indexes)"""
        return self.config.get('index_column', 'id')

    def get_index_size(self, engine: str) -> float:
        """Get index size in MB"""
        table = self.get_actual_table_name()
        idx_name = f"idx_{table}_{engine}"
        size_bytes = self.db.get_relation_size(idx_name)
        return size_bytes / (1024 * 1024)

    def execute_query(self, query: Query, engine: str, cache_mode: str,
                      build_time_ms: float, index_size_mb: float) -> WorkloadResult:
        """Execute query and return result with metrics"""

        table = self.get_actual_table_name()
        idx_name = f"idx_{table}_{engine}"

        # Set PostgreSQL parameters
        self.db.execute("SET enable_seqscan = off;")
        self.db.execute("SET enable_bitmapscan = off;")
        self.db.execute("SET enable_indexonlyscan = on;")

        workers = self.config.get('parallelism', 0)
        self.db.execute(f"SET max_parallel_workers_per_gather = {workers};")

        # Determine number of runs based on cache mode
        if cache_mode == 'cold':
            # Cold cache: single run (flushing before each run is too slow)
            repeat = 1
            warmup = False
        else:
            # Hot/warm cache: multiple runs with warmup
            repeat = query.repeat if hasattr(query, 'repeat') and query.repeat else 5
            warmup = True

        # Warmup run for hot/warm cache (discarded)
        if warmup:
            self.cache.flush_relation(idx_name, cache_mode)
            self.db.execute(query.sql)  # Warmup, not measured

        # Flush cache for cold tests
        if cache_mode == 'cold':
            self.cache.flush_relation(idx_name, cache_mode)

        # Run query multiple times and collect timings
        timings = []
        shared_hit_sum = 0
        shared_read_sum = 0
        rows_returned = 0

        for i in range(repeat):
            _, stats = self.db.execute_with_stats(query.sql)
            timings.append(stats['exec_time'])
            shared_hit_sum += stats.get('shared_hit', 0)
            shared_read_sum += stats.get('shared_read', 0)
            rows_returned = stats.get('rows', 0)

        # Calculate percentiles
        perc = calculate_percentiles(timings)

        return WorkloadResult(
            workload_id=self.get_workload_id(),
            engine=engine,
            query_id=query.id,
            cache_mode=cache_mode,
            latency_p50_ms=perc['p50'],
            latency_p95_ms=perc['p95'],
            latency_p99_ms=perc['p99'],
            latency_avg_ms=perc['avg'],
            latency_min_ms=perc['min'],
            latency_max_ms=perc['max'],
            index_size_mb=index_size_mb,
            build_time_ms=build_time_ms,
            shared_hit=shared_hit_sum // repeat,
            shared_read=shared_read_sum // repeat,
            rows_scanned=0,  # Not easily available
            rows_returned=rows_returned,
            config=self.config
        )

    def cleanup(self):
        """Clean up table and indexes"""
        table = self.get_actual_table_name()
        self.db.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
