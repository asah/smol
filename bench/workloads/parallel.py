"""Parallel scaling test - show SMOL's parallelization advantage"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query
from typing import List


class ParallelScalingWorkload(WorkloadBase):
    """Test parallel scan scaling with different worker counts"""

    def get_workload_id(self) -> str:
        rows = self.config['rows']
        workers = self.config.get('parallelism', 4)
        return f"parallel_{workers}w_{rows//1000}k"

    def get_description(self) -> str:
        workers = self.config.get('parallelism', 4)
        return f"Parallel ({workers} workers): {self.config['rows']:,} rows"

    def get_index_columns(self) -> str:
        return 'id'

    def generate_data(self):
        """Generate data optimized for parallel scanning"""
        rows = self.config['rows']

        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            i AS id,
            (i % 100)::int AS category,
            (random() * 1000)::int AS value
        FROM generate_series(1, {rows}) i
        ORDER BY 2;  -- Sort by category for some RLE

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

        # Configure INCLUDE columns
        self.config['include_columns'] = ['category', 'value']

    def get_queries(self) -> List[Query]:
        workers = self.config.get('parallelism', 4)

        return [
            Query(
                id=f'full_scan_{workers}w',
                sql=f"SELECT count(*), sum(value) FROM {self.table_name}",
                description=f"Full scan with {workers} workers"
            )
        ]
