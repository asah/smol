"""Selectivity range test - find SMOL vs BTREE crossover point"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query
from typing import List


class SelectivityRangeWorkload(WorkloadBase):
    """Test range of selectivities to find where SMOL wins/loses"""

    def get_workload_id(self) -> str:
        rows = self.config['rows']
        selectivity = self.config.get('selectivity', 0.1)
        return f"selectivity_{selectivity*100:.1f}pct_{rows//1000}k"

    def get_description(self) -> str:
        selectivity = self.config.get('selectivity', 0.1)
        return f"Selectivity {selectivity*100:.1f}%: {self.config['rows']:,} rows"

    def get_index_columns(self) -> str:
        return 'id'

    def generate_data(self):
        """Generate sequential data for selectivity testing"""
        rows = self.config['rows']

        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            i AS id,
            (random() * 1000)::int AS value
        FROM generate_series(1, {rows}) i;

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

        # Configure INCLUDE columns
        self.config['include_columns'] = ['value']

    def get_queries(self) -> List[Query]:
        rows = self.config['rows']
        selectivity = self.config.get('selectivity', 0.1)

        # Calculate threshold for desired selectivity
        threshold = int(rows * (1 - selectivity))

        return [
            Query(
                id=f'sel_{selectivity*100:.1f}pct',
                sql=f"SELECT count(*), sum(value) FROM {self.table_name} WHERE id >= {threshold}",
                description=f"Range scan returning {selectivity*100:.1f}% of rows"
            )
        ]
