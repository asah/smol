"""Dimension table workload - small lookup tables"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query
from typing import List


class DimensionWorkload(WorkloadBase):
    """Dimension tables: small, mostly-static lookup tables with frequent joins"""

    def get_workload_id(self) -> str:
        rows = self.config['rows']
        return f"dimension_{rows//1000}k"

    def get_description(self) -> str:
        return f"Dimension table: {self.config['rows']:,} rows"

    def get_index_columns(self) -> str:
        return 'id'

    def generate_data(self):
        """Small lookup table with realistic distribution"""
        rows = self.config['rows']

        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            i AS id,
            -- Country codes (realistic for dimension table)
            (ARRAY['US','CN','IN','BR','ID','PK','NG','BD','RU','MX'])[
                1 + (i % 10)
            ] AS country_code,
            'Customer ' || i AS name,
            ('{{'active','inactive','suspended'}}'::text[])[
                1 + (i % 3)
            ] AS status,
            ('2020-01-01'::date + (i % 1000)) AS created_date
        FROM generate_series(1, {rows}) i;

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

        # Configure INCLUDE columns
        self.config['include_columns'] = ['country_code', 'name']

    def get_queries(self) -> List[Query]:
        # Get a random ID for point lookup
        import random
        random_id = random.randint(1, self.config['rows'])

        return [
            Query(
                id='point_lookup',
                sql=f"SELECT * FROM {self.table_name} WHERE id = {random_id}",
                description="Point lookup (random ID)",
                repeat=10
            ),
            Query(
                id='range_scan',
                sql=f"SELECT count(*) FROM {self.table_name} WHERE id >= {self.config['rows'] // 2}",
                description="Range scan (50% selectivity)"
            )
        ]
