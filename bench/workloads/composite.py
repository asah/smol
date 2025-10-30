"""Composite key workload - multi-column indexes"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query
from typing import List


class CompositeWorkload(WorkloadBase):
    """Composite keys: (date, id) indexes with selective queries"""

    def get_workload_id(self) -> str:
        rows = self.config['rows']
        return f"composite_{rows//1000}k"

    def get_description(self) -> str:
        return f"Composite key (date, id): {self.config['rows']:,} rows"

    def get_index_columns(self) -> str:
        # SMOL supports multi-column, BTREE we'll use single column + INCLUDE
        return 'order_date'

    def generate_data(self):
        """Order/event table with composite key"""
        rows = self.config['rows']

        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            ('2024-01-01'::date + (i % 365)) AS order_date,
            i AS order_id,
            (random() * 5000)::int AS amount,
            (random() * 1000)::int AS customer_id
        FROM generate_series(1, {rows}) i
        ORDER BY 1, 2;

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

        # Configure INCLUDE columns
        self.config['include_columns'] = ['order_id', 'amount']

    def get_queries(self) -> List[Query]:
        return [
            Query(
                id='recent_orders',
                sql=f"""
                    SELECT count(*), sum(amount)
                    FROM {self.table_name}
                    WHERE order_date >= '2024-10-01'
                """,
                description="Recent orders (Q4 2024)"
            ),
            Query(
                id='single_day',
                sql=f"""
                    SELECT count(*)
                    FROM {self.table_name}
                    WHERE order_date >= '2024-06-15' AND order_date < '2024-06-16'
                """,
                description="Single day orders"
            )
        ]
