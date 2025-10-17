"""Sparse/partial index workload"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query
from typing import List


class SparseWorkload(WorkloadBase):
    """Sparse indexes: filtered subset with extreme compression potential"""

    def get_workload_id(self) -> str:
        rows = self.config['rows']
        return f"sparse_{rows//1000}k"

    def get_description(self) -> str:
        return f"Sparse index: {self.config['rows']:,} rows (filtered to ~40%)"

    def get_index_columns(self) -> str:
        return 'status'

    def generate_data(self):
        """Orders table, index only on pending/processing statuses"""
        rows = self.config['rows']

        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            i AS order_id,
            ('{{'pending','processing','shipped','delivered','cancelled'}}'::text[])[
                1 + floor(random() * 5)::int
            ] AS status,
            NOW() - (random() * INTERVAL '90 days') AS order_date,
            (random() * 10000)::numeric(10,2) AS total
        FROM generate_series(1, {rows}) i;

        -- Only keep active orders for indexing (pending + processing)
        DELETE FROM {self.table_name}
        WHERE status NOT IN ('pending', 'processing');

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

        # Configure INCLUDE columns
        self.config['include_columns'] = ['order_date', 'total']

    def get_queries(self) -> List[Query]:
        return [
            Query(
                id='pending_orders',
                sql=f"""
                    SELECT count(*), sum(total)
                    FROM {self.table_name}
                    WHERE status = 'pending'
                """,
                description="Pending orders (very selective on small domain)"
            ),
            Query(
                id='all_active',
                sql=f"""
                    SELECT count(*)
                    FROM {self.table_name}
                    WHERE status IN ('pending', 'processing')
                """,
                description="All active orders"
            )
        ]
