"""Event stream workload with hot keys"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query
from typing import List


class EventStreamWorkload(WorkloadBase):
    """Event streams: Zipfian distribution, hot keys get 80% of traffic"""

    def get_workload_id(self) -> str:
        rows = self.config['rows']
        users = self.config.get('distinct_users', 1000)
        return f"events_{rows//1000}k_{users}_users"

    def get_description(self) -> str:
        return f"Event stream: {self.config['rows']:,} rows, {self.config.get('distinct_users', 1000)} users"

    def get_index_columns(self) -> str:
        return 'user_id'

    def generate_data(self):
        """Event data with power-law distribution"""
        rows = self.config['rows']
        distinct_users = self.config.get('distinct_users', 1000)

        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            -- Zipfian user_id (top 1% = 50% of events)
            CASE
                WHEN random() < 0.5 THEN floor(random() * ({distinct_users} * 0.01))::int
                WHEN random() < 0.8 THEN floor(random() * ({distinct_users} * 0.2))::int
                ELSE floor(random() * {distinct_users})::int
            END AS user_id,
            ('{{'click','view','purchase','cart_add'}}'::text[])[
                1 + floor(random() * 4)::int
            ] AS event_type,
            NOW() - (random() * INTERVAL '30 days') AS event_time,
            (random() * 1000)::int AS session_id
        FROM generate_series(1, {rows}) i
        ORDER BY 1;  -- Pre-sort for RLE

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

        # Configure INCLUDE columns
        self.config['include_columns'] = ['event_type', 'session_id']

    def get_queries(self) -> List[Query]:
        return [
            Query(
                id='hot_user',
                sql=f"""
                    SELECT count(*), event_type
                    FROM {self.table_name}
                    WHERE user_id = 5
                    GROUP BY 2
                """,
                description="Hot user events (high duplicate key)"
            ),
            Query(
                id='user_range',
                sql=f"""
                    SELECT count(*)
                    FROM {self.table_name}
                    WHERE user_id < 100
                """,
                description="User range scan"
            )
        ]
