"""Time-series analytics workload"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query
from typing import List


class TimeSeriesWorkload(WorkloadBase):
    """Time-series analytics: ordered timestamps, low cardinality metrics, range scans"""

    def get_workload_id(self) -> str:
        rows = self.config['rows']
        metrics = self.config.get('metrics', 50)
        card = 'ultra_low' if metrics < 100 else 'low' if metrics < 500 else 'med'
        return f"timeseries_{rows//1000}k_{card}_card"

    def get_description(self) -> str:
        return f"Time-series: {self.config['rows']:,} rows, {self.config.get('metrics', 50)} metrics"

    def get_index_columns(self) -> str:
        return 'metric_id'

    def generate_data(self):
        """Generate realistic time-series with temporal skew"""
        rows = self.config['rows']
        metrics = self.config.get('metrics', 50)

        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            -- Recent data accessed more (temporal locality)
            CURRENT_TIMESTAMP -
                (random() * POWER(0.7, 2) * INTERVAL '365 days') AS ts,

            -- Zipfian metric distribution (80/20 rule)
            (CASE
                WHEN random() < 0.8
                THEN floor(random() * {metrics} * 0.2)::int
                ELSE floor(random() * {metrics})::int
            END) AS metric_id,

            -- Value with some correlation to time
            (random() * 100 + 10 * sin(i::float / 10000.0))::float4 AS value,

            -- Fixed-width includes
            (random() * 10000)::int AS customer_id,
            (random() * 5)::smallint AS severity
        FROM generate_series(1, {rows}) i
        ORDER BY 2;  -- Pre-sort by metric_id for RLE benefit

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

        # Configure INCLUDE columns
        self.config['include_columns'] = ['customer_id', 'severity']

    def get_queries(self) -> List[Query]:
        return [
            Query(
                id='hot_metric',
                sql=f"""
                    SELECT count(*), avg(value)
                    FROM {self.table_name}
                    WHERE metric_id = 5
                """,
                description="Single hot metric (equality)"
            ),
            Query(
                id='metric_range',
                sql=f"""
                    SELECT count(*), avg(value)
                    FROM {self.table_name}
                    WHERE metric_id BETWEEN 10 AND 20
                """,
                description="Metric range scan"
            )
        ]
