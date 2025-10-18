"""
Backward Scan Workload - Tests ORDER BY ... DESC efficiency

SMOL has special optimizations for backward scans. This workload
compares BTREE vs SMOL for descending order queries.
"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query


class BackwardScanWorkload(WorkloadBase):
    """Test backward scan efficiency"""

    def get_workload_id(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"backward_{rows//1000}k"

    def get_description(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"Backward scans: {rows:,} rows"

    def generate_data(self):
        """Generate sequential data for backward scan testing"""
        rows = self.config.get('rows', 1_000_000)

        self.db.execute(f"DROP TABLE IF EXISTS {self.table_name} CASCADE;")
        self.db.execute(f"""
            CREATE UNLOGGED TABLE {self.table_name} (
                id int4,
                payload int4
            );
        """)

        self.db.execute(f"""
            INSERT INTO {self.table_name}
            SELECT i, i % 1000
            FROM generate_series(1, {rows}) i;
        """)

        self.db.execute(f"VACUUM (FREEZE, ANALYZE) {self.table_name};")

    def get_queries(self):
        """Return backward scan queries"""
        return [
            Query(
                id='desc_limit100',
                sql=f"SELECT * FROM {self.table_name} ORDER BY id DESC LIMIT 100;",
                description="Last 100 rows (backward)",
                repeat=5
            ),
            Query(
                id='desc_limit10k',
                sql=f"SELECT * FROM {self.table_name} ORDER BY id DESC LIMIT 10000;",
                description="Last 10K rows (backward)",
                repeat=5
            ),
            Query(
                id='desc_range',
                sql=f"SELECT * FROM {self.table_name} WHERE id > 900000 ORDER BY id DESC;",
                description="Range + backward scan",
                repeat=5
            ),
        ]
