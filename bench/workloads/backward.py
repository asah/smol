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

        self.db.execute("DROP TABLE IF EXISTS bwd_test CASCADE;")
        self.db.execute("""
            CREATE TABLE bwd_test (
                id int4 PRIMARY KEY,
                payload int4
            );
        """)

        print(f"Generating data...", end=' ', flush=True)
        self.db.execute(f"""
            INSERT INTO bwd_test
            SELECT i, i % 1000
            FROM generate_series(1, {rows}) i;
        """)
        print("done")

    def get_queries(self):
        """Return backward scan queries"""
        return [
            Query(
                id='desc_limit100',
                sql="SELECT * FROM bwd_test ORDER BY id DESC LIMIT 100;",
                description="Last 100 rows (backward)",
                repeat=5
            ),
            Query(
                id='desc_limit10k',
                sql="SELECT * FROM bwd_test ORDER BY id DESC LIMIT 10000;",
                description="Last 10K rows (backward)",
                repeat=5
            ),
            Query(
                id='desc_range',
                sql="SELECT * FROM bwd_test WHERE id > 900000 ORDER BY id DESC;",
                description="Range + backward scan",
                repeat=5
            ),
        ]

    def get_table_name(self) -> str:
        return "bwd_test"

    def get_index_column(self) -> str:
        return "id"
