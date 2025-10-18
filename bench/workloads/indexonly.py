"""
Index-Only Scan Workload - Tests covering index efficiency

When all columns are in the index (via INCLUDE), can SMOL's
columnar format avoid heap access more efficiently than BTREE?
"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query


class IndexOnlyScanWorkload(WorkloadBase):
    """Test index-only scan efficiency"""

    def get_workload_id(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"ios_{rows//1000}k"

    def get_description(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"Index-only scans: {rows:,} rows"

    def generate_data(self):
        """Generate data for index-only scan testing"""
        rows = self.config.get('rows', 1_000_000)

        self.db.execute("DROP TABLE IF EXISTS ios_test CASCADE;")
        self.db.execute("""
            CREATE TABLE ios_test (
                id int4,
                status text,
                value int4,
                extra text
            );
        """)

        print(f"Generating data...", end=' ', flush=True)
        self.db.execute(f"""
            INSERT INTO ios_test
            SELECT
                i,
                CASE (i % 3)
                    WHEN 0 THEN 'active'
                    WHEN 1 THEN 'pending'
                    ELSE 'completed'
                END,
                i % 1000,
                'padding_' || i
            FROM generate_series(1, {rows}) i;
        """)

        # VACUUM to ensure visibility map is up to date (enables IOS)
        self.db.execute("VACUUM ios_test;")
        print("done")

    def get_queries(self):
        """Return index-only scan queries"""
        return [
            Query(
                id='ios_range',
                sql="SELECT id FROM ios_test WHERE id BETWEEN 100000 AND 200000;",
                description="Index-only scan (100K rows)",
                repeat=5
            ),
            Query(
                id='ios_small',
                sql="SELECT id FROM ios_test WHERE id BETWEEN 100000 AND 110000;",
                description="Index-only scan (10K rows)",
                repeat=5
            ),
        ]

    def get_table_name(self) -> str:
        return "ios_test"

    def get_index_column(self) -> str:
        return "id"
