"""
LIMIT Query Workload - Tests early termination efficiency

Does SMOL efficiently stop scanning when LIMIT is reached, or does
it have overhead compared to BTREE's early termination?
"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query


class LimitWorkload(WorkloadBase):
    """Test LIMIT clause efficiency"""

    def get_workload_id(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"limit_{rows//1000}k"

    def get_description(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"LIMIT queries: {rows:,} rows"

    def generate_data(self):
        """Generate data for LIMIT testing"""
        rows = self.config.get('rows', 1_000_000)

        self.db.execute("DROP TABLE IF EXISTS limit_test CASCADE;")
        self.db.execute("""
            CREATE TABLE limit_test (
                id int4 PRIMARY KEY,
                category int4,
                value int4
            );
        """)

        print(f"Generating data...", end=' ', flush=True)
        self.db.execute(f"""
            INSERT INTO limit_test
            SELECT
                i,
                i % 100,
                random() * 1000
            FROM generate_series(1, {rows}) i;
        """)
        print("done")

    def get_queries(self):
        """Return LIMIT queries with different sizes"""
        return [
            Query(
                id='limit10',
                sql="SELECT * FROM limit_test WHERE category = 42 LIMIT 10;",
                description="LIMIT 10 (tiny result)",
                repeat=5
            ),
            Query(
                id='limit1000',
                sql="SELECT * FROM limit_test WHERE category = 42 LIMIT 1000;",
                description="LIMIT 1000 (medium result)",
                repeat=5
            ),
            Query(
                id='range_limit100',
                sql="SELECT * FROM limit_test WHERE id > 500000 ORDER BY id LIMIT 100;",
                description="Range scan + LIMIT 100",
                repeat=5
            ),
        ]

    def get_table_name(self) -> str:
        return "limit_test"

    def get_index_column(self) -> str:
        return "id"
