"""
Partial Index Workload - Tests filtered index efficiency

Very common pattern: CREATE INDEX ... WHERE status = 'active'
Tests if SMOL's compression is even better on small, selective indexes.
"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query


class PartialIndexWorkload(WorkloadBase):
    """Test partial index efficiency"""

    def get_workload_id(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"partial_{rows//1000}k"

    def get_description(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"Partial index: {rows:,} rows (10% indexed)"

    def generate_data(self):
        """Generate data with filtered subset"""
        rows = self.config.get('rows', 1_000_000)

        self.db.execute("DROP TABLE IF EXISTS partial_test CASCADE;")
        self.db.execute("""
            CREATE TABLE partial_test (
                id int4,
                status text,
                category int4
            );
        """)

        print(f"Generating data...", end=' ', flush=True)
        self.db.execute(f"""
            INSERT INTO partial_test
            SELECT
                i,
                CASE WHEN i % 10 = 0 THEN 'active' ELSE 'inactive' END,
                i % 100
            FROM generate_series(1, {rows}) i;
        """)
        print("done")

    def get_queries(self):
        """Return queries that use partial index"""
        return [
            Query(
                id='active_category',
                sql="SELECT * FROM partial_test WHERE status = 'active' AND category = 42;",
                description="Partial index scan (small result)",
                repeat=5
            ),
            Query(
                id='active_range',
                sql="SELECT * FROM partial_test WHERE status = 'active' AND id BETWEEN 100000 AND 200000;",
                description="Partial index range scan",
                repeat=5
            ),
        ]

    def get_table_name(self) -> str:
        return "partial_test"

    def get_index_column(self) -> str:
        return "id"

    def get_btree_index_sql(self, table: str, column: str) -> str:
        """Override to create partial index"""
        return f"""
            CREATE INDEX idx_{table}_btree ON {table}({column})
            WHERE status = 'active';
        """

    def get_smol_index_sql(self, table: str, column: str) -> str:
        """Override to create partial index"""
        return f"""
            CREATE INDEX idx_{table}_smol ON {table}
            USING smol({column})
            WHERE status = 'active';
        """
