"""
IN Clause Workload - Tests multiple equality lookups

Very common query pattern: WHERE id IN (1, 2, 3, ...)
Tests efficiency of multiple point lookups in a single query.
"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query


class InClauseWorkload(WorkloadBase):
    """Test IN clause efficiency"""

    def get_workload_id(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"in_clause_{rows//1000}k"

    def get_description(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        return f"IN clause queries: {rows:,} rows"

    def generate_data(self):
        """Generate data for IN clause testing"""
        rows = self.config.get('rows', 1_000_000)

        self.db.execute("DROP TABLE IF EXISTS in_test CASCADE;")
        self.db.execute("""
            CREATE TABLE in_test (
                id int4,
                payload text
            );
        """)

        print(f"Generating data...", end=' ', flush=True)
        self.db.execute(f"""
            INSERT INTO in_test
            SELECT i, 'data_' || i
            FROM generate_series(1, {rows}) i;
        """)
        print("done")

    def get_queries(self):
        """Return IN clause queries with varying sizes"""
        # Generate IN lists with scattered IDs
        in_5 = ','.join(str(i * 10000) for i in range(1, 6))
        in_50 = ','.join(str(i * 2000) for i in range(1, 51))
        in_500 = ','.join(str(i * 200) for i in range(1, 501))

        return [
            Query(
                id='in_5',
                sql=f"SELECT * FROM in_test WHERE id IN ({in_5});",
                description="IN clause with 5 values",
                repeat=5
            ),
            Query(
                id='in_50',
                sql=f"SELECT * FROM in_test WHERE id IN ({in_50});",
                description="IN clause with 50 values",
                repeat=5
            ),
            Query(
                id='in_500',
                sql=f"SELECT * FROM in_test WHERE id IN ({in_500});",
                description="IN clause with 500 values",
                repeat=5
            ),
        ]

    def get_table_name(self) -> str:
        return "in_test"

    def get_index_column(self) -> str:
        return "id"
