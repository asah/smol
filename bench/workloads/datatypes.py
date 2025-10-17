"""
Data Types Workload - Tests different numeric types

SMOL may have optimizations for specific data types (int2, int8, etc).
This workload tests the same query pattern across different types.
"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query


class DataTypesWorkload(WorkloadBase):
    """Test different data types"""

    def get_workload_id(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        dtype = self.config.get('data_type', 'int2')
        return f"dtype_{dtype}_{rows//1000}k"

    def get_description(self) -> str:
        rows = self.config.get('rows', 1_000_000)
        dtype = self.config.get('data_type', 'int2')
        return f"Data type ({dtype}): {rows:,} rows"

    def generate_data(self):
        """Generate data with specified type"""
        rows = self.config.get('rows', 1_000_000)
        dtype = self.config.get('data_type', 'int2')

        # Determine value range based on type
        if dtype == 'int2':
            max_val = 32767
        elif dtype == 'int8':
            max_val = rows * 2
        elif dtype == 'float4':
            max_val = rows * 2
        elif dtype == 'date':
            max_val = None  # Special handling
        else:
            max_val = rows * 2

        self.db.execute("DROP TABLE IF EXISTS dtype_test CASCADE;")

        if dtype == 'date':
            self.db.execute("""
                CREATE TABLE dtype_test (
                    id date PRIMARY KEY,
                    payload int4
                );
            """)
            print(f"Generating data...", end=' ', flush=True)
            self.db.execute(f"""
                INSERT INTO dtype_test
                SELECT
                    '2020-01-01'::date + (i || ' days')::interval,
                    i % 1000
                FROM generate_series(1, {rows}) i;
            """)
        else:
            self.db.execute(f"""
                CREATE TABLE dtype_test (
                    id {dtype} PRIMARY KEY,
                    payload int4
                );
            """)
            print(f"Generating data...", end=' ', flush=True)
            self.db.execute(f"""
                INSERT INTO dtype_test
                SELECT
                    (i % {max_val})::{dtype},
                    i % 1000
                FROM generate_series(1, {rows}) i;
            """)

        print("done")

    def get_queries(self):
        """Return queries adapted to data type"""
        dtype = self.config.get('data_type', 'int2')

        if dtype == 'date':
            return [
                Query(
                    id='range_1month',
                    sql="""
                        SELECT * FROM dtype_test
                        WHERE id BETWEEN '2020-06-01' AND '2020-06-30';
                    """,
                    description="Date range (1 month)",
                    repeat=5
                ),
                Query(
                    id='recent',
                    sql="""
                        SELECT * FROM dtype_test
                        WHERE id > '2022-01-01'
                        ORDER BY id DESC LIMIT 1000;
                    """,
                    description="Recent dates (backward scan)",
                    repeat=5
                ),
            ]
        else:
            rows = self.config.get('rows', 1_000_000)
            mid = rows // 2
            return [
                Query(
                    id='range_1pct',
                    sql=f"SELECT * FROM dtype_test WHERE id BETWEEN {mid} AND {mid + rows//100};",
                    description="Range scan (1% of data)",
                    repeat=5
                ),
                Query(
                    id='point',
                    sql=f"SELECT * FROM dtype_test WHERE id = {mid};",
                    description="Point lookup",
                    repeat=5
                ),
            ]

    def get_table_name(self) -> str:
        return "dtype_test"

    def get_index_column(self) -> str:
        return "id"
