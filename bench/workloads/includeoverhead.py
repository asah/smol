"""INCLUDE column overhead test - columnar format advantage"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query
from typing import List


class IncludeOverheadWorkload(WorkloadBase):
    """Test INCLUDE column overhead (SMOL should handle better)"""

    def get_workload_id(self) -> str:
        rows = self.config['rows']
        n_includes = self.config.get('n_includes', 2)
        return f"include_{n_includes}cols_{rows//1000}k"

    def get_description(self) -> str:
        n_includes = self.config.get('n_includes', 2)
        return f"INCLUDE overhead ({n_includes} cols): {self.config['rows']:,} rows"

    def get_index_columns(self) -> str:
        return 'id'

    def generate_data(self):
        """Generate data with multiple potential INCLUDE columns"""
        rows = self.config['rows']
        n_includes = self.config.get('n_includes', 2)

        # Generate column definitions
        col_defs = ['i AS id']
        for n in range(8):  # Generate 8 columns, use subset
            col_defs.append(f"(random() * 1000)::int AS inc{n+1}")

        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            {', '.join(col_defs)}
        FROM generate_series(1, {rows}) i;

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

        # Configure INCLUDE columns based on n_includes
        if n_includes > 0:
            include_cols = [f'inc{n+1}' for n in range(n_includes)]
            self.config['include_columns'] = include_cols
        else:
            self.config['include_columns'] = []

    def get_queries(self) -> List[Query]:
        n_includes = self.config.get('n_includes', 2)

        # Build SELECT list
        if n_includes > 0:
            select_cols = ', '.join([f'inc{n+1}' for n in range(n_includes)])
            select_list = f'{select_cols}, count(*)'
        else:
            select_list = 'count(*)'

        return [
            Query(
                id=f'scan_{n_includes}inc',
                sql=f"SELECT {select_list} FROM {self.table_name} WHERE id >= 500000",
                description=f"Range scan with {n_includes} INCLUDE columns"
            )
        ]
