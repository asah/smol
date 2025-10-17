"""Text/UUID key workload - test text32 zero-copy optimization"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query
from typing import List


class TextKeysWorkload(WorkloadBase):
    """Text keys: UUID/VARCHAR with C collation for zero-copy"""

    def get_workload_id(self) -> str:
        rows = self.config['rows']
        key_type = self.config.get('key_type', 'uuid')
        return f"textkeys_{key_type}_{rows//1000}k"

    def get_description(self) -> str:
        key_type = self.config.get('key_type', 'uuid')
        return f"Text keys ({key_type}): {self.config['rows']:,} rows"

    def get_index_columns(self) -> str:
        return 'key_col'

    def generate_data(self):
        """Generate data with text/UUID keys"""
        rows = self.config['rows']
        key_type = self.config.get('key_type', 'uuid')

        if key_type == 'uuid':
            # UUID keys (16 bytes, fits in zero-copy)
            key_def = "uuid"
            key_gen = f"('00000000-0000-0000-0000-' || lpad((i % 10000)::text, 12, '0'))::uuid"
        elif key_type == 'varchar':
            # Short VARCHAR with C collation (fits in text32)
            key_def = "varchar(32) COLLATE \"C\""
            key_gen = "'CODE-' || lpad((i % 10000)::text, 8, '0')"
        else:
            # Default to text
            key_def = "text COLLATE \"C\""
            key_gen = "'KEY-' || (i % 10000)::text"

        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            {key_gen} AS key_col,
            (random() * 1000)::int AS value1,
            (random() * 1000)::int AS value2
        FROM generate_series(1, {rows}) i
        ORDER BY 1;  -- Pre-sort for RLE

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

        # Configure INCLUDE columns
        self.config['include_columns'] = ['value1', 'value2']

    def get_queries(self) -> List[Query]:
        key_type = self.config.get('key_type', 'uuid')

        if key_type == 'uuid':
            key1 = "'00000000-0000-0000-0000-000000005000'::uuid"
            key2 = "'00000000-0000-0000-0000-000000007000'::uuid"
        else:
            key1 = "'CODE-00005000'"
            key2 = "'CODE-00007000'"

        return [
            Query(
                id='equality',
                sql=f"SELECT count(*) FROM {self.table_name} WHERE key_col = {key1}",
                description="Equality on text key"
            ),
            Query(
                id='range',
                sql=f"SELECT count(*) FROM {self.table_name} WHERE key_col >= {key1} AND key_col < {key2}",
                description="Range scan on text key"
            )
        ]
