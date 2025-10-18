"""Database connection and environment detection"""

import re
import subprocess
import time
from typing import Optional, Tuple


class DatabaseConnection:
    """PostgreSQL database connection wrapper"""

    def __init__(self, dbname: str = 'postgres'):
        self.dbname = dbname

    def execute(self, sql: str, quiet: bool = True) -> str:
        """Execute SQL and return output"""
        args = ['psql', '-d', self.dbname, '-t', '-A']
        if quiet:
            args.append('-q')

        result = subprocess.run(
            args,
            input=sql.encode(),
            capture_output=True,
            timeout=300
        )

        if result.returncode != 0:
            raise RuntimeError(f"SQL failed: {result.stderr.decode()}\nSQL: {sql[:200]}")

        return result.stdout.decode().strip()

    def execute_timed(self, sql: str) -> float:
        """Execute SQL and return elapsed time in milliseconds"""
        start = time.time()
        self.execute(sql)
        return (time.time() - start) * 1000

    def execute_with_stats(self, sql: str) -> Tuple[str, dict]:
        """Execute with EXPLAIN ANALYZE and return results + stats"""
        explain_sql = f"EXPLAIN (ANALYZE, TIMING ON, BUFFERS ON, FORMAT JSON) {sql}"

        import json
        result = self.execute(explain_sql)

        try:
            data = json.loads(result)
            plan = data[0]['Plan']

            stats = {
                'exec_time': data[0].get('Execution Time', 0),
                'plan_time': data[0].get('Planning Time', 0),
                'shared_hit': plan.get('Shared Hit Blocks', 0),
                'shared_read': plan.get('Shared Read Blocks', 0),
                'rows': plan.get('Actual Rows', 0)
            }

            # Get the actual result
            actual_result = self.execute(sql)

            return actual_result, stats
        except (json.JSONDecodeError, KeyError) as e:
            # Fallback: just time the query
            elapsed = self.execute_timed(sql)
            return '', {
                'exec_time': elapsed,
                'plan_time': 0,
                'shared_hit': 0,
                'shared_read': 0,
                'rows': 0
            }

    def get_relation_size(self, relation: str) -> int:
        """Get size of relation in bytes"""
        result = self.execute(f"SELECT pg_relation_size('{relation}');")
        return int(result) if result and result.strip() else 0


class PostgresEnvironment:
    """Auto-detect PostgreSQL configuration and scale tests accordingly"""

    def __init__(self, db: Optional[DatabaseConnection] = None):
        self.db = db or DatabaseConnection()
        self.shared_buffers_bytes = self._get_shared_buffers()
        self.shared_buffers_mb = self.shared_buffers_bytes / (1024**2)
        self.pg_version = self._get_pg_version()
        self.data_directory = self._get_data_dir()
        self.max_workers = self._get_max_workers()

    def _get_shared_buffers(self) -> int:
        """Get shared_buffers in bytes"""
        result = self.db.execute("SHOW shared_buffers;")
        # Parse result like "64MB" or "8192kB"
        match = re.match(r'(\d+)(kB|MB|GB)?', result)
        if match:
            value = int(match.group(1))
            unit = match.group(2) or 'kB'

            if unit == 'kB':
                return value * 1024
            elif unit == 'MB':
                return value * 1024 * 1024
            elif unit == 'GB':
                return value * 1024 * 1024 * 1024

        # Default fallback
        return 128 * 1024 * 1024

    def _get_pg_version(self) -> str:
        """Get PostgreSQL version"""
        result = self.db.execute("SHOW server_version;")
        return result.split()[0] if result else 'unknown'

    def _get_data_dir(self) -> str:
        """Get PostgreSQL data directory"""
        return self.db.execute("SHOW data_directory;")

    def _get_max_workers(self) -> int:
        """Get max parallel workers"""
        result = self.db.execute("SHOW max_parallel_workers_per_gather;")
        return int(result) if result and result.isdigit() else 0

    def get_test_scale(self) -> str:
        """Returns: 'micro', 'standard', or 'large'"""
        if self.shared_buffers_mb < 256:
            return 'micro'
        elif self.shared_buffers_mb < 2048:
            return 'standard'
        else:
            return 'large'

    def get_scale_params(self) -> dict:
        """Returns dict with row counts, parallelism, cache tests for current scale"""
        scale_configs = {
            'micro': {
                'rows': [100_000, 500_000],
                'parallelism': [0, 2],
                'cache_modes': ['hot'],
                'timeout_minutes': 3,
                'name': 'CI/Small VM'
            },
            'standard': {
                'rows': [1_000_000, 5_000_000],
                'parallelism': [0, 4],
                'cache_modes': ['hot', 'cold'],
                'timeout_minutes': 15,
                'name': 'Developer Workstation'
            },
            'large': {
                'rows': [5_000_000, 20_000_000, 50_000_000],
                'parallelism': [0, 4, 16],
                'cache_modes': ['hot', 'warm', 'cold'],
                'timeout_minutes': 45,
                'name': 'Production-like'
            }
        }
        return scale_configs[self.get_test_scale()]
