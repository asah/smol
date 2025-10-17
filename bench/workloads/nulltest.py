"""NULL rejection test - document SMOL's limitation"""

from bench.workloads.base import WorkloadBase
from bench.lib.metrics import Query, WorkloadResult
from typing import List


class NullRejectionWorkload(WorkloadBase):
    """Test that SMOL rejects NULL values (important limitation)"""

    def get_workload_id(self) -> str:
        return "null_rejection"

    def get_description(self) -> str:
        return "NULL rejection test (SMOL limitation)"

    def get_index_columns(self) -> str:
        return 'id'

    def generate_data(self):
        """Generate small table with NULLs"""
        sql = f"""
        DROP TABLE IF EXISTS {self.table_name} CASCADE;

        CREATE UNLOGGED TABLE {self.table_name} AS
        SELECT
            CASE WHEN i % 10 = 0 THEN NULL ELSE i END AS id,
            i AS value
        FROM generate_series(1, 1000) i;

        ALTER TABLE {self.table_name} SET (autovacuum_enabled = off);
        VACUUM (FREEZE, ANALYZE) {self.table_name};
        """
        self.db.execute(sql)

    def get_queries(self) -> List[Query]:
        # No actual queries - this is a build-time test
        return []

    def run(self) -> List[WorkloadResult]:
        """Override run to test NULL rejection during index build"""
        results = []

        print(f"\n{'='*70}")
        print(f"Workload: {self.get_description()}")
        print(f"{'='*70}")

        # Generate data
        print(f"Generating data with NULLs...", end='', flush=True)
        self.generate_data()
        print(" done")

        # Test BTREE (should succeed)
        print(f"\nTesting BTREE:")
        print(f"  Building index...", end='', flush=True)
        try:
            build_time = self.build_index('btree')
            print(f" ✓ {build_time:.0f}ms (accepts NULLs)")
        except Exception as e:
            print(f" ✗ ERROR: {e}")

        # Test SMOL (should fail with clear error)
        print(f"\nTesting SMOL:")
        print(f"  Building index...", end='', flush=True)
        try:
            build_time = self.build_index('smol')
            print(f" ✗ UNEXPECTED: SMOL accepted NULLs (should reject!)")
        except Exception as e:
            error_msg = str(e)
            if 'NULL' in error_msg.upper():
                print(f" ✓ Correctly rejected NULLs")
                print(f"    Error: {error_msg[:100]}...")
            else:
                print(f" ✗ Wrong error: {error_msg[:100]}")

        # Cleanup
        self.cleanup()

        # Return empty results (this is a pass/fail test)
        return results
