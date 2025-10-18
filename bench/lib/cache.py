"""Cache control - portable, no sudo required"""


class CacheController:
    """Portable cache management (no sudo required)"""

    def __init__(self, db):
        self.db = db
        self.has_pg_prewarm = self._check_pg_prewarm()
        self.has_buffercache = self._check_pg_buffercache()

    def _check_pg_prewarm(self) -> bool:
        """Check if pg_prewarm extension is available"""
        try:
            result = self.db.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_prewarm';")
            return bool(result.strip())
        except:
            return False

    def _check_pg_buffercache(self) -> bool:
        """Check if pg_buffercache extension is available"""
        try:
            result = self.db.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_buffercache';")
            return bool(result.strip())
        except:
            return False

    def flush_relation(self, relation: str, mode: str = 'hot'):
        """
        Flush caches for a relation

        Modes:
        - 'hot': No flush (everything in cache)
        - 'warm': CHECKPOINT + DISCARD PLANS (partial eviction)
        - 'cold': Best-effort eviction using available extensions
        """
        if mode == 'hot':
            return

        # Always checkpoint and discard plans
        self.db.execute("CHECKPOINT;")
        self.db.execute("DISCARD PLANS;")

        if mode == 'cold':
            if self.has_buffercache:
                # Use pg_buffercache to evict if available
                try:
                    self.db.execute(
                        f"SELECT pg_buffercache_evict_relation('{relation}'::regclass);"
                    )
                except:
                    pass  # Ignore errors, best effort

            # Additional cache stress to help eviction
            try:
                # Touch all pages then checkpoint to simulate cold start
                self.db.execute(f"SELECT count(*) FROM {relation};")
                self.db.execute("CHECKPOINT;")
            except:
                pass  # Best effort
