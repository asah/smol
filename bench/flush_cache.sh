#!/bin/bash
# Flush PostgreSQL and OS caches for cold cache benchmarking
# Uses pg_buffercache_evict_relation() and vmtouch for targeted eviction

set -e

RELATION="${1:-}"

if [ -z "$RELATION" ]; then
    echo "Usage: $0 <relation_name>"
    echo "  Evicts specified relation from PostgreSQL buffers and OS cache"
    exit 1
fi

echo "Flushing relation: $RELATION"

# Step 1: Flush dirty buffers to disk
echo "  1. CHECKPOINT (flush dirty buffers)..."
psql -c "CHECKPOINT;" >/dev/null

# Step 2: Evict from PostgreSQL shared_buffers using pg_buffercache
echo "  2. Evicting from PostgreSQL shared_buffers..."
EVICT_RESULT=$(psql -t -A -c "SELECT buffers_evicted FROM pg_buffercache_evict_relation('$RELATION'::regclass);" 2>&1)

if echo "$EVICT_RESULT" | grep -q "ERROR"; then
    echo "  ⚠ Could not evict from shared_buffers (relation may not exist)"
else
    BUFFERS_EVICTED=$(echo "$EVICT_RESULT" | head -1)
    echo "  ✓ Evicted $BUFFERS_EVICTED buffers from shared_buffers"
fi

# Step 3: Evict from OS page cache using vmtouch via evict_pg_relations.sh
echo "  3. Evicting from OS page cache..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -x "$SCRIPT_DIR/evict_pg_relations.sh" ]; then
    "$SCRIPT_DIR/evict_pg_relations.sh" "$RELATION" 2>&1 | sed 's/^/    /'
    echo "  ✓ OS cache eviction complete"
else
    echo "  ⚠ evict_pg_relations.sh not found, skipping OS cache eviction"
fi

echo "✓ Cache flush complete for $RELATION"
