#!/bin/bash
# ============================================================================
# Automated Thrashing Test Runner
# ============================================================================
#
# This script:
#   1. Sets shared_buffers to 256MB (forces cache eviction)
#   2. Restarts PostgreSQL
#   3. Runs the thrashing test
#   4. Analyzes results
#   5. Restores original shared_buffers
#
# Usage:
#   ./bench/run_thrash_test.sh
#
# ============================================================================

set -euo pipefail

PSQL="/usr/local/pgsql/bin/psql"
PG_CTL="/usr/local/pgsql/bin/pg_ctl"
PGDATA="/var/lib/postgresql/data"

echo "========================================="
echo "SMOL Thrashing Test - Automated Runner"
echo "========================================="
echo ""

# Check current shared_buffers
echo "Step 1: Checking current configuration..."
CURRENT_SB=$(su - postgres -c "$PSQL -t -c 'SHOW shared_buffers;'" | tr -d ' ')
echo "  Current shared_buffers: $CURRENT_SB"

if [[ "$CURRENT_SB" == "256MB" ]]; then
    echo "  ✓ Already set to 256MB, no change needed"
    NEED_RESTORE=false
else
    echo "  → Will temporarily change to 256MB for testing"
    NEED_RESTORE=true
    ORIGINAL_SB="$CURRENT_SB"
fi

echo ""

# Set to 256MB if needed
if [[ "$NEED_RESTORE" == "true" ]]; then
    echo "Step 2: Setting shared_buffers to 256MB..."
    su - postgres -c "$PSQL -c \"ALTER SYSTEM SET shared_buffers = '256MB';\""
    echo "  ✓ Configuration updated"
    echo ""

    echo "Step 3: Restarting PostgreSQL..."
    make stop >/dev/null 2>&1 || true
    sleep 2
    make start >/dev/null 2>&1
    echo "  ✓ PostgreSQL restarted"
    echo ""

    # Verify
    NEW_SB=$(su - postgres -c "$PSQL -t -c 'SHOW shared_buffers;'" | tr -d ' ')
    echo "  Verified shared_buffers: $NEW_SB"
    if [[ "$NEW_SB" != "256MB" ]]; then
        echo "  ✗ ERROR: shared_buffers is $NEW_SB, expected 256MB"
        exit 1
    fi
    echo ""
fi

# Run test
echo "Step 4: Running thrashing test..."
echo "  This will take 5-10 minutes..."
echo ""

mkdir -p results
LOGFILE="results/thrash_focused_$(date +%Y%m%d-%H%M%S).log"

cd /home/postgres && $PSQL -f bench/thrash_focused.sql 2>&1 | tee "$LOGFILE"

echo ""
echo "========================================="
echo "Step 5: Analyzing Results"
echo "========================================="
echo ""

python3 bench/analyze_timing.py "$LOGFILE"

echo ""

# Restore original shared_buffers
if [[ "$NEED_RESTORE" == "true" ]]; then
    echo "========================================="
    echo "Step 6: Restoring Original Configuration"
    echo "========================================="
    echo ""
    echo "Restoring shared_buffers to $ORIGINAL_SB..."
    su - postgres -c "$PSQL -c \"ALTER SYSTEM SET shared_buffers = '$ORIGINAL_SB';\""
    echo ""
    echo "⚠️  To apply restored settings, restart PostgreSQL:"
    echo "    make stop && make start"
    echo ""
fi

echo "========================================="
echo "Test Complete!"
echo "========================================="
echo ""
echo "Log file: $LOGFILE"
echo ""
