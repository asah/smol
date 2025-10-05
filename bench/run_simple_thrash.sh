#!/bin/bash
# ============================================================================
# Simple Thrashing Test with 64MB shared_buffers
# ============================================================================
#
# This script:
#   1. Backs up postgresql.conf
#   2. Sets shared_buffers to 64MB directly in postgresql.conf
#   3. Restarts PostgreSQL
#   4. Runs the thrashing test (10M rows, 1000 distinct keys)
#   5. Analyzes results
#   6. Optionally restores original config
#
# ============================================================================

set -euo pipefail

PGDATA="/var/lib/postgresql/data"
PGCONF="$PGDATA/postgresql.conf"
PSQL="/usr/local/pgsql/bin/psql"

echo "========================================="
echo "Simple Thrashing Test Runner"
echo "========================================="
echo ""

# Backup postgresql.conf
echo "Step 1: Backing up postgresql.conf..."
cp "$PGCONF" "$PGCONF.backup.$(date +%Y%m%d-%H%M%S)"
echo "  ✓ Backup created"
echo ""

# Set shared_buffers to 64MB
echo "Step 2: Setting shared_buffers = 64MB in postgresql.conf..."
sed -i "s/^shared_buffers = .*/shared_buffers = 64MB/" "$PGCONF"
echo "  ✓ Configuration updated"
echo ""

# Restart PostgreSQL
echo "Step 3: Restarting PostgreSQL..."
make stop >/dev/null 2>&1 || true
sleep 2
make start >/dev/null 2>&1
echo "  ✓ PostgreSQL restarted"
echo ""

# Verify setting
ACTUAL_SB=$(su - postgres -c "$PSQL -t -c 'SHOW shared_buffers;'" | tr -d ' ')
echo "  Verified shared_buffers: $ACTUAL_SB"

if [[ "$ACTUAL_SB" != "64MB" ]]; then
    echo "  ✗ ERROR: shared_buffers is $ACTUAL_SB, expected 64MB"
    echo "  Check $PGCONF manually"
    exit 1
fi
echo ""

# Run test
echo "Step 4: Running thrashing test..."
echo "  Dataset: 10M rows, 1000 distinct keys"
echo "  Expected BTREE: ~300 MB (4.7x shared_buffers)"
echo "  Expected SMOL:  ~115 MB (1.8x shared_buffers)"
echo "  Expected runtime: 3-5 minutes"
echo ""

mkdir -p results
LOGFILE="results/thrash_simple_$(date +%Y%m%d-%H%M%S).log"

cd /home/postgres && $PSQL -f bench/thrash_simple.sql 2>&1 | tee "$LOGFILE"

echo ""
echo "========================================="
echo "Step 5: Analyzing Results"
echo "========================================="
echo ""

if command -v python3 >/dev/null 2>&1; then
    python3 bench/analyze_timing.py "$LOGFILE" || echo "Analysis script failed, review log manually"
else
    echo "Python3 not found, review timings manually in log file"
fi

echo ""
echo "========================================="
echo "Test Complete!"
echo "========================================="
echo ""
echo "Log file: $LOGFILE"
echo ""
echo "To restore original shared_buffers:"
echo "  1. Find latest backup: ls -lt $PGCONF.backup.*"
echo "  2. Restore: cp $PGCONF.backup.XXXXXX $PGCONF"
echo "  3. Restart: make stop && make start"
echo ""
