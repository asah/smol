#!/bin/bash
# Coverage analysis script - identifies missing test scenarios

if [ ! -f smol.c.gcov ]; then
    echo "ERROR: smol.c.gcov not found. Run 'make coverage' first."
    exit 1
fi

echo "=== SMOL CODE COVERAGE ANALYSIS ==="
echo ""

# Overall stats (excluding debug logging lines with SMOL_LOGF)
total_lines=$(awk '/^[ ]*[0-9]+:/ || /^[ ]*#####:/ {if ($0 !~ /SMOL_LOGF/ && $0 !~ /INSTR_TIME/) count++} END {print count}' smol.c.gcov)
covered_lines=$(awk '/^[ ]*[0-9]+:/ {if ($0 !~ /SMOL_LOGF/ && $0 !~ /INSTR_TIME/) count++} END {print count}' smol.c.gcov)
uncovered_lines=$(awk '/^[ ]*#####:/ {if ($0 !~ /SMOL_LOGF/ && $0 !~ /INSTR_TIME/) count++} END {print count}' smol.c.gcov)
debug_lines=$(awk '/^[ ]*#####:/ && ($0 ~ /SMOL_LOGF/ || $0 ~ /INSTR_TIME/) {count++} END {print count}' smol.c.gcov)
coverage_pct=$(awk "BEGIN {if ($total_lines > 0) printf \"%.2f\", ($covered_lines * 100.0) / $total_lines; else print 0}")

echo "Total executable lines: $total_lines (excluding $debug_lines debug logging lines)"
echo "Covered lines: $covered_lines"
echo "Uncovered lines: $uncovered_lines"
echo "Coverage: $coverage_pct%"
echo ""

# Analyze uncovered sections by function
echo "=== UNCOVERED CODE BY CATEGORY ==="
echo ""

# Error handling paths
echo "1. ERROR PATHS (validation/error handling):"
grep -n "^[ ]*#####:.*ereport(ERROR" smol.c.gcov | wc -l | xargs echo "   Uncovered error paths:"

# Backward scans
echo ""
echo "2. BACKWARD SCANS:"
grep -n "^[ ]*#####:" smol.c.gcov | grep -E "(backward|DESC|rightmost)" | wc -l | xargs echo "   Uncovered backward scan lines:"

# Parallel scans
echo ""
echo "3. PARALLEL SCANS:"
grep -n "^[ ]*#####:" smol.c.gcov | grep -i "parallel" | wc -l | xargs echo "   Uncovered parallel scan lines:"

# Debug logging (excluded from coverage calculation)
echo ""
echo "4. DEBUG LOGGING (excluded from coverage):"
echo "   Debug logging lines (SMOL_LOGF/INSTR_TIME): $debug_lines (not counted)"

# Two-column specific paths
echo ""
echo "5. TWO-COLUMN INDEX FEATURES:"
grep -n "^[ ]*#####:" smol.c.gcov | grep -E "(two_col|k2_eq|second.*key)" | wc -l | xargs echo "   Uncovered two-column lines:"

# Include column edge cases
echo ""
echo "6. INCLUDE COLUMN EDGE CASES:"
grep -n "^[ ]*#####:" smol.c.gcov | grep -i "include" | wc -l | xargs echo "   Uncovered INCLUDE lines:"

echo ""
echo "=== TOP MISSING TEST SCENARIOS ==="
echo ""
echo "Based on uncovered lines, add tests for:"
echo ""

# Check specific uncovered features
if grep -q "^[ ]*#####:.*smol prototype supports 1 or 2 key columns only" smol.c.gcov; then
    echo "  ❌ 3+ column index validation (line 678)"
fi

if grep -q "^[ ]*#####:.*INCLUDE columns currently supported only for single-key" smol.c.gcov; then
    echo "  ❌ Two-column + INCLUDE validation (line 706)"
fi

if grep -q "^[ ]*#####:.*rightmost_leaf" smol.c.gcov; then
    echo "  ❌ Backward scan / DESC ORDER BY queries"
fi

if grep -q "^[ ]*#####:.*parallel.*claim" smol.c.gcov; then
    echo "  ❌ Parallel scans with >1 worker (current test may use 0 workers)"
fi

if grep -q "^[ ]*#####:.*DatumGetChar" smol.c.gcov; then
    echo "  ❌ char (1-byte) key type"
fi

if grep -q "^[ ]*#####:.*DatumGetInt16.*key" smol.c.gcov; then
    echo "  ❌ int2 (int16) key scenarios"
fi

if grep -q "^[ ]*#####:.*k2_eq.*Int64" smol.c.gcov; then
    echo "  ❌ Two-column index with int8 second key"
fi

echo ""
echo "=== DETAILED UNCOVERED LINES (first 30, excluding debug) ==="
echo ""
awk '/^[ ]*#####:/ {if ($0 !~ /SMOL_LOGF/ && $0 !~ /INSTR_TIME/) {line=substr($0, index($0, ":")+1); sub(/^[ ]*[0-9]+:/, "", line); printf "%6d: %s\n", NR, line}}' smol.c.gcov | head -30

echo ""
echo "Full details in: smol.c.gcov"
