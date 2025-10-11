#!/bin/bash
# Calculate coverage excluding GCOV_EXCL marked lines

# Total executable lines (not marked with -)
total_lines=$(grep -E "^ *[0-9#]+:" smol.c.gcov | wc -l)

# Uncovered lines (marked with #####)
uncovered_lines=$(grep "^ *#####:" smol.c.gcov | wc -l)

# Uncovered lines that have GCOV_EXCL marker (should be excluded)
excluded_uncovered=$(grep "^ *#####:" smol.c.gcov | grep -c "GCOV_EXCL")

# Covered lines that have GCOV_EXCL (these would be in GCOV_EXCL_START/STOP blocks that ARE executed)
# We need to find lines with execution counts that have GCOV_EXCL
covered_excl=$(grep -E "^ *[0-9]+\*?:" smol.c.gcov | grep -c "GCOV_EXCL" || echo 0)

# Calculate adjusted metrics
adjusted_total=$((total_lines - excluded_uncovered - covered_excl))
adjusted_uncovered=$((uncovered_lines - excluded_uncovered))
adjusted_covered=$((adjusted_total - adjusted_uncovered))

# Calculate percentage
if [ $adjusted_total -gt 0 ]; then
    pct=$(echo "scale=2; $adjusted_covered * 100 / $adjusted_total" | bc)
else
    pct=0
fi

echo "=== Coverage Report (excluding GCOV_EXCL) ==="
echo "Total executable lines: $total_lines"
echo "Covered lines: $((total_lines - uncovered_lines))"
echo "Uncovered lines: $uncovered_lines"
echo "Lines marked GCOV_EXCL (uncovered): $excluded_uncovered"
echo "Lines marked GCOV_EXCL (covered): $covered_excl"
echo ""
echo "Adjusted total lines: $adjusted_total"
echo "Adjusted uncovered lines: $adjusted_uncovered"
echo "Adjusted covered lines: $adjusted_covered"
echo "Coverage: ${pct}%"
