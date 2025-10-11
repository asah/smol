#!/bin/bash
# Calculate true coverage excluding GCOV_EXCL marked lines

# Count lines between GCOV_EXCL_START and GCOV_EXCL_STOP
awk '
/GCOV_EXCL_START/ { excl=1 }
/GCOV_EXCL_STOP/ { excl=0; next }
excl && /^ *#####:/ { excluded_uncov++ }
excl && /^ *[0-9]+\*?:/ { excluded_cov++ }
!excl && /^ *#####:/ && !/GCOV_EXCL/ { uncovered++ }
!excl && /^ *[0-9]+\*?:/ && !/GCOV_EXCL/ { covered++ }
/GCOV_EXCL_LINE/ && /^ *#####:/ { excluded_uncov_line++ }
/GCOV_EXCL_LINE/ && /^ *[0-9]+\*?:/ { excluded_cov_line++ }
END {
    total_excluded = excluded_uncov + excluded_cov + excluded_uncov_line + excluded_cov_line
    total_measured = uncovered + covered
    total_lines = total_measured + total_excluded

    if (total_measured > 0) {
        pct = (covered * 100.0) / total_measured
    } else {
        pct = 0
    }

    printf "Total lines: %d\n", total_lines
    printf "Excluded (GCOV_EXCL): %d\n", total_excluded
    printf "  - Excluded uncovered: %d\n", excluded_uncov + excluded_uncov_line
    printf "  - Excluded covered: %d\n", excluded_cov + excluded_cov_line
    printf "Measured lines: %d\n", total_measured
    printf "Covered: %d\n", covered
    printf "Uncovered: %d\n", uncovered
    printf "Coverage: %.2f%%\n", pct
}
' smol.c.gcov
