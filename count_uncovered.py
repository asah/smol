#!/usr/bin/env python3
"""
Count uncovered lines in smol.c.gcov, excluding GCOV_EXCL regions.

GCOV_EXCL patterns:
- Single-line: /* GCOV_EXCL_LINE */
- Multi-line: /* GCOV_EXCL_START */ ... /* GCOV_EXCL_STOP */
"""

import sys
import re

def count_uncovered_lines(gcov_file):
    uncovered = []
    in_excl_region = False

    with open(gcov_file, 'r') as f:
        for line in f:
            # Check for GCOV_EXCL markers
            if 'GCOV_EXCL_START' in line:
                in_excl_region = True
            elif 'GCOV_EXCL_STOP' in line:
                in_excl_region = False
                continue

            # Skip lines in exclusion regions
            if in_excl_region:
                continue

            # Skip lines with GCOV_EXCL_LINE marker
            if 'GCOV_EXCL_LINE' in line:
                continue

            # Check if line is uncovered
            if line.strip().startswith('#####:'):
                match = re.match(r'\s*#####:\s*(\d+):(.*)', line)
                if match:
                    linenum = int(match.group(1))
                    content = match.group(2).strip()
                    uncovered.append((linenum, content))

    return uncovered

def main():
    if len(sys.argv) < 2:
        print("Usage: count_uncovered.py <gcov_file>")
        sys.exit(1)

    gcov_file = sys.argv[1]
    uncovered = count_uncovered_lines(gcov_file)

    print(f"=== Uncovered Lines (excluding GCOV_EXCL regions) ===")
    print(f"Total: {len(uncovered)} lines")
    print()

    # Group by ranges for readability
    if uncovered:
        print("Line numbers:")
        ranges = []
        start = uncovered[0][0]
        prev = start

        for linenum, _ in uncovered[1:]:
            if linenum == prev + 1:
                prev = linenum
            else:
                if start == prev:
                    ranges.append(str(start))
                else:
                    ranges.append(f"{start}-{prev}")
                start = linenum
                prev = linenum

        # Add last range
        if start == prev:
            ranges.append(str(start))
        else:
            ranges.append(f"{start}-{prev}")

        # Print in groups of 10
        for i in range(0, len(ranges), 10):
            print("  " + ", ".join(ranges[i:i+10]))

if __name__ == "__main__":
    main()
