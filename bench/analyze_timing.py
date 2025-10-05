#!/usr/bin/env python3
"""
Parse psql output with \\timing on and calculate average query latencies.

Usage:
    psql -f bench/thrash_focused.sql 2>&1 | tee results/thrash.log
    python3 bench/analyze_timing.py results/thrash.log
"""

import sys
import re

def parse_timings(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()

    btree_times = []
    smol_times = []
    current_index = None
    in_query = False

    for i, line in enumerate(lines):
        # Detect which index we're testing
        if 'Phase 3: BTREE Thrashing Test' in line or 'PHASE 4: BTREE Thrashing Test' in line:
            current_index = 'btree'
        elif 'Phase 4: SMOL Thrashing Test' in line or 'PHASE 5: SMOL Thrashing Test' in line:
            current_index = 'smol'

        # Detect query start
        if 'BTREE Query' in line or 'SMOL Query' in line:
            in_query = True
            continue

        # Parse timing lines after query (format: "Time: 123.456 ms")
        if in_query and current_index:
            match = re.search(r'Time:\s+([\d.]+)\s+ms', line)
            if match:
                time_ms = float(match.group(1))
                # Only count query times (skip small utility times)
                if time_ms > 10:  # Query times should be > 10ms
                    if current_index == 'btree':
                        btree_times.append(time_ms)
                    elif current_index == 'smol':
                        smol_times.append(time_ms)
                in_query = False

    return btree_times, smol_times

def analyze(btree_times, smol_times):
    if not btree_times or not smol_times:
        print("ERROR: No timing data found!")
        print(f"BTREE queries: {len(btree_times)}")
        print(f"SMOL queries: {len(smol_times)}")
        return

    print("=" * 60)
    print("THRASHING TEST ANALYSIS")
    print("=" * 60)
    print()

    print(f"BTREE Queries: {len(btree_times)}")
    print(f"  First 5:  {', '.join(f'{t:.2f}' for t in btree_times[:5])} ms")
    print(f"  Last 5:   {', '.join(f'{t:.2f}' for t in btree_times[-5:])} ms")
    print(f"  Average (all):     {sum(btree_times) / len(btree_times):.2f} ms")
    print(f"  Average (last 10): {sum(btree_times[-10:]) / len(btree_times[-10:]):.2f} ms")
    print()

    print(f"SMOL Queries: {len(smol_times)}")
    print(f"  First 5:  {', '.join(f'{t:.2f}' for t in smol_times[:5])} ms")
    print(f"  Last 5:   {', '.join(f'{t:.2f}' for t in smol_times[-5:])} ms")
    print(f"  Average (all):     {sum(smol_times) / len(smol_times):.2f} ms")
    print(f"  Average (last 10): {sum(smol_times[-10:]) / len(smol_times[-10:]):.2f} ms")
    print()

    # Calculate speedup
    btree_avg = sum(btree_times[-10:]) / len(btree_times[-10:])
    smol_avg = sum(smol_times[-10:]) / len(smol_times[-10:])
    speedup = btree_avg / smol_avg

    print("=" * 60)
    print(f"SPEEDUP (last 10 queries): {speedup:.2f}x")
    print("=" * 60)
    print()

    if speedup < 1.5:
        print("⚠️  WARNING: Speedup is less than 1.5x!")
        print()
        print("Possible reasons:")
        print("  1. Indexes are fitting in shared_buffers (no thrashing)")
        print("  2. Run-detection overhead dominates (SMOL bottleneck)")
        print("  3. Need lower shared_buffers to force cache eviction")
        print()
        print("Recommendations:")
        print("  - Check index sizes vs shared_buffers")
        print("  - Try: ALTER SYSTEM SET shared_buffers = '128MB';")
        print("  - Restart PostgreSQL and re-run test")
    else:
        print(f"✓ SUCCESS: SMOL is {speedup:.2f}x faster!")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <log_file>")
        sys.exit(1)

    btree_times, smol_times = parse_timings(sys.argv[1])
    analyze(btree_times, smol_times)
