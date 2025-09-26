#!/usr/bin/env python3
import csv, os, sys

rows = os.environ.get('ROWS')
workers = os.environ.get('WORKERS')
inc = os.environ.get('INC')
csv_path = os.environ.get('CSV', 'results/text_adv.csv')

def load():
    with open(csv_path, newline='') as f:
        return list(csv.DictReader(f))

def main():
    try:
        recs = load()
    except FileNotFoundError:
        print(f"CSV not found: {csv_path}")
        sys.exit(2)
    # filter
    fr = []
    for r in recs:
        if rows and r['rows'] != rows: continue
        if workers and r['workers'] != workers: continue
        if inc and r['inc'] != inc: continue
        fr.append(r)
    if not fr:
        print("No matching records. Set ROWS/WORKERS/INC or check CSV path.")
        sys.exit(1)
    # group by strlen
    by = {}
    for r in fr:
        k = r['strlen']
        by.setdefault(k, {})[r['index']] = r
    # Note: text CSV includes uniqvals and distribution per row; we summarize per length
    print(f"rows={rows or 'ANY'} workers={workers or 'ANY'} inc={inc or 'ANY'}")
    print("strlen, uniqvals, distribution, btree_ms, smol_ms, speedup, btree_MB, smol_MB")
    for strlen in sorted(by.keys(), key=lambda x:int(x)):
        d = by[strlen]
        if 'btree' not in d or 'smol' not in d: continue
        b = d['btree']; s = d['smol']
        b_ms = float(b['count_ms']); s_ms = float(s['count_ms'])
        b_sz = int(b['size_bytes'])/1048576.0
        s_sz = int(s['size_bytes'])/1048576.0
        sp = b_ms/s_ms if s_ms>0 else float('inf')
        print(f"{strlen}, {b['uniqvals']}, {b.get('distribution','')}, {b_ms:.3f}, {s_ms:.3f}, {sp:.2f}x, {b_sz:.1f}, {s_sz:.1f}")

if __name__ == '__main__':
    main()
