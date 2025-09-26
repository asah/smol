#!/usr/bin/env python3
import csv, os, sys

rows = os.environ.get('ROWS')
workers = os.environ.get('WORKERS')
inc = os.environ.get('INC')
csv_path = os.environ.get('CSV', 'results/rle_adv.csv')

def load():
    with open(csv_path, newline='') as f:
        r = csv.DictReader(f)
        return list(r)

def filt(recs):
    out = []
    for rec in recs:
        if rows and rec['rows'] != rows: continue
        if workers and rec['workers'] != workers: continue
        if inc and rec['inc'] != inc: continue
        out.append(rec)
    return out

def group_key(rec):
    return (rec['dup_frac'], rec['query'])

def main():
    try:
        recs = load()
    except FileNotFoundError:
        print(f"CSV not found: {csv_path}")
        sys.exit(2)
    recs = filt(recs)
    if not recs:
        print("No matching records. Set ROWS/WORKERS/INC or check CSV path.")
        sys.exit(1)
    # Group by dup_frac,query then pick btree+smol
    by = {}
    for r in recs:
        k = group_key(r)
        by.setdefault(k, {})[r['index']] = r
    keys = sorted(by.keys(), key=lambda k:(float(k[0]), k[1]))
    print(f"rows={rows or 'ANY'} workers={workers or 'ANY'} inc={inc or 'ANY'}")
    print("dup_frac, query, btree_ms, smol_ms, speedup(btree/smol), btree_size_MB, smol_size_MB")
    for k in keys:
        d = by[k]
        if 'btree' not in d or 'smol' not in d: continue
        b = d['btree']; s = d['smol']
        b_ms = float(b['exec_ms']); s_ms = float(s['exec_ms'])
        try:
            b_sz = int(b['size_bytes'])/1048576.0
            s_sz = int(s['size_bytes'])/1048576.0
        except ValueError:
            b_sz = s_sz = 0.0
        sp = b_ms/s_ms if s_ms>0 else float('inf')
        print(f"{k[0]}, {k[1]}, {b_ms:.3f}, {s_ms:.3f}, {sp:.2f}x, {b_sz:.1f}, {s_sz:.1f}")

if __name__ == '__main__':
    main()

