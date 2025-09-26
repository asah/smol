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

def normalize(rec):
    # Support both old and new schemas
    if 'uniqvals' in rec and 'distribution' in rec:
        return {
            'rows': rec.get('rows',''), 'workers': rec.get('workers',''), 'inc': rec.get('inc',''),
            'uniqvals': rec.get('uniqvals',''), 'distribution': rec.get('distribution',''),
            'index': rec.get('index',''), 'query': rec.get('query',''),
            'size_bytes': rec.get('size_bytes',''), 'exec_ms': rec.get('exec_ms','')
        }
    else:
        # legacy: dup_frac only
        return {
            'rows': rec.get('rows',''), 'workers': rec.get('workers',''), 'inc': rec.get('inc',''),
            'uniqvals': 'NA', 'distribution': 'heavy',
            'index': rec.get('index',''), 'query': rec.get('query',''),
            'size_bytes': rec.get('size_bytes',''), 'exec_ms': rec.get('exec_ms','')
        }

def main():
    try:
        recs = load()
    except FileNotFoundError:
        print(f"CSV not found: {csv_path}")
        sys.exit(2)
    recs = [normalize(r) for r in filt(recs)]
    if not recs:
        print("No matching records. Set ROWS/WORKERS/INC or check CSV path.")
        sys.exit(1)
    # Group by (uniqvals,distribution,query) and pick btree+smol
    by = {}
    for r in recs:
        k = (r['uniqvals'], r['distribution'], r['query'])
        by.setdefault(k, {})[r['index']] = r
    def keyconv(k):
        uv = k[0]
        try:
            uvn = float(uv)
        except:
            uvn = 0.0
        return (uvn, k[1], k[2])
    keys = sorted(by.keys(), key=keyconv)
    print(f"rows={rows or 'ANY'} workers={workers or 'ANY'} inc={inc or 'ANY'}")
    print("uniqvals, distribution, query, btree_ms, smol_ms, speedup, btree_MB, smol_MB")
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
        print(f"{k[0]}, {k[1]}, {k[2]}, {b_ms:.3f}, {s_ms:.3f}, {sp:.2f}x, {b_sz:.1f}, {s_sz:.1f}")

if __name__ == '__main__':
    main()
