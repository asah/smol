#!/usr/bin/env python3
import csv, os, sys

rows = os.environ.get('ROWS')
workers = os.environ.get('WORKERS')
inc = os.environ.get('INC')
csv_path = os.environ.get('CSV', 'results/rle_adv.csv')
out_path_env = os.environ.get('OUT', None)

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

def main():
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed. Try: python3 -m pip install --user matplotlib", file=sys.stderr)
        sys.exit(2)
    try:
        recs = load()
    except FileNotFoundError:
        print(f"CSV not found: {csv_path}")
        sys.exit(2)
    recs = filt(recs)
    if not recs:
        print("No matching records. Set ROWS/WORKERS/INC or check CSV path.")
        sys.exit(1)
    # Build series per query
    series = {}
    for r in recs:
        # Support new schema; fall back to dup_frac for legacy
        uv = r.get('uniqvals', r.get('dup_frac','NA'))
        key = (uv, r['query'])
        series.setdefault(key, {})[r['index']] = float(r.get('exec_ms', r.get('count_ms','nan')))
    uniqs = sorted({k[0] for k in series.keys()}, key=lambda x: (float(x) if x!='NA' else -1))
    queries = sorted({k[1] for k in series.keys()})
    fig, axes = plt.subplots(1, len(queries), figsize=(6*len(queries),4), squeeze=False)
    for idx, q in enumerate(queries):
        x = list(range(len(uniqs)))
        bt = [series[(d,q)].get('btree', float('nan')) for d in uniqs]
        sm = [series[(d,q)].get('smol', float('nan')) for d in uniqs]
        ax = axes[0][idx]
        w = 0.35
        ax.bar([i-w/2 for i in x], bt, width=w, label='btree')
        ax.bar([i+w/2 for i in x], sm, width=w, label='smol')
        ax.set_xticks(x)
        ax.set_xticklabels(uniqs)
        ax.set_title(f"{q} (rows={rows or 'ANY'}, inc={inc or 'ANY'}, workers={workers or 'ANY'})")
        ax.set_ylabel('ms')
        ax.set_xlabel('uniqvals')
        ax.legend()
    plt.tight_layout()
    out = out_path_env or f"results/rle_adv_{rows or 'any'}_{inc or 'inc'}_{workers or 'wk'}.png"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, dpi=120)
    print(f"Saved plot to {out}")

if __name__ == '__main__':
    main()
