#!/usr/bin/env python3
import csv, os, sys

rows = os.environ.get('ROWS')
workers = os.environ.get('WORKERS')
inc = os.environ.get('INC')
csv_path = os.environ.get('CSV', 'results/text_adv.csv')
out_path_env = os.environ.get('OUT', None)

def load():
    with open(csv_path, newline='') as f:
        return list(csv.DictReader(f))

def filt(recs):
    out = []
    for r in recs:
        if rows and r['rows'] != rows: continue
        if workers and r['workers'] != workers: continue
        if inc and r['inc'] != inc: continue
        out.append(r)
    return out

def main():
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed", file=sys.stderr)
        sys.exit(2)
    try:
        recs = load()
    except FileNotFoundError:
        print(f"CSV not found: {csv_path}")
        sys.exit(2)
    recs = filt(recs)
    if not recs:
        print("No matching records.")
        sys.exit(1)
    # group by strlen
    by = {}
    for r in recs:
        by.setdefault(r['strlen'], {})[r['index']] = float(r['count_ms'])
    lens = sorted(by.keys(), key=lambda x:int(x))
    x = list(range(len(lens)))
    bt = [by[l].get('btree', float('nan')) for l in lens]
    sm = [by[l].get('smol', float('nan')) for l in lens]
    fig, ax = plt.subplots(figsize=(8,4))
    w = 0.35
    ax.bar([i-w/2 for i in x], bt, width=w, label='btree')
    ax.bar([i+w/2 for i in x], sm, width=w, label='smol')
    ax.set_xticks(x)
    ax.set_xticklabels(lens)
    ax.set_xlabel('string length (bytes)')
    ax.set_ylabel('COUNT ms')
    ax.set_title(f"text COUNT (rows={rows or 'ANY'}, workers={workers or 'ANY'}, inc={inc or 'ANY'})")
    ax.legend()
    plt.tight_layout()
    out = out_path_env or f"results/text_adv_{rows or 'any'}_{workers or 'wk'}_{inc or 'inc'}.png"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    plt.savefig(out, dpi=120)
    print(f"Saved plot to {out}")

if __name__ == '__main__':
    main()

