#!/usr/bin/env python3
"""Summarize shuffle load CSV metrics.

Input CSV columns: run_id,ts_epoch,mode,world,rows_per_rank,iter,rows,time_s,rows_per_sec

Usage:
  ./scripts/analyze_shuffle_csv.py build/shuffle_load.csv [--group-by mode,world]
"""
import argparse
import csv
from collections import defaultdict

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('csv_path', nargs='+')
    ap.add_argument('--group-by', default='mode,world')
    return ap.parse_args()

def main():
    args = parse_args()
    group_keys = [k.strip() for k in args.group_by.split(',') if k.strip()]
    rows = []
    for path in args.csv_path:
        try:
            with open(path, 'r') as f:
                for r in csv.DictReader(f):
                    rows.append(r)
        except FileNotFoundError:
            continue
    # Filter out any stray header rows accidentally appended mid-file
    clean_rows = []
    for r in rows:
        try:
            float(r.get('time_s', ''))
            float(r.get('rows_per_sec', ''))
        except Exception:
            continue
        clean_rows.append(r)
    groups = defaultdict(list)
    for r in clean_rows:
        key = tuple(r.get(k, '') for k in group_keys)
        groups[key].append(r)
    print(f"Groups by {group_keys} -> {len(groups)} groups")
    for key, lst in groups.items():
        times = [float(r['time_s']) for r in lst]
        rps = [float(r['rows_per_sec']) for r in lst]
        world = lst[0].get('world')
        rows_per_rank = lst[0].get('rows_per_rank')
        total_rows = lst[0].get('rows')
        print(f"- {key}: runs={len(lst)} world={world} rows_per_rank={rows_per_rank} rows={total_rows}")
        print(f"  best_s={min(times):.6f} avg_s={sum(times)/len(times):.6f} peak_rps={max(rps):.0f} avg_rps={sum(rps)/len(rps):.0f}")

if __name__ == '__main__':
    main()
