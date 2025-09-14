#!/usr/bin/env bash
set -euo pipefail

# Optional envs: SHUFFLE_LOAD_ROWS, SHUFFLE_LOAD_ITERS, IFACE, OUT_DIR
NP=$(nproc)
OUT_DIR=${OUT_DIR:-build}
mkdir -p "$OUT_DIR"

echo "[+] Running shuffle (one- and two-sided) on $NP ranks"
./scripts/run_shuffle_all_cores.sh || true

echo "[+] Running cogroup (one- and two-sided) on $NP ranks"
./scripts/run_cogroup_all_cores.sh || true

echo "[+] Combined summary"
./scripts/analyze_shuffle_csv.py \
  "$OUT_DIR/jni_shuffle_one_n${NP}.csv" \
  "$OUT_DIR/jni_shuffle_two_n${NP}.csv" \
  "$OUT_DIR/jni_cogroup_one_n${NP}.csv" \
  "$OUT_DIR/jni_cogroup_two_n${NP}.csv"

