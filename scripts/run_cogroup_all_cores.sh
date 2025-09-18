#!/usr/bin/env bash
set -euo pipefail

NP=$(nproc)
ROWS=${SHUFFLE_LOAD_ROWS:-200000}
ITERS=${SHUFFLE_LOAD_ITERS:-1}
IFACE=${IFACE:-}
OUT_DIR=${OUT_DIR:-build}
mkdir -p "$OUT_DIR"

CSV_ONE="$OUT_DIR/jni_cogroup_one_n${NP}.csv"
CSV_TWO="$OUT_DIR/jni_cogroup_two_n${NP}.csv"

echo "[+] Running cogroup one-sided np=$NP rows=$ROWS iters=$ITERS iface='${IFACE}'"
mvn -q -f drsquirrel-java-project/pom.xml -Darrow.version=12.0.0 -P jni-cogroup-load -DskipTests \
  package verify -Dnp=$NP -Drows=$ROWS -Diters=$ITERS -Dout=$(pwd)/$CSV_ONE -Diface="${IFACE}" || true

echo "[+] Running cogroup two-sided np=$NP rows=$ROWS iters=$ITERS iface='${IFACE}'"
mvn -q -f drsquirrel-java-project/pom.xml -Darrow.version=12.0.0 -P jni-cogroup-load -DskipTests \
  package verify -Dnp=$NP -Drows=$ROWS -Diters=$ITERS -Dout=$(pwd)/$CSV_TWO -Dmode=two -Diface="${IFACE}" || true

echo "[+] Consolidated summary"
./scripts/analyze_shuffle_csv.py "$CSV_ONE" "$CSV_TWO"

