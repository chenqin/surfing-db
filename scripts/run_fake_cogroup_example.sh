#!/usr/bin/env bash
set -euo pipefail

# Run the fake source/sink cogroup example under MPI.
#
# Usage:
#   scripts/run_fake_cogroup_example.sh [--build] [--np N] [--mode one|two] [--rows N] [--iters N] [--iface IFACE] [--out DIR] [--sort-by FIELD]
#
# Examples:
#   scripts/run_fake_cogroup_example.sh --build --np 4 --mode one --rows 100000 --iters 1
#   IFACE=eth0 scripts/run_fake_cogroup_example.sh --np 2 --mode two --rows 50000 --iters 2

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
BUILD_DIR="$ROOT/build"

NP=2
MODE=one
ROWS=100000
ITERS=1
IFACE=${IFACE:-}
OUT_DIR="$BUILD_DIR/examples/fake-cogroup-out"
DO_BUILD=0
SORT_BY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build) DO_BUILD=1; shift ;;
    --np) NP="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --iters) ITERS="$2"; shift 2 ;;
    --iface) IFACE="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    --sort-by) SORT_BY="$2"; shift 2 ;;
    -h|--help)
      grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if [[ "$DO_BUILD" == "1" ]]; then
  echo "[+] Building native + Java + example"
  CMAKE_EXTRA_FLAGS=-DENABLE_MPI_TESTS=ON "$ROOT/scripts/build_install.sh"
  mvn -q -f "$ROOT/drsquirrel-java-project/pom.xml" -Darrow.version=12.0.0 -DskipTests install
  mvn -q -f "$ROOT/examples/fake-cogroup-app/pom.xml" -DskipTests package
fi

FAT_JAR="$ROOT/drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar"
EX_JAR="$ROOT/examples/fake-cogroup-app/target/fake-cogroup-app-0.1.0-SNAPSHOT.jar"

if [[ ! -f "$FAT_JAR" ]]; then
  echo "[-] Missing $FAT_JAR. Build it first (use --build)."; exit 2
fi
if [[ ! -f "$EX_JAR" ]]; then
  echo "[-] Missing $EX_JAR. Build it first (use --build)."; exit 2
fi

mkdir -p "$OUT_DIR"

IFACE_OPT="--mca btl_tcp_if_exclude lo,docker0"
if [[ -n "${IFACE}" ]]; then
  IFACE_OPT="--mca btl_tcp_if_include ${IFACE}"
fi

echo "[+] Running fake-cogroup-app np=$NP mode=$MODE rows=$ROWS iters=$ITERS out=$OUT_DIR sort-by='${SORT_BY}' iface='${IFACE}'"
export LD_LIBRARY_PATH="$BUILD_DIR:${LD_LIBRARY_PATH:-}"

mpiexec -np "$NP" --use-hwthread-cpus --oversubscribe --map-by core --bind-to core \
  ${IFACE_OPT} \
  java -Djava.library.path="$BUILD_DIR" \
       -cp "$FAT_JAR:$EX_JAR" \
       com.example.fakecogroup.Main \
       --mode "$MODE" --rows "$ROWS" --iters "$ITERS" --out "$OUT_DIR" ${SORT_BY:+--sort-by "$SORT_BY"}

echo "[+] Done. Samples written to: $OUT_DIR"
