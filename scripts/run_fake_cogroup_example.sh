#!/usr/bin/env bash
set -euo pipefail

# Run the fake source/sink cogroup example under MPI.
#
# Usage:
#   scripts/run_fake_cogroup_example.sh [--build] [--np N] [--mode one|two] [--rows N] [--iters N] [--iface IFACE] [--out DIR] [--sort-by FIELD] \
#       [--thrift-path PATH --thrift-struct NAME --payload-left FILE [--payload-right FILE] [--key-field FIELD]] [--deep-thrift [--deep-rows N]]
#
# Examples:
#   scripts/run_fake_cogroup_example.sh --build --np 4 --mode one --rows 100000 --iters 1
#   IFACE=eth0 scripts/run_fake_cogroup_example.sh --np 2 --mode two --rows 50000 --iters 2
#   scripts/run_fake_cogroup_example.sh --deep-thrift --iters 2 --np 4 --sort-by event_id

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
BUILD_DIR="$ROOT/build"
JAVA_HOME_DEFAULT="$ROOT/.jdks/jdk-11.0.2"
if [ -d "$JAVA_HOME_DEFAULT" ]; then
  export JAVA_HOME="$JAVA_HOME_DEFAULT"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

NP=2
MODE=one
ROWS=100000
ITERS=1
IFACE=${IFACE:-}
OUT_DIR="$BUILD_DIR/examples/fake-cogroup-out"
DO_BUILD=0
SORT_BY=""
USE_THRIFT=0
THRIFT_PATH=""
THRIFT_STRUCT=""
PAYLOAD_LEFT=""
PAYLOAD_RIGHT=""
KEY_FIELD=""
DEEP_THRIFT=0
DEEP_ROWS=20000
DEEP_LEFT_SEED=12345
DEEP_RIGHT_SEED=67890

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
    --thrift-path) THRIFT_PATH="$2"; USE_THRIFT=1; shift 2 ;;
    --thrift-struct) THRIFT_STRUCT="$2"; USE_THRIFT=1; shift 2 ;;
    --payload-left) PAYLOAD_LEFT="$2"; USE_THRIFT=1; shift 2 ;;
    --payload-right) PAYLOAD_RIGHT="$2"; USE_THRIFT=1; shift 2 ;;
    --key-field) KEY_FIELD="$2"; USE_THRIFT=1; shift 2 ;;
    --deep-thrift) DEEP_THRIFT=1; USE_THRIFT=1; shift ;;
    --deep-rows) DEEP_ROWS="$2"; shift 2 ;;
    --deep-left-seed) DEEP_LEFT_SEED="$2"; shift 2 ;;
    --deep-right-seed) DEEP_RIGHT_SEED="$2"; shift 2 ;;
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

if [[ "$DEEP_THRIFT" == "1" ]]; then
  DEEP_DIR="$BUILD_DIR/examples/deep-thrift"
  mkdir -p "$DEEP_DIR"
  if [[ -z "$PAYLOAD_LEFT" ]]; then
    PAYLOAD_LEFT="$DEEP_DIR/deep_left.bin"
  fi
  if [[ -z "$PAYLOAD_RIGHT" ]]; then
    PAYLOAD_RIGHT="$DEEP_DIR/deep_right.bin"
  fi
  echo "[+] Generating DeepEvent payloads ($DEEP_ROWS rows per file)"
  java -cp "$FAT_JAR" org.surfing.drsquirrel.bench.DeepEventGenerator "$PAYLOAD_LEFT" "$DEEP_ROWS" "$DEEP_LEFT_SEED"
  java -cp "$FAT_JAR" org.surfing.drsquirrel.bench.DeepEventGenerator "$PAYLOAD_RIGHT" "$DEEP_ROWS" "$DEEP_RIGHT_SEED"
  if [[ -z "$THRIFT_PATH" ]]; then
    THRIFT_PATH="$ROOT/src/bench/deep_event.thrift"
  fi
  if [[ -z "$THRIFT_STRUCT" ]]; then
    THRIFT_STRUCT="DeepEvent"
  fi
  if [[ -z "$KEY_FIELD" ]]; then
    KEY_FIELD="event_id"
  fi
  ROWS="$DEEP_ROWS"
fi

mkdir -p "$OUT_DIR"

if [[ "$USE_THRIFT" == "1" ]]; then
  if [[ -z "$THRIFT_PATH" || -z "$THRIFT_STRUCT" || -z "$PAYLOAD_LEFT" ]]; then
    echo "[-] Thrift mode requires --thrift-path, --thrift-struct and --payload-left (or --deep-thrift)."
    exit 2
  fi
  export SHUFFLE_THRIFT_PATH="$THRIFT_PATH"
  export SHUFFLE_THRIFT_STRUCT="$THRIFT_STRUCT"
  export SHUFFLE_THRIFT_KEY_FIELD="$KEY_FIELD"
  export SHUFFLE_THRIFT_PAYLOAD_LEFT="$PAYLOAD_LEFT"
  if [[ -n "$PAYLOAD_RIGHT" ]]; then
    export SHUFFLE_THRIFT_PAYLOAD_RIGHT="$PAYLOAD_RIGHT"
  else
    unset SHUFFLE_THRIFT_PAYLOAD_RIGHT || true
  fi
else
  unset SHUFFLE_THRIFT_PATH SHUFFLE_THRIFT_STRUCT SHUFFLE_THRIFT_KEY_FIELD SHUFFLE_THRIFT_PAYLOAD_LEFT SHUFFLE_THRIFT_PAYLOAD_RIGHT || true
fi

IFACE_OPT="--mca btl_tcp_if_exclude lo,docker0"
if [[ -n "${IFACE}" ]]; then
  IFACE_OPT="--mca btl_tcp_if_include ${IFACE}"
fi
# Allow overriding transport to avoid UCX issues (e.g., force TCP)
MPIRUN_EXTRA_OPTS=${MPIRUN_EXTRA_OPTS:-"--mca pml ob1 --mca btl tcp,self --mca mtl ^ofi --mca coll ^hcoll"}
# Allow passing extra JVM flags for debugging (e.g., -Xint)
JAVA_EXTRA_OPTS=${JAVA_EXTRA_OPTS:-}

if [[ "$USE_THRIFT" == "1" ]]; then
  echo "[+] Running fake-cogroup-app np=$NP mode=$MODE thrift_struct=$THRIFT_STRUCT key=$KEY_FIELD left=$PAYLOAD_LEFT right=${PAYLOAD_RIGHT:-reuse} iters=$ITERS out=$OUT_DIR sort-by='${SORT_BY}' iface='${IFACE}'"
else
  echo "[+] Running fake-cogroup-app np=$NP mode=$MODE rows=$ROWS iters=$ITERS out=$OUT_DIR sort-by='${SORT_BY}' iface='${IFACE}'"
fi
export LD_LIBRARY_PATH="$BUILD_DIR:${LD_LIBRARY_PATH:-}"

mpiexec -np "$NP" --use-hwthread-cpus --oversubscribe --map-by core --bind-to core \
  ${IFACE_OPT} ${MPIRUN_EXTRA_OPTS} \
  java ${JAVA_EXTRA_OPTS} -Djava.library.path="$BUILD_DIR" \
       -cp "$FAT_JAR:$EX_JAR" \
       com.example.fakecogroup.Main \
       --mode "$MODE" --rows "$ROWS" --iters "$ITERS" --out "$OUT_DIR" ${SORT_BY:+--sort-by "$SORT_BY"}

echo "[+] Done. Samples written to: $OUT_DIR"
