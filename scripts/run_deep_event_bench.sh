#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
BUILD_DIR="$ROOT_DIR/build"
JNI_BUILD_DIR="$ROOT_DIR/surfingthriftjni/target/nativebuild"
JAR="$ROOT_DIR/surfingthriftjni/target/surfingthriftjni-1.0-SNAPSHOT-jar-with-dependencies.jar"
THRIFT_SCHEMA="$ROOT_DIR/src/bench/deep_event.thrift"

SMALL_PAYLOAD="${DEEP_EVENT_SMALL_PAYLOAD:-/tmp/deep_event_small.bin}"
LARGE_PAYLOAD="${DEEP_EVENT_LARGE_PAYLOAD:-/tmp/deep_event_large.bin}"
SMALL_ROWS="${DEEP_EVENT_SMALL_ROWS:-250000}"
LARGE_ROWS="${DEEP_EVENT_LARGE_ROWS:-25000}"
SMALL_ITERS="${DEEP_EVENT_SMALL_ITERS:-4}"
LARGE_ITERS="${DEEP_EVENT_LARGE_ITERS:-4}"
FORCE_REGEN="${DEEP_EVENT_FORCE_REGEN:-0}"

SMALL_OUT="$BUILD_DIR/deep_event_small_array.txt"
LARGE_BB_OUT="$BUILD_DIR/deep_event_large_bb.txt"
LARGE_BBP_OUT="$BUILD_DIR/deep_event_large_bbp.txt"

mkdir -p "$BUILD_DIR"

# Build JNI + Java wrapper when artifacts are missing.
if [ ! -f "$JAR" ] || [ ! -d "$JNI_BUILD_DIR" ]; then
  (cd "$ROOT_DIR/surfingthriftjni" && mvn -q -Darrow.version=12.0.0 -DskipTests package)
fi

# Generate DeepEvent payloads (small & large) if absent or forced.
if [ ! -f "$SMALL_PAYLOAD" ] || [ "$FORCE_REGEN" = "1" ]; then
  echo "Generating DeepEvent small payload: $SMALL_PAYLOAD ($SMALL_ROWS rows)"
  java -cp "$JAR" org.surfing.deep.bench.DeepEventPayloadGenerator "$SMALL_PAYLOAD" "$SMALL_ROWS" small
fi

if [ ! -f "$LARGE_PAYLOAD" ] || [ "$FORCE_REGEN" = "1" ]; then
  echo "Generating DeepEvent large payload: $LARGE_PAYLOAD ($LARGE_ROWS rows)"
  java -cp "$JAR" org.surfing.deep.bench.DeepEventPayloadGenerator "$LARGE_PAYLOAD" "$LARGE_ROWS" large
fi

export LD_LIBRARY_PATH="$JNI_BUILD_DIR:${LD_LIBRARY_PATH:-}"

echo "== DeepEvent small payloads (array mode) =="
java -Djava.library.path="$JNI_BUILD_DIR" \
  -cp "$JAR" \
  org.surfing.drsquirrel.jni.JniDecodeBench \
  "$SMALL_PAYLOAD" "$THRIFT_SCHEMA" DeepEvent "$SMALL_ITERS" array | tee "$SMALL_OUT"

echo "\n== DeepEvent large payloads (ByteBuffer mmap) =="
java -Xmx3g -Djava.library.path="$JNI_BUILD_DIR" \
  -cp "$JAR" \
  org.surfing.drsquirrel.jni.JniDecodeBench \
  "$LARGE_PAYLOAD" "$THRIFT_SCHEMA" DeepEvent "$LARGE_ITERS" bb | tee "$LARGE_BB_OUT"

echo "\n== DeepEvent large payloads (ByteBuffer pooled) =="
java -Xmx3g -Djava.library.path="$JNI_BUILD_DIR" \
  -cp "$JAR" \
  org.surfing.drsquirrel.jni.JniDecodeBench \
  "$LARGE_PAYLOAD" "$THRIFT_SCHEMA" DeepEvent "$LARGE_ITERS" bbp | tee "$LARGE_BBP_OUT"

echo "\n== Summary =="
small_jni=$(grep -m1 "^JNI Best:" "$SMALL_OUT" 2>/dev/null | sed 's/^JNI Best: //' || true)
small_java=$(grep -m1 "^Java Best:" "$SMALL_OUT" 2>/dev/null | sed 's/^Java Best: //' || true)
bb_jni=$(grep -m1 "^JNI Best:" "$LARGE_BB_OUT" 2>/dev/null | sed 's/^JNI Best: //' || true)
bbp_jni=$(grep -m1 "^JNI Best:" "$LARGE_BBP_OUT" 2>/dev/null | sed 's/^JNI Best: //' || true)

printf "DeepEvent small array — JNI: %s\n" "${small_jni:-n/a}"
if [ -n "${small_java:-}" ]; then
  printf "DeepEvent small array — Java: %s\n" "$small_java"
else
  echo "DeepEvent small array — Java: skipped"
fi
printf "DeepEvent large mmap — JNI: %s\n" "${bb_jni:-n/a}"
printf "DeepEvent large pooled — JNI: %s\n" "${bbp_jni:-n/a}"

small_bytes=$(stat -c %s "$SMALL_PAYLOAD" 2>/dev/null || wc -c < "$SMALL_PAYLOAD")
large_bytes=$(stat -c %s "$LARGE_PAYLOAD" 2>/dev/null || wc -c < "$LARGE_PAYLOAD")
small_mb=$(awk -v b="$small_bytes" 'BEGIN { printf "%.2f", b/1048576 }')
large_mb=$(awk -v b="$large_bytes" 'BEGIN { printf "%.2f", b/1048576 }')
printf "Payload stats — small: %s bytes (%.2f MB, %s rows) | large: %s bytes (%.2f MB, %s rows)\n" \
  "$small_bytes" "$small_mb" "$SMALL_ROWS" "$large_bytes" "$large_mb" "$LARGE_ROWS"
