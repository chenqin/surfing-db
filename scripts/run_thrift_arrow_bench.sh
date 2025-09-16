#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
BUILD_DIR="$ROOT_DIR/build"
JNI_BUILD_DIR="$ROOT_DIR/surfingthriftjni/target/nativebuild"
JAR="$ROOT_DIR/surfingthriftjni/target/surfingthriftjni-1.0-SNAPSHOT-jar-with-dependencies.jar"
NATIVES_JAR="$ROOT_DIR/surfingthriftjni/target"/surfingthriftjni-1.0-SNAPSHOT-natives-*
PAY_SMALL="/tmp/mabs_payloads.bin"
PAY_LARGE20K="/tmp/mabs_payloads_large_20k.bin"

# Ensure dataset generators (C++ tools) are built in root build dir
if [ ! -f "$BUILD_DIR/GenMabsPayloads" ] || [ ! -f "$BUILD_DIR/GenMabsPayloadsLarge" ]; then
  cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release
  cmake --build "$BUILD_DIR" --target GenMabsPayloads GenMabsPayloadsLarge -j
fi

# Build JNI + Java wrapper via Maven (also builds libsurfingthriftjni.* into target/nativebuild)
if [ ! -f "$JAR" ] || [ ! -d "$JNI_BUILD_DIR" ]; then
  (cd "$ROOT_DIR/surfingthriftjni" && mvn -q -DskipTests package)
fi

# Generate datasets
if [ ! -f "$PAY_SMALL" ]; then
  "$BUILD_DIR/GenMabsPayloads" "$PAY_SMALL" 200000
fi
if [ ! -f "$PAY_LARGE20K" ]; then
  "$BUILD_DIR/GenMabsPayloadsLarge" "$PAY_LARGE20K" 20000 256 16 512
fi

echo "== Small payloads (array mode) =="
java -Djava.library.path="$JNI_BUILD_DIR" -cp "$JAR" com.pinterest.drsquirrel.jni.JniDecodeBench "$PAY_SMALL" "$ROOT_DIR/src/mabs.thrift" MabsMetrics 7 | tee "$BUILD_DIR/bench_small_array.txt"

echo "\n== Large payloads (ByteBuffer mmap) =="
java -Xmx3g -Djava.library.path="$JNI_BUILD_DIR" -cp "$JAR" com.pinterest.drsquirrel.jni.JniDecodeBench "$PAY_LARGE20K" "$ROOT_DIR/src/mabs.thrift" MabsMetrics 5 bb | tee "$BUILD_DIR/bench_large_bb.txt"

echo "\n== Large payloads (ByteBuffer pooled) =="
java -Xmx3g -Djava.library.path="$JNI_BUILD_DIR" -cp "$JAR" com.pinterest.drsquirrel.jni.JniDecodeBench "$PAY_LARGE20K" "$ROOT_DIR/src/mabs.thrift" MabsMetrics 5 bbp | tee "$BUILD_DIR/bench_large_bbp.txt"

echo "\n== Summary =="
small_jni=$(grep -m1 "^JNI Best:" "$BUILD_DIR/bench_small_array.txt" | sed 's/^JNI Best: //')
small_java=$(grep -m1 "^Java Best:" "$BUILD_DIR/bench_small_array.txt" | sed 's/^Java Best: //')
bb_jni=$(grep -m1 "^JNI Best:" "$BUILD_DIR/bench_large_bb.txt" | sed 's/^JNI Best: //')
bbp_jni=$(grep -m1 "^JNI Best:" "$BUILD_DIR/bench_large_bbp.txt" | sed 's/^JNI Best: //')
printf "Small array — JNI: %s\n" "$small_jni"
printf "Small array — Java: %s\n" "$small_java"
printf "Large mmap — JNI: %s\n" "$bb_jni"
printf "Large pooled — JNI: %s\n" "$bbp_jni"
