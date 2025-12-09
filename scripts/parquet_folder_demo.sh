#!/bin/bash
# Demonstration script for Parquet folder read/write functionality

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$ROOT_DIR/build"
TEST_OUTPUT="/tmp/surfingdb_parquet_demo"

echo "========================================="
echo "Surfing DB Parquet Folder Demo"
echo "========================================="
echo ""

# Clean up previous test data
if [ -d "$TEST_OUTPUT" ]; then
    echo "Cleaning up previous test data..."
    rm -rf "$TEST_OUTPUT"
fi

# C++ Demo
echo ""
echo "=== C++ Demo ==="
echo ""

if [ -f "$BUILD_DIR/parquet_folder_tool" ]; then
    echo "Writing Parquet files with C++ tool..."
    "$BUILD_DIR/parquet_folder_tool" write "$TEST_OUTPUT/cpp" 3 100

    echo ""
    echo "Reading Parquet files with C++ tool..."
    "$BUILD_DIR/parquet_folder_tool" read "$TEST_OUTPUT/cpp"
else
    echo "C++ tool not built. Run: cmake --build build --target parquet_folder_tool"
fi

# Python Demo
echo ""
echo "=== Python Demo ==="
echo ""

if [ -f "$ROOT_DIR/examples/python/parquet_folder_example.py" ]; then
    if command -v python3 &> /dev/null; then
        echo "Running Python roundtrip demo..."
        python3 "$ROOT_DIR/examples/python/parquet_folder_example.py" \
            --mode roundtrip \
            --output "$TEST_OUTPUT/python" \
            --num-files 3 \
            --rows-per-file 100
    else
        echo "Python3 not found. Skipping Python demo."
    fi
else
    echo "Python example not found."
fi

# Java Demo
echo ""
echo "=== Java Demo ==="
echo ""

JAVA_JAR="$ROOT_DIR/drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [ -f "$JAVA_JAR" ]; then
    echo "Writing sample Parquet files with Java..."
    java -cp "$JAVA_JAR" \
        org.surfing.drsquirrel.arrow.ParquetFolderWriter \
        "$TEST_OUTPUT/java"

    echo ""
    echo "Reading Parquet files with Java..."
    java -cp "$JAVA_JAR" \
        org.surfing.drsquirrel.arrow.ParquetFolderReader \
        "$TEST_OUTPUT/java"
else
    echo "Java JAR not built. Run: mvn -q -f drsquirrel-java-project/pom.xml package"
fi

# Summary
echo ""
echo "========================================="
echo "Demo Complete!"
echo "========================================="
echo ""
echo "Test data written to: $TEST_OUTPUT"
echo ""
echo "Directory structure:"
ls -lhR "$TEST_OUTPUT" 2>/dev/null || echo "No test data generated"
echo ""
echo "To clean up: rm -rf $TEST_OUTPUT"
