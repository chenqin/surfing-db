#!/usr/bin/env bash
#
# Run Spark RAPIDS Moving Average GPU Demo
#
# This script demonstrates GPU-accelerated moving average computation
# on 100-element float arrays using NVIDIA RAPIDS and Apache Spark.
#
# Requirements:
# - NVIDIA GPU with CUDA support
# - Spark 3.x with RAPIDS Accelerator plugin
# - CUDA Toolkit 11.x or 12.x
#
# Usage:
#   ./scripts/run_rapids_moving_avg.sh [OPTIONS]
#
# Options:
#   --num-records NUM    Number of records to generate (default: 1000000)
#   --window-size SIZE   Moving average window size (default: 5)
#   --array-size SIZE    Size of float arrays (default: 100)
#   --output PATH        Output parquet path (default: artifacts/moving_avg_output.parquet)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default configuration
NUM_RECORDS="${NUM_RECORDS:-1000000}"
WINDOW_SIZE="${WINDOW_SIZE:-5}"
ARRAY_SIZE="${ARRAY_SIZE:-100}"
OUTPUT_PATH="${OUTPUT_PATH:-artifacts/moving_avg_output.parquet}"

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --num-records)
      NUM_RECORDS="$2"
      shift 2
      ;;
    --window-size)
      WINDOW_SIZE="$2"
      shift 2
      ;;
    --array-size)
      ARRAY_SIZE="$2"
      shift 2
      ;;
    --output)
      OUTPUT_PATH="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check for NVIDIA GPU
if ! command -v nvidia-smi &> /dev/null; then
    echo "WARNING: nvidia-smi not found. NVIDIA GPU may not be available."
    echo "This demo requires CUDA-capable GPU for optimal performance."
fi

# Detect Spark installation
if [[ -n "${SPARK_HOME:-}" ]]; then
    SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit &> /dev/null; then
    SPARK_SUBMIT="spark-submit"
else
    echo "ERROR: spark-submit not found. Please set SPARK_HOME or add Spark to PATH."
    exit 1
fi

# Build if necessary
JAR_PATH="$PROJECT_ROOT/drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar"
if [[ ! -f "$JAR_PATH" ]]; then
    echo "Building Java project..."
    cd "$PROJECT_ROOT"
    mvn -q -f drsquirrel-java-project/pom.xml package -DskipTests
fi

# Check for RAPIDS plugin (optional - will use CPU if not available)
RAPIDS_JAR="${RAPIDS_JAR:-}"
RAPIDS_OPTS=""

if [[ -n "$RAPIDS_JAR" && -f "$RAPIDS_JAR" ]]; then
    echo "Using RAPIDS Accelerator: $RAPIDS_JAR"
    RAPIDS_OPTS="--jars $RAPIDS_JAR"
else
    echo "RAPIDS Accelerator not configured. Will run on CPU."
    echo "To enable GPU acceleration, set RAPIDS_JAR to the RAPIDS plugin JAR path."
    echo "Download from: https://nvidia.github.io/spark-rapids/"
fi

echo "================================================================"
echo "Spark RAPIDS Moving Average Demo"
echo "================================================================"
echo "Records:     $NUM_RECORDS"
echo "Array size:  $ARRAY_SIZE floats"
echo "Window size: $WINDOW_SIZE"
echo "Output:      $OUTPUT_PATH"
echo "JAR:         $JAR_PATH"
echo "================================================================"
echo ""

# Run Spark job with RAPIDS configuration
exec "$SPARK_SUBMIT" \
  --master "local[*]" \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.explain=ALL \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.resource.gpu.amount=0.25 \
  --conf spark.rapids.memory.gpu.pool=NONE \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  $RAPIDS_OPTS \
  --class com.pinterest.drsquirrel.spark.RapidsMovingAverage \
  "$JAR_PATH" \
  --num-records "$NUM_RECORDS" \
  --window-size "$WINDOW_SIZE" \
  --array-size "$ARRAY_SIZE" \
  --output "$OUTPUT_PATH"
