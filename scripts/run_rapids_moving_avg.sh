#!/usr/bin/env bash
#
# CUDA Moving Average Benchmark Suite
#
# Comprehensive benchmark testing rolling window computations
# on multiple float arrays with varying configurations.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BENCHMARK_BIN="$PROJECT_ROOT/build/cuda/moving_average_benchmark"

# Check if benchmark is built
if [[ ! -f "$BENCHMARK_BIN" ]]; then
    echo "ERROR: Benchmark not found at $BENCHMARK_BIN"
    echo "Please build first with: nvcc -O3 -std=c++14 -o build/cuda/moving_average_benchmark src/cuda/moving_average_kernel.cu src/cuda/moving_average_benchmark.cpp -lcudart"
    exit 1
fi

# Check for GPU
if ! command -v nvidia-smi &> /dev/null; then
    echo "WARNING: nvidia-smi not found. GPU may not be available."
fi

echo "================================================================"
echo "CUDA Moving Average - Rolling Window Benchmark Suite"
echo "================================================================"
nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader
echo "================================================================"
echo ""

# Benchmark configurations: numArrays arraySize windowSize
CONFIGS=(
    # Small dataset - various window sizes
    "100000 100 5"
    "100000 100 10"
    "100000 100 20"
    "100000 100 50"

    # Medium dataset - various array sizes
    "500000 50 10"
    "500000 100 10"
    "500000 200 10"
    "500000 500 10"

    # Large dataset - stress test
    "1000000 100 10"
    "2000000 100 10"
    "5000000 100 10"

    # Extra large arrays
    "100000 1000 10"
    "50000 2000 20"
)

echo "Running ${#CONFIGS[@]} benchmark configurations..."
echo ""

for config in "${CONFIGS[@]}"; do
    read -r numArrays arraySize windowSize <<< "$config"

    echo "----------------------------------------------------------------"
    echo "Configuration: $numArrays arrays × $arraySize floats, window=$windowSize"
    echo "----------------------------------------------------------------"

    $BENCHMARK_BIN $numArrays $arraySize $windowSize

    echo ""
    sleep 1
done

echo "================================================================"
echo "Benchmark suite completed!"
echo "================================================================"
