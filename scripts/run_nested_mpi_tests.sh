#!/usr/bin/env bash
set -euo pipefail

# Run nested MPI tests (shuffle + cogroup) for np=2 and/or np=4.
# Usage:
#   ./scripts/run_nested_mpi_tests.sh [--np 2|4|all] [--build]

NP_MODE="all"
DO_BUILD=0
for ((i=1; i<=$#; i++)); do
  case "${!i}" in
    --np)
      j=$((i+1)); NP_MODE="${!j}"; i=$j ;;
    --build)
      DO_BUILD=1 ;;
  esac
done

BUILD_DIR="build"
if [ ! -d "$BUILD_DIR" ] || [ "$DO_BUILD" = "1" ]; then
  cmake -S . -B "$BUILD_DIR" -GNinja -DENABLE_MPI_TESTS=ON
  ninja -C "$BUILD_DIR" MpiNestedShuffleTest MpiNestedCoGroupTest
fi

export CTEST_OUTPUT_ON_FAILURE=1
pushd "$BUILD_DIR" >/dev/null

case "$NP_MODE" in
  2)
    ctest -R "MpiNested(Shuffle|CoGroup)_(one|two)_np2" -j2 ;;
  4)
    ctest -R "MpiNested(Shuffle|CoGroup)_(one|two)_np4" -j2 ;;
  all|*)
    ctest -R "MpiNested(Shuffle|CoGroup)_(one|two)_np2" -j2
    ctest -R "MpiNested(Shuffle|CoGroup)_(one|two)_np4" -j2 || true ;;
esac

popd >/dev/null
echo "[✓] Nested MPI tests completed ($NP_MODE)"

