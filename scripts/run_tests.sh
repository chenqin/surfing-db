#!/usr/bin/env bash
set -euo pipefail

# Run C++ unit tests, optional MPI shuffle tests, and Java tests.
# Assumes the project is already built (see scripts/build_install.sh).

RUN_MPI=1
for arg in "$@"; do
  case "$arg" in
    --no-mpi) RUN_MPI=0 ;;
  esac
done

ARROW_PREFIX_DEFAULT="${HOME}/arrow-12-install"
BUILD_DIR="build"

if [ ! -d "${BUILD_DIR}" ]; then
  echo "Build directory not found. Run ./scripts/build_install.sh first." >&2
  exit 1
fi

export CTEST_OUTPUT_ON_FAILURE=1
pushd "${BUILD_DIR}" >/dev/null

echo "[+] Running C++ unit tests"
ctest -j"$(nproc)" -R SurfingDbTests

if [ "$RUN_MPI" = "1" ]; then
  echo "[+] Running MPI shuffle tests (np=2,4)"
  ctest -j2 -R "MpiShuffleTest|MpiTwoSideShuffleTest_np2|MpiTwoSideShuffleTest_np4|MpiShuffleRandom_one_np4|MpiShuffleRandom_two_np4"
else
  echo "[i] Skipping MPI tests"
fi

popd >/dev/null

echo "[+] Running Java tests"
mvn -q -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 test

echo "[✓] All tests passed"
