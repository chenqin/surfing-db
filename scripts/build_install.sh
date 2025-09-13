#!/usr/bin/env bash
set -euo pipefail

# This script installs system dependencies, builds Arrow C++ from source (v12),
# configures the project with CMake + Ninja, and builds all targets.
#
# Usage:
#   ./scripts/build_install.sh [--no-sudo] [--arrow-prefix DIR]
#
# Options:
#   --no-sudo        Do not use sudo for apt installs (assumes root)
#   --arrow-prefix   Install prefix for Arrow (default: $HOME/arrow-12-install)

USE_SUDO=1
ARROW_PREFIX="${HOME}/arrow-12-install"
for arg in "$@"; do
  case "$arg" in
    --no-sudo) USE_SUDO=0 ;;
    --arrow-prefix)
      shift
      ARROW_PREFIX="$1" ;;
  esac
done

APT="apt-get"
if ! command -v ${APT} >/dev/null 2>&1; then
  echo "apt-get not found; please install dependencies manually." >&2
  exit 1
fi

SUDO="sudo"
if [ "${USE_SUDO}" = "0" ]; then
  SUDO=""
fi

echo "[+] Installing system packages"
${SUDO} ${APT} update -y
${SUDO} ${APT} install -y \
  build-essential cmake ninja-build pkg-config git curl ca-certificates \
  openjdk-8-jdk maven \
  libomp-dev libopenmpi-dev \
  libssl-dev libboost-dev libgoogle-glog-dev \
  pybind11-dev libcurl4-openssl-dev zlib1g-dev libzstd-dev liblz4-dev libsnappy-dev libbrotli-dev \
  libthrift-dev librdkafka-dev flex bison

export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export PATH="$JAVA_HOME/bin:$PATH"

# Build Arrow C++ 12.0.0 static
ARROW_SRC="${HOME}/apache-arrow-12.0.0"
if [ ! -d "${ARROW_SRC}" ]; then
  echo "[+] Fetching Arrow 12.0.0 sources"
  ( cd "${HOME}" && \
    curl -fsSLO https://downloads.apache.org/arrow/arrow-12.0.0/apache-arrow-12.0.0.tar.gz || \
    curl -fsSLO https://archive.apache.org/dist/arrow/arrow-12.0.0/apache-arrow-12.0.0.tar.gz )
  tar -C "${HOME}" -xzf "${HOME}/apache-arrow-12.0.0.tar.gz"
fi

echo "[+] Building Arrow C++ (static) to ${ARROW_PREFIX}"
mkdir -p "${ARROW_SRC}/cpp/build"
cmake -S "${ARROW_SRC}/cpp" -B "${ARROW_SRC}/cpp/build" -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DARROW_PARQUET=ON -DARROW_DATASET=ON -DARROW_JSON=ON -DARROW_FILESYSTEM=ON \
  -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF \
  -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_BZ2=ON \
  -DCMAKE_INSTALL_PREFIX="${ARROW_PREFIX}"
ninja -C "${ARROW_SRC}/cpp/build" -j"$(nproc)"
ninja -C "${ARROW_SRC}/cpp/build" install

echo "[+] Configuring and building project"
mkdir -p build
cmake -S . -B build -GNinja -DCMAKE_PREFIX_PATH="${ARROW_PREFIX}" ${CMAKE_EXTRA_FLAGS:-}
ninja -C build -j"$(nproc)"

echo "[+] Building Java module"
mvn -q -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests=false clean package

echo "[✓] Build complete"
