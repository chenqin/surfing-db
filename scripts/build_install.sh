#!/usr/bin/env bash
set -euo pipefail

# This script installs system dependencies, installs Apache Arrow C++ via APT
# (preferred), or optionally builds Arrow C++ from source, then configures and
# builds the project with CMake + Ninja.
#
# Usage:
#   ./scripts/build_install.sh [--no-sudo] [--arrow-prefix DIR]
#
# Options:
#   --no-sudo            Do not use sudo for apt installs (assumes root)
#   --use-system-arrow   Install Arrow via APT (libarrow-dev, libparquet-dev, ...)
#   --build-arrow        Force build Arrow from source (if APT not desired)
#   --arrow-prefix       Install prefix for Arrow (source-build only; default: $HOME/arrow-12-install)
#   --arrow-version      Arrow version for source-build (default: 12.0.0)

USE_SUDO=1
ARROW_PREFIX="${HOME}/arrow-12-install"
ARROW_VERSION="12.0.0"
USE_SYSTEM_ARROW=1
BUILD_ARROW=0
for arg in "$@"; do
  case "$arg" in
    --no-sudo) USE_SUDO=0 ;;
    --use-system-arrow) USE_SYSTEM_ARROW=1 ;;
    --build-arrow) BUILD_ARROW=1 ; USE_SYSTEM_ARROW=0 ;;
    --arrow-prefix)
      shift
      ARROW_PREFIX="$1" ;;
    --arrow-version)
      shift
      ARROW_VERSION="$1" ;;
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
  libomp-dev libopenmpi-dev openmpi-bin \
  libssl-dev libboost-dev libgoogle-glog-dev \
  pybind11-dev libcurl4-openssl-dev zlib1g-dev libzstd-dev liblz4-dev libsnappy-dev libbrotli-dev \
  libthrift-dev librdkafka-dev flex bison

export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export PATH="$JAVA_HOME/bin:$PATH"

if [ "$USE_SYSTEM_ARROW" = "1" ] && [ "$BUILD_ARROW" = "0" ]; then
  echo "[+] Installing Arrow C++ from APT"
  # Add Apache Arrow APT source (fallback to 'noble' if codename not supported)
  ${SUDO} ${APT} install -y lsb-release wget gnupg || true
  CODENAME=$(lsb_release --codename --short 2>/dev/null || echo noble)
  DEB_URL="https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-${CODENAME}.deb"
  ALT_URL="https://packages.apache.org/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-noble.deb"
  TMP_DEB=/tmp/apache-arrow-apt-source.deb
  if ! wget -q -O "$TMP_DEB" "$DEB_URL"; then
    echo "[i] Falling back to Arrow APT source for noble";
    wget -q -O "$TMP_DEB" "$ALT_URL" || true
  fi
  if [ -f "$TMP_DEB" ]; then
    ${SUDO} ${APT} install -y "$TMP_DEB" || true
    rm -f "$TMP_DEB"
  fi
  ${SUDO} ${APT} update -y || true
  ${SUDO} ${APT} install -y libarrow-dev libparquet-dev libarrow-dataset-dev || {
    echo "[!] Failed to install Arrow from APT. You can retry with --build-arrow to build from source." >&2
    exit 4
  }
fi

if [ "$BUILD_ARROW" = "1" ]; then
  echo "[+] Building Arrow C++ ${ARROW_VERSION} (static) to ${ARROW_PREFIX}"
  ARROW_TOP_PATTERN="*apache-arrow-${ARROW_VERSION}"
  ARROW_SRC=""
  if compgen -G "${HOME}/${ARROW_TOP_PATTERN}" > /dev/null; then
    ARROW_SRC=$(echo ${HOME}/${ARROW_TOP_PATTERN} | awk '{print $1}')
  else
    echo "[+] Fetching Arrow ${ARROW_VERSION} sources"
    TARBALL="apache-arrow-${ARROW_VERSION}.tar.gz"
    cd "${HOME}"
    URLS=(
      "https://archive.apache.org/dist/arrow/arrow-${ARROW_VERSION}/${TARBALL}"
      "https://downloads.apache.org/arrow/arrow-${ARROW_VERSION}/${TARBALL}"
      "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${ARROW_VERSION}.tar.gz"
    )
    SUCCESS=0
    for u in "${URLS[@]}"; do
      echo "[i] Trying ${u}"
      if curl -fL --retry 3 --retry-delay 2 -o "${TARBALL}" "${u}"; then SUCCESS=1; break; fi
    done
    if [ "${SUCCESS}" != "1" ]; then
      echo "[!] Failed to download Arrow ${ARROW_VERSION}." >&2; exit 2
    fi
    tar -xzf "${TARBALL}"
    if compgen -G "${HOME}/${ARROW_TOP_PATTERN}" > /dev/null; then
      ARROW_SRC=$(echo ${HOME}/${ARROW_TOP_PATTERN} | awk '{print $1}')
    else
      echo "[!] Extracted Arrow tarball but directory not found" >&2; exit 3
    fi
  fi
  mkdir -p "${ARROW_SRC}/cpp/build"
  cmake -S "${ARROW_SRC}/cpp" -B "${ARROW_SRC}/cpp/build" -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DARROW_PARQUET=ON -DARROW_DATASET=ON -DARROW_JSON=ON -DARROW_FILESYSTEM=ON \
    -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF \
    -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_BZ2=ON \
    -DCMAKE_INSTALL_PREFIX="${ARROW_PREFIX}"
  ninja -C "${ARROW_SRC}/cpp/build" -j"$(nproc)"
  ninja -C "${ARROW_SRC}/cpp/build" install
fi

echo "[+] Configuring and building project"
mkdir -p build
if [ "$BUILD_ARROW" = "1" ]; then
  cmake -S . -B build -GNinja -DCMAKE_PREFIX_PATH="${ARROW_PREFIX}" ${CMAKE_EXTRA_FLAGS:-}
else
  cmake -S . -B build -GNinja ${CMAKE_EXTRA_FLAGS:-}
fi
ninja -C build -j"$(nproc)"

echo "[+] Building Java module"
mvn -q -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests=false clean package

echo "[✓] Build complete"
