#!/usr/bin/env bash
set -euo pipefail

# This script installs system dependencies, installs Apache Arrow C++ via APT
# (preferred), or optionally builds Arrow C++ from source, then configures and
# builds the project with CMake + Ninja.
#
# Usage:
#   ./scripts/build_install.sh [--no-sudo] [--arrow-prefix DIR] [--with-cuda] [--with-rapids]
#
# Options:
#   --no-sudo            Do not use sudo for apt installs (assumes root)
#   --use-system-arrow   Install Arrow via APT (libarrow-dev, libparquet-dev, ...)
#   --build-arrow        Force build Arrow from source (if APT not desired)
#   --arrow-prefix       Install prefix for Arrow (source-build only; default: $HOME/arrow-12-install)
#   --arrow-version      Arrow version for source-build (default: 12.0.0)
#   --with-cuda          Install CUDA Toolkit and development libraries
#   --with-rapids        Download RAPIDS Accelerator for Spark (requires --with-cuda)

USE_SUDO=1
ARROW_PREFIX="${HOME}/arrow-12-install"
ARROW_VERSION="12.0.0"
USE_SYSTEM_ARROW=1
BUILD_ARROW=0
INSTALL_CUDA=0
INSTALL_RAPIDS=0
RAPIDS_VERSION="24.02.0"
SPARK_VERSION="3.5"
for arg in "$@"; do
  case "$arg" in
    --no-sudo) USE_SUDO=0 ;;
    --use-system-arrow) USE_SYSTEM_ARROW=1 ;;
    --build-arrow) BUILD_ARROW=1 ; USE_SYSTEM_ARROW=0 ;;
    --with-cuda) INSTALL_CUDA=1 ;;
    --with-rapids) INSTALL_RAPIDS=1 ;;
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

if [ "$INSTALL_CUDA" = "1" ]; then
  echo "[+] Installing CUDA Toolkit and development libraries"

  # Check if CUDA is already installed
  if command -v nvcc >/dev/null 2>&1; then
    echo "[i] CUDA already installed: $(nvcc --version | grep release)"
  else
    # Install CUDA Toolkit via NVIDIA's official repos
    echo "[+] Adding NVIDIA CUDA repository"
    UBUNTU_VERSION=$(lsb_release -rs | tr -d '.')
    CUDA_REPO_PKG="cuda-keyring_1.1-1_all.deb"

    wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu${UBUNTU_VERSION}/x86_64/${CUDA_REPO_PKG}
    ${SUDO} dpkg -i ${CUDA_REPO_PKG}
    rm -f ${CUDA_REPO_PKG}

    ${SUDO} ${APT} update -y
    ${SUDO} ${APT} install -y cuda-toolkit-12-2 cuda-drivers || {
      echo "[!] Failed to install CUDA. Please install manually from https://developer.nvidia.com/cuda-downloads" >&2
      exit 5
    }
  fi

  # Install additional CUDA development libraries
  echo "[+] Installing CUDA development libraries"
  ${SUDO} ${APT} install -y \
    libcublas-dev-12-2 \
    libcusparse-dev-12-2 \
    libcurand-dev-12-2 \
    libcufft-dev-12-2 || echo "[i] Some CUDA dev libraries not available via APT"

  # Set up CUDA environment
  export CUDA_HOME=/usr/local/cuda-12.2
  export PATH=$CUDA_HOME/bin:$PATH
  export LD_LIBRARY_PATH=$CUDA_HOME/lib64:${LD_LIBRARY_PATH:-}

  echo "[i] CUDA installed: $(nvcc --version | grep release || echo 'version check failed')"
  nvidia-smi --query-gpu=name,driver_version --format=csv,noheader || echo "[!] nvidia-smi not available"
fi

if [ "$INSTALL_RAPIDS" = "1" ]; then
  if [ "$INSTALL_CUDA" != "1" ] && ! command -v nvcc >/dev/null 2>&1; then
    echo "[!] RAPIDS requires CUDA. Please use --with-cuda flag or install CUDA manually." >&2
    exit 6
  fi

  echo "[+] Downloading RAPIDS Accelerator for Spark ${RAPIDS_VERSION}"
  RAPIDS_JAR="rapids-4-spark_2.12-${RAPIDS_VERSION}.jar"
  RAPIDS_DIR="${HOME}/.rapids"
  mkdir -p "${RAPIDS_DIR}"

  RAPIDS_URL="https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/${RAPIDS_VERSION}/${RAPIDS_JAR}"

  if [ ! -f "${RAPIDS_DIR}/${RAPIDS_JAR}" ]; then
    echo "[+] Downloading from ${RAPIDS_URL}"
    curl -fL -o "${RAPIDS_DIR}/${RAPIDS_JAR}" "${RAPIDS_URL}" || {
      echo "[!] Failed to download RAPIDS. Please download manually from https://nvidia.github.io/spark-rapids/" >&2
      exit 7
    }
  else
    echo "[i] RAPIDS JAR already exists: ${RAPIDS_DIR}/${RAPIDS_JAR}"
  fi

  # Set environment variable for easy access
  export RAPIDS_JAR="${RAPIDS_DIR}/${RAPIDS_JAR}"

  echo "[i] RAPIDS JAR downloaded to: ${RAPIDS_JAR}"
  echo "[i] Set RAPIDS_JAR environment variable in your shell profile:"
  echo "    export RAPIDS_JAR=${RAPIDS_JAR}"
fi

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
  ${SUDO} ${APT} install -y libarrow-dev libparquet-dev libarrow-compute-dev libarrow-dataset-dev || {
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
    -DARROW_COMPUTE=ON -DARROW_PARQUET=ON -DARROW_DATASET=ON -DARROW_JSON=ON -DARROW_FILESYSTEM=ON \
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
mvn -q -f drsquirrel-java-project/pom.xml -Darrow.version=12.0.0 -DskipTests=false clean package

if [ "$INSTALL_CUDA" = "1" ]; then
  echo "[+] Building CUDA moving average benchmark"
  if command -v nvcc >/dev/null 2>&1; then
    mkdir -p build/cuda
    nvcc -O3 -std=c++14 -o build/cuda/moving_average_benchmark \
      src/cuda/moving_average_kernel.cu \
      src/cuda/moving_average_benchmark.cpp \
      -I/usr/local/cuda/include \
      -L/usr/local/cuda/lib64 \
      -lcudart || echo "[!] CUDA benchmark build failed"

    if [ -f build/cuda/moving_average_benchmark ]; then
      echo "[✓] CUDA benchmark built: build/cuda/moving_average_benchmark"
      echo "[i] Run with: ./build/cuda/moving_average_benchmark [numArrays] [arraySize] [windowSize]"
      echo "[i] Example: ./build/cuda/moving_average_benchmark 1000000 100 1024"
    fi
  else
    echo "[!] nvcc not found. Skipping CUDA benchmark build."
  fi
fi

echo "[✓] Build complete"

if [ "$INSTALL_RAPIDS" = "1" ]; then
  echo ""
  echo "================================================================"
  echo "RAPIDS Setup Complete"
  echo "================================================================"
  echo "To use RAPIDS in your shell, add to ~/.bashrc or ~/.zshrc:"
  echo "  export RAPIDS_JAR=${RAPIDS_JAR}"
  echo ""
  echo "Run RAPIDS moving average benchmark:"
  echo "  ./scripts/run_rapids_moving_avg.sh"
  echo "================================================================"
fi
