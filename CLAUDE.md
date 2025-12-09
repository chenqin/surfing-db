# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Surfing DB is a performance-focused data processing system for Apache Arrow with Thrift ingestion, MPI-based distributed shuffle/cogroup primitives, and multi-language (C++/Java/Python) decoders. Key features include:

- **Thrift → Arrow conversion**: Native JNI decoder (`surfingthriftjni`) for nested Thrift payloads (e.g., `DeepEvent`) with SIMD optimizations
- **MPI shuffle & cogroup**: One-sided RMA and two-sided send/recv data movement across ranks with Arrow RecordBatches
- **Automatic memory management**: Configurable disk spilling to handle datasets larger than RAM (see MEMORY_MANAGEMENT.md)
- **Multi-language support**: C++ core, Java/JNI bridge, Python bindings via pybind11

## Build Commands

### C++ Core
```bash
# Full build with Arrow deps (APT or source)
./scripts/build_install.sh

# Manual CMake build
cmake -S . -B build -GNinja
cmake --build build
```

**Key C++ targets:**
- `surfingdb_core` - Arrow shuffle/cogroup kernels (aggregates `surfmeta` + `surftable`)
- `thrift2arrow` - CLI tool for Thrift → Arrow schema conversion
- `parquet_folder_tool` - Parquet folder read/write utility
- `GenMabsPayloads*` - Dataset generators for benchmarks
- `surfingprocessorsjni` - JNI library for shuffle/cogroup
- `surfingprocessorspy` - Python module (pybind11)

### Java Modules

**Modular structure (see MODULAR_BUILD.md):**
- `surfingthriftjni` - Thrift→Arrow JNI + Parquet I/O (unified module)
- `drsquirrel-java-project` - MPI/Flink workflows (depends on surfingthriftjni)

```bash
# Build all Java modules
mvn clean install

# Individual modules
mvn -f surfingthriftjni/pom.xml package
mvn -f drsquirrel-java-project/pom.xml package

# Specify Arrow version
mvn -f surfingthriftjni/pom.xml -Darrow.version=12.0.0 package
```

### Python
```bash
python -m venv .venv && source .venv/bin/activate
pip install pyarrow
cmake --build build --target surfingprocessorspy
PYTHONPATH=build python examples/python/cogroup_benchmark.py --rows 100000
```

## Testing

### Run All Tests
```bash
./scripts/run_tests.sh              # C++ unit + MPI + Java
./scripts/run_tests.sh --no-mpi     # Skip MPI tests
```

### C++ Tests (CTest)
```bash
cd build
ctest -j$(nproc) -R SurfingDbTests                    # Unit tests
ctest -j2 -R "MpiShuffleTest|MpiTwoSideShuffleTest"  # MPI shuffle (np=2,4)
ctest -j2 -R "MpiNested.*"                           # Nested schema tests

# Run specific test suite
./SurfingDbTests --gtest_filter=ArrowSpillTest.*
./ThriftParserTests --gtest_filter=ThriftParser.Nested*
```

### Java Tests
```bash
export LD_LIBRARY_PATH=$(pwd)/build:${LD_LIBRARY_PATH:-}
mvn -f drsquirrel-java-project/pom.xml -Darrow.version=12.0.0 test
```

### Single Test Examples
```bash
# C++ unit test
./build/ThriftParserTests --gtest_filter=ThriftParser.ParseBasic

# MPI test (2 ranks)
mpiexec -np 2 ./build/MpiCoGroupTest

# Java JNI benchmark
java -Djava.library.path=build -cp surfingthriftjni/target/surfingthriftjni-1.0-SNAPSHOT.jar \
  org.surfing.drsquirrel.jni.JniDecodeBench
```

## Architecture

### C++ Core Structure (`src/`)

**`meta/` - Schema & Thrift parsing**
- `thrift_parser.h/cpp` - Parses `.thrift` IDL files → `mschema` / Arrow schema
- `schema.h/cpp` - Internal schema representation (`mschema`, `Field`, `Value`)
- `node.h/cpp` - AST nodes for Thrift parsing

**`table/` - Arrow processing**
- `processors.h/cpp` - MPI shuffle/cogroup primitives (one-sided/two-sided)
- `mtable.h/cpp`, `mrow.h/cpp` - Internal table/row representations
- `processors_jni.cpp` - JNI bridge exposing shuffle/cogroup to Java
- `processors_py.cpp` - Python bindings (pybind11)

**Build targets defined in:**
- `core/CMakeLists.txt` - Aggregates `surfmeta` + `surftable` into `surfingdb_core`
- `src/meta/meta.cmake` - Defines `surfmeta` library
- `src/table/table.cmake` - Defines `surftable` library

### Java Architecture

**`surfingthriftjni/` - Unified JNI Module**
- **Native:** `surfingthriftjni/native/src/thrift_decode_jni.cpp` - Thrift→Arrow JNI decoder with SIMD support
- **Java APIs:**
  - `org.surfing.drsquirrel.jni.NativeThriftDecoder` - JNI decoder entry point
  - `org.surfing.drsquirrel.jni.NativeParquetIO` - JNI Parquet folder I/O
  - `org.surfing.parquet.*` - Pure Java Parquet utilities (fallback)
  - `org.surfing.config.MemoryConfig` - Memory/spilling configuration

**`drsquirrel-java-project/` - Workflows & Benchmarks**
- `org.surfing.drsquirrel.jni.*` - MPI JNI runners (shuffle, cogroup load tests)
- `org.surfing.drsquirrel.flink.*` - Flink DataStream examples
- `org.surfing.drsquirrel.bench.*` - Benchmark harnesses
- `org.surfing.drsquirrel.tools.ThriftToParquet` - CLI converter

### Key Design Patterns

**Thrift Parsing Pipeline:**
1. `.thrift` file → `ThriftSchemaParser::parseToArrow()` → Arrow schema
2. Binary Thrift payload → `NativeThriftDecoder.convert()` → `VectorSchemaRoot`
3. Supports nested structs, lists, maps with configurable size defaults

**MPI Shuffle/Cogroup:**
- Input: `arrow::RecordBatchVector` + key field + partitioner function
- One-sided path: MPI RMA windows for zero-copy transfers
- Two-sided path: MPI send/recv with explicit serialization
- Output: Locally-owned partition as `RecordBatch`

**Memory Management (disk spilling):**
- Automatic: Batches > `SURFING_MAX_BATCH_MEMORY` (default 512MB) split & spilled to Arrow IPC files
- Multi-directory support: `SURFING_TEMP_DIRS` (comma-separated) for parallel I/O across disks
- Load balancing: `ROUND_ROBIN` (default), `SPACE_AWARE`, `RANDOM`
- See MEMORY_MANAGEMENT.md for configuration

## Common Workflows

### DeepEvent Benchmark (Thrift→Arrow JNI)
```bash
scripts/run_deep_event_bench.sh
# Outputs: build/deep_event_*.txt (JNI vs Java latencies, payload sizes)
# Tune with: DEEP_EVENT_SMALL_ROWS, DEEP_EVENT_LARGE_ROWS, DEEP_EVENT_FORCE_REGEN=1
```

### MPI Shuffle/Cogroup
```bash
# 4-rank cogroup with generated data
mpiexec -np 4 java -Djava.library.path=$PWD/build \
  -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
  org.surfing.drsquirrel.jni.JniCogroupLoadRunner --two

# Cogroup with Thrift payloads
mpiexec -np 4 java -Djava.library.path=$PWD/build \
  -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
  org.surfing.drsquirrel.jni.JniCogroupLoadRunner --two \
  --thrift-path src/bench/deep_event.thrift \
  --thrift-struct DeepEvent \
  --payload-left /tmp/deep_left.bin \
  --payload-right /tmp/deep_right.bin \
  --key-field event_id
```

### Parquet Folder Operations
```bash
# C++ CLI
./build/parquet_folder_tool --input /data/input --output /data/output

# Java API (see PARQUET_FOLDER_USAGE.md)
# Python example in examples/python/parquet_folder_example.py
```

### Flink DataStream (DeepEvent datagen)
```bash
mvn -q -f drsquirrel-java-project/pom.xml -Darrow.version=12.0.0 package
FLINK_HOME=/path/to/flink \
  "$FLINK_HOME/bin/flink" run \
  --class org.surfing.drsquirrel.flink.DeepEventDataStreamExample \
  drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar
# Tune: -Ddeep.event.count=100000 -Ddeep.event.rate=20000
```

## Configuration

### Environment Variables

**Build:**
- `SURF_SIMD=ON` - Enable SIMD optimizations in JNI decoder
- `SURF_THRIFT_DECODE_THREADS` - Parallel decode threads (default: auto)

**Runtime (benchmarks):**
- `SHUFFLE_TEST_SEED` - Random seed for MPI loads
- `COGROUP_THRIFT_FILE`, `COGROUP_THRIFT_STRUCT` - Enable Thrift ingestion in MPI runners
- `COGROUP_THRIFT_PAYLOAD[_LEFT/_RIGHT]` - Pre-generated Thrift frames (`[u32 len][payload]`)
- `DEEP_EVENT_SMALL_ROWS`, `DEEP_EVENT_LARGE_ROWS` - Dataset sizes for JNI benchmark
- `SHUFFLE_LOAD_OUT=/tmp/cogroup.csv` - CSV log aggregation

**Memory/Spilling (see MEMORY_MANAGEMENT.md):**
- `SURFING_MAX_BATCH_MEMORY` - Max batch memory (default: 512MB = 536870912 bytes)
- `SURFING_TEMP_DIR` - Single temp directory (backward compatible)
- `SURFING_TEMP_DIRS` - Multiple temp dirs (comma-separated) for parallel I/O
- `SURFING_LOAD_BALANCING` - `ROUND_ROBIN` (default), `SPACE_AWARE`, `RANDOM`
- `SURFING_ENABLE_SPILLING` - Enable/disable spilling (default: 1)

### CMake Options
```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_MPI_TESTS=ON \
  -DBUILD_CPP_KAFKA=OFF \
  -DCMAKE_PREFIX_PATH=/path/to/arrow-install
```

## Troubleshooting

### Build Issues

**Arrow not found:**
```bash
# Option 1: Use APT (recommended)
./scripts/build_install.sh --use-system-arrow

# Option 2: Build from source
./scripts/build_install.sh --build-arrow --arrow-prefix $HOME/arrow-12-install
```

**JNI library not found:**
```bash
export LD_LIBRARY_PATH=$(pwd)/build:${LD_LIBRARY_PATH:-}
# Or specify: -Djava.library.path=$(pwd)/build
```

**Maven build fails (missing dependency):**
```bash
# Build in dependency order
mvn clean install  # Builds: surfingthriftjni → drsquirrel-java
```

### Runtime Issues

**OOM when processing large datasets:**
```bash
# Lower spill threshold
export SURFING_MAX_BATCH_MEMORY=268435456  # 256MB

# Or use multiple disks for better I/O
export SURFING_TEMP_DIRS="/mnt/disk1/spill,/mnt/disk2/spill"
export SURFING_LOAD_BALANCING=ROUND_ROBIN
```

**Disk full during spill:**
```bash
# Use space-aware balancing
export SURFING_TEMP_DIRS="/mnt/disk1/spill,/mnt/disk2/spill"
export SURFING_LOAD_BALANCING=SPACE_AWARE

# Or cleanup old spill files
find /tmp/surfing_spill -name "surfing_spill_*" -mtime +1 -delete
```

**MPI tests hang:**
```bash
# Check MPI environment
mpiexec --version
# Ensure no firewall blocking, sufficient shared memory
```

## Key Files & Locations

**Build outputs:**
- Native libs: `build/libsurfingprocessorsjni.so`, `build/libsurfingthriftjni.so` (from Maven build)
- Java JARs:
  - `surfingthriftjni/target/surfingthriftjni-1.0-SNAPSHOT.jar`
  - `drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar`

**Test data:**
- Thrift schemas: `src/bench/*.thrift`
- Generated payloads: `/tmp/deep_*.bin` (created by benchmark scripts)

**Documentation:**
- Memory management: `MEMORY_MANAGEMENT.md`
- Modular build: `MODULAR_BUILD.md`
- Parquet usage: `PARQUET_FOLDER_USAGE.md`, `PARQUET_QUICKSTART.md`
- SIMD optimizations: `SIMD_OPTIMIZATIONS.md`

## Development Notes

- **Thrift schema changes:** Regenerate with `thrift --gen cpp src/bench/your_schema.thrift`
- **Adding MPI tests:** Update `CMakeLists.txt` under `if(ENABLE_MPI_TESTS)` block
- **JNI changes:** Native code in `surfingthriftjni/native/`, build via Maven: `mvn -f surfingthriftjni/pom.xml package`
- **Arrow version:** Coordinated via `-Darrow.version=12.0.0` in Maven and `find_package(Arrow REQUIRED)` in CMake
- **SIMD optimizations:** Native JNI decoder supports AVX2/AVX-512 (see SIMD_OPTIMIZATIONS.md)
