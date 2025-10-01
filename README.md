# Surfing DB

Surfing DB is a performance lab for moving complex Thrift payloads into Apache Arrow, shuffling them across MPI ranks, and benchmarking multi-language decoders.

## Highlights
- **Nested Thrift ⇢ Arrow at speed** – JNI bridge (`surfingthriftjni`) decodes deeply nested payloads (e.g. `DeepEvent`) into Arrow `VectorSchemaRoot`s with SIMD-aware native code and Java fallbacks.
- **MPI shuffle & cogroup primitives** – one-sided RMA or two-sided send/recv data movers balance thousands of Arrow RecordBatches across ranks with optional Thrift ingest.
- **End-to-end benchmarks** – repeatable runners contrast JNI vs Java decode, throttle multi-rank shuffle/cogroup, and emit size + throughput summaries for easy regression tracking.
- **Automatic memory management** – Configurable disk spilling prevents OOM errors when processing large datasets. Supports multi-directory spilling for 2-4x I/O throughput. See [MEMORY_MANAGEMENT.md](MEMORY_MANAGEMENT.md) for details.

## Quick Start
```bash
./scripts/build_install.sh          # C++ libraries, Arrow deps (APT or source)
cmake --build build                 # native targets, including JNI library
mvn -q -f surfingthriftjni/pom.xml -Darrow.version=12.0.0 -DskipTests package
```
Run validation:
```bash
./scripts/run_tests.sh
```

## Key Workloads
### 1. DeepEvent JNI Benchmark
Profiles nested Thrift payload decode via JNI vs pure Java.
```bash
scripts/run_deep_event_bench.sh
```
Outputs under `build/deep_event_*.txt` include:
- JNI & Java best latencies (array / mmap / pooled `ByteBuffer`)
- Payload byte + MB sizes and row counts
Tune runs with environment variables:
- `DEEP_EVENT_SMALL_ROWS`, `DEEP_EVENT_LARGE_ROWS`
- `DEEP_EVENT_SMALL_ITERS`, `DEEP_EVENT_LARGE_ITERS`
- `DEEP_EVENT_FORCE_REGEN=1` to rebuild datasets

### 2. MPI Shuffle & Cogroup
High-cardinality join/load tests that optionally source Thrift payloads.
```bash
mpiexec -np 4 java -Djava.library.path=$PWD/build \
  -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
  com.pinterest.drsquirrel.jni.JniCogroupLoadRunner --two
```
Use Thrift inputs:
```bash
mpiexec -np 4 java -Djava.library.path=$PWD/build \
  -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
  com.pinterest.drsquirrel.jni.JniCogroupLoadRunner --two \
  --thrift-path src/bench/deep_event.thrift \
  --thrift-struct DeepEvent \
  --payload-left /tmp/deep_left.bin \
  --payload-right /tmp/deep_right.bin \
  --key-field event_id
```
Rank 0 prints decode throughput, global row counts, and iteration stats; append `SHUFFLE_LOAD_OUT=/tmp/cogroup.csv` for CSV log aggregation.

### 3. All-cores Helpers
Local stress tests pin every hardware thread.
```bash
./scripts/run_shuffle_all_cores.sh
./scripts/run_cogroup_all_cores.sh
./scripts/run_all_load_all_cores.sh
```

### 4. Flink DataStream (DeepEvent datagen)
Demonstrates how to feed Flink's datagen source into the generated `DeepEvent` Java stub and access nested fields downstream.
```bash
mvn -q -f drsquirrel-java-project/pom.xml -Darrow.version=12.0.0 -DskipTests package
FLINK_HOME=/path/to/flink \
  "$FLINK_HOME/bin/flink" run \
  --class com.pinterest.drsquirrel.flink.DeepEventDataStreamExample \
  drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar
```
Tune with JVM system properties, e.g. `-Ddeep.event.count=100000 -Ddeep.event.rate=20000` to control record count and generator rate.

## Tools & Integrations
- **Thrift schema conversion** – `thrift2arrow` (C++) and `org.apache.thrift.ext.*` (Java) translate `.thrift` IDL into Arrow schemas / builders.
- **Parquet / CSV dumps** – `com.pinterest.drsquirrel.tools.ThriftToParquet` converts Thrift payload batches with either Java or JNI decoder paths.
- **Parquet folder operations** – Read/write collections of Parquet files across C++, Java, and Python. See [PARQUET_FOLDER_USAGE.md](PARQUET_FOLDER_USAGE.md) for details.
- **Spark RAPIDS demo** – `scripts/run_fake_cogroup_example.sh` and `scripts/run_rapids_groupby.sh` showcase GPU-accelerated group-by on decoded Arrow columns.
- **Python shim** – `examples/python/cogroup_benchmark.py` drives the native processors through Arrow C Data using PyArrow (MPI optional).

## Building Blocks
### Native (C++)
```
cmake -S . -B build -GNinja
cmake --build build
```
Key targets:
- `surfingdb_core` – Arrow shuffle/cogroup kernels
- `thrift2arrow` – Thrift ⇢ Arrow CLI
- `parquet_folder_tool` – Parquet folder read/write utility
- `GenMabsPayloads*` – dataset generators for benchmarks

### Java / JNI
**Modular Build (Recommended):**
```
mvn clean install  # Builds all modules: surfingthriftjni, surfing-parquet-java, drsquirrel-java
```

**Individual Modules:**
```
mvn -f surfingthriftjni/pom.xml package        # Thrift→Arrow JNI
mvn -f surfing-parquet-java/pom.xml package    # Parquet utilities
mvn -f drsquirrel-java-project/pom.xml package # MPI/Flink workflows
```

Artifacts:
- `surfingthriftjni/target/surfingthriftjni-1.0-SNAPSHOT.jar` + native lib
- `surfing-parquet-java/target/surfing-parquet-java-1.0-SNAPSHOT.jar`
- `drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar`

See [MODULAR_BUILD.md](MODULAR_BUILD.md) for details on the new modular structure.

### Python
```
python -m venv .venv && source .venv/bin/activate
pip install pyarrow
cmake --build build --target surfingprocessorspy
PYTHONPATH=build python examples/python/cogroup_benchmark.py --rows 100000 --iters 3
```
Add `mpiexec -np <ranks>` to distribute across a cluster.

## Configuration Cheatsheet

### General Configuration
| Variable | Purpose |
| --- | --- |
| `SHUFFLE_TEST_SEED` | Seed randomized MPI loads |
| `COGROUP_THRIFT_FILE` / `COGROUP_THRIFT_STRUCT` | Enable Thrift ingestion in MPI runners |
| `COGROUP_THRIFT_PAYLOAD[_LEFT/_RIGHT]` | Provide pre-generated Thrift frames (`[u32 len][payload]` format) |
| `DEEP_EVENT_SMALL_ROWS`, `DEEP_EVENT_LARGE_ROWS` | Control dataset sizes for JNI benchmark |
| `SURF_SIMD=ON` | Enable SIMD build for JNI decoder |
| `SURF_THRIFT_DECODE_THREADS` | Override parallel decode threads in JNI |

### Memory Management & Disk Spilling
Configure automatic disk spilling to handle datasets larger than available RAM:

| Variable | Purpose | Default |
| --- | --- | --- |
| `SURFING_MAX_BATCH_MEMORY` | Maximum memory per batch (bytes) | 512MB |
| `SURFING_TEMP_DIR` | Single temp directory for spilled files | `/tmp/surfing_spill` |
| `SURFING_TEMP_DIRS` | Multiple temp directories (comma-separated) for parallel I/O | - |
| `SURFING_LOAD_BALANCING` | Load balancing strategy: `ROUND_ROBIN`, `SPACE_AWARE`, `RANDOM` | `ROUND_ROBIN` |
| `SURFING_ENABLE_SPILLING` | Enable/disable disk spilling | `1` (enabled) |

**Example - Multi-directory spilling for 2-4x I/O throughput:**
```bash
# Distribute spill across multiple NVMe drives
export SURFING_TEMP_DIRS="/mnt/nvme0/spill,/mnt/nvme1/spill,/mnt/nvme2/spill"
export SURFING_LOAD_BALANCING=ROUND_ROBIN
export SURFING_MAX_BATCH_MEMORY=1073741824  # 1GB threshold

# Java API alternative
java -cp ... -Djava.library.path=... YourApp
```

See [MEMORY_MANAGEMENT.md](MEMORY_MANAGEMENT.md) for detailed configuration, performance tuning, and troubleshooting.

## Contributing
1. Fork / branch from `main`.
2. Run `./scripts/run_tests.sh` (or targeted modules) before submitting.
3. Open a PR describing the workload and datasets touched.

---
Need help reproducing a benchmark or integrating an Arrow workflow? Check the scripts under `scripts/` and open an issue with the command you ran and the payload description.
