# Surfing DB

High-performance shuffle and cogroup primitives for Apache Arrow RecordBatches with MPI, plus a lightweight Thrift IDL → Arrow schema toolkit and Java/JNI bindings.

this is originally build in 2021 in hawaii

## Features
- Arrow-native shuffle (one-sided MPI RMA and two-sided send/recv)
- Arrow-native cogroup for co-partitioning two inputs by key
- Thrift `.thrift` → Arrow Schema converter (+ Matcha `mschema`)
- Java integrations: JNI wrappers for shuffle/cogroup and a JNI Thrift decoder
- Python bindings: pybind11 module exposing shuffle/cogroup over Arrow C Data
- Example Java app demonstrating MPI cogroup

## Quick Start
Prerequisites (Ubuntu/Debian):
- C++: build-essential, CMake/Ninja, OpenMPI, OpenMP, OpenSSL, glog, Thrift, Apache Arrow/Parquet dev
- Java: OpenJDK 8+, Maven

One-shot build (installs Arrow via APT by default):
- `./scripts/build_install.sh`
- Enable MPI tests at configure time: `CMAKE_EXTRA_FLAGS=-DENABLE_MPI_TESTS=ON ./scripts/build_install.sh`
- If APT cannot resolve Arrow dependencies (e.g. missing `libabsl20220623t64`/`libre2-10`), rerun with `./scripts/build_install.sh --build-arrow` to download and build Arrow C++ 12.0.0 locally under `~/arrow-12-install`.

Run tests:
- All (C++ unit, MPI if enabled, Java): `./scripts/run_tests.sh`
- Skip MPI: `./scripts/run_tests.sh --no-mpi`
- Nested MPI only: `./scripts/run_nested_mpi_tests.sh --np all`

## Building Manually
Configure + build:
- `cmake -S . -B build -GNinja`
- `ninja -C build`
- Core C++ libraries live in the `core/` subproject (`surfingdb_core` aggregates meta/table/connector).

Java modules:
- `mvn -f drsquirrel-java-project/pom.xml -Darrow.version=12.0.0 -DskipTests package`
- JNI Thrift decoder: `mvn -f surfingthriftjni/pom.xml -Darrow.version=12.0.0 -DskipTests package`

Python bindings:
- Build native module: `cmake --build build --target surfingprocessorspy`
- Create a virtualenv (recommended) and install PyArrow: `python3 -m venv .venv && . .venv/bin/activate && python -m pip install pyarrow`
- Run the cogroup benchmark (single rank): `. .venv/bin/activate && PYTHONPATH=build python examples/python/cogroup_benchmark.py --rows 100000 --iters 3`
- Under MPI: `mpiexec -np 4 --use-hwthread-cpus --oversubscribe --map-by core --bind-to core env PYTHONPATH=build .venv/bin/python examples/python/cogroup_benchmark.py --rows 100000 --iters 3`

## Running (JNI under MPI)
Set native library path:
- `export LD_LIBRARY_PATH=$PWD/build:$LD_LIBRARY_PATH`

Shuffle load (one-sided by default):
- `mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniShuffleLoadRunner`
- Two-sided: add `two`

Cogroup load:
- `mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniCogroupLoadRunner`
- Two-sided: add `two`
- Decode pre-generated Thrift payloads: append `--thrift-path src/bench/deep_event.thrift --thrift-struct DeepEvent --payload-left /tmp/deep_left.bin --payload-right /tmp/deep_right.bin --key-field event_id`

All-cores helpers (local machine):
- Shuffle: `./scripts/run_shuffle_all_cores.sh`
- Cogroup: `./scripts/run_cogroup_all_cores.sh`
- Both: `./scripts/run_all_load_all_cores.sh`

## Example: Fake Cogroup App (Java + MPI)
Build and run (np=4):
- `bash scripts/run_fake_cogroup_example.sh --build --np 4 --mode one --rows 100000 --iters 1`
Two-sided:
- `bash scripts/run_fake_cogroup_example.sh --np 2 --mode two --rows 50000 --iters 2`
Optional post-sort by a field present in outputs (e.g. `la` or `rb`):
- `--sort-by la`
- Decode deep nested Thrift payloads via surfingthriftjni: `bash scripts/run_fake_cogroup_example.sh --deep-thrift --np 4 --iters 2 --sort-by event_id`
- Bring your own payloads: pass `--thrift-path PATH --thrift-struct StructName --payload-left /path/to/left.bin [--payload-right ...] [--key-field field]`.
Output samples per rank land under `build/examples/fake-cogroup-out/`.
The bundled `DeepEvent` schema now includes a five-level nested structure (`deep_universe`) composed of lists and maps of structs to stress deep decoding.

## Thrift → Arrow
C++ usage (library): see `src/meta/thrift_parser.h`.

CLI tool `thrift2arrow`:
- Build: `ninja -C build thrift2arrow`
- Usage:
  - `./build/thrift2arrow path/to/file.thrift StructName`
  - `--flatten` to flatten direct struct fields
  - `--sep=__` to customize prefix separator
  - `--flatten-list-structs` / `--flatten-map-structs` to expand nested struct collections
  - `--raw` to print Arrow `Schema::ToString()`

## JNI Thrift Decoder (Optional)
Module: `surfingthriftjni/` — builds `libsurfingthriftjni.*` and a jar-with-dependencies.

Benchmarks:
- `mvn -f surfingthriftjni/pom.xml -P jni-bench -Darrow.version=12.0.0 -DskipTests verify`

Programmatic use (Java): `com.pinterest.drsquirrel.jni.NativeThriftDecoder` — decode `byte[]` or `ByteBuffer` payload batches into Arrow `VectorSchemaRoot`.

## Environment & Tuning
- `SHUFFLE_TEST_SEED` — seed randomized MPI tests
- `MATCHA_SPILL_DIR` — pre-shuffle partition spill directory
- `MATCHA_POST_SHUFFLE_SPILL_DIR` — spill merged output; `MATCHA_POST_SHUFFLE_SPILL_MIN_ROWS`, `MATCHA_POST_SHUFFLE_SPILL_MAX_ROWS`
- MPI flags: `--use-hwthread-cpus --oversubscribe --map-by core --bind-to core`; prefer `--mca btl_tcp_if_include <iface>` on dense local runs

## Contributing
1. Create a feature branch
2. Build and run tests (`./scripts/run_tests.sh`)
3. Open a PR against `main`

---

Note: Scripts assume Ubuntu with `apt`. Arrow C++ 12 can be installed via APT or, if dependencies are unavailable, built from source with `./scripts/build_install.sh --build-arrow` (outputs static libs in `~/arrow-12-install`).

## Architecture Overview

High-level flow for shuffle/cogroup:

```
Java App → NativeProcessors (JNI) → C++ processors → MPI world
               │                                 │
               │ Arrow C Data Interface          │
               └──────────────┬──────────────────┘
                              │
                        One-sided (RMA)
                        - MPI_Win + MPI_Get over
                          concatenated Arrow IPC buffers

                        Two-sided (send/recv)
                        - MPI_Isend / MPI_Irecv into
                          a receive buffer, then
                          Arrow IPC deserialization
```

Thrift payloads → Arrow (optional fast path):

```
byte[] / ByteBuffer → NativeThriftDecoder (JNI) → C++ decode → Arrow C Data → VectorSchemaRoot
                 (fallback) GenericThriftToArrowConverter (Java)
```

Key implementation notes:
- Partitioning uses Arrow Scalar hashing of the key column; default partitioner is hash % world.
- Serialization uses Arrow IPC to move per-destination partitions between ranks.
- Critical paths use OpenMP for parallel loops (export, group, deserialize).

## Performance Notes
- MPI execution flags for dense local testing:
  - `--use-hwthread-cpus --oversubscribe --map-by core --bind-to core`
  - Prefer `--mca btl_tcp_if_include <iface>` over excluding `lo,docker0`.
- Threads: OpenMP loops honor `OMP_NUM_THREADS`; tune per machine.
- Spill-to-disk safeguards (optional):
  - Pre-shuffle: `MATCHA_SPILL_DIR`, `MATCHA_SPILL_MAX_ROWS`
  - Post-shuffle: `MATCHA_POST_SHUFFLE_SPILL_DIR`, `MATCHA_POST_SHUFFLE_SPILL_MIN_ROWS`, `MATCHA_POST_SHUFFLE_SPILL_MAX_ROWS`
- Load tests and CSV summaries:
  - All cores (local): `./scripts/run_shuffle_all_cores.sh`, `./scripts/run_cogroup_all_cores.sh`, `./scripts/run_all_load_all_cores.sh`
  - JNI Thrift decode benches: `./scripts/run_thrift_arrow_bench.sh` or `mvn -f surfingthriftjni/pom.xml -P jni-bench verify`

## Performance Results

Thrift decode (JNI vs Java)
- Run: `make thrift-bench`
- Outputs written under `build/bench_small_array.txt`, `build/bench_large_bb.txt`, `build/bench_large_bbp.txt`.
- The script prints a summary at the end, e.g.:
  - `Small array — JNI: 3.2 GB/s`
  - `Small array — Java: 1.1 GB/s`
  - `Large mmap — JNI: 8.9 GB/s`
  - `Large pooled — JNI: 9.3 GB/s`

JNI shuffle/cogroup (all cores)
- Shuffle: `make shuffle-all-cores`
- Cogroup: `make cogroup-all-cores`
- Both: `make load-all-cores`
- CSVs are written to `build/` and summarized automatically. You can re-run analysis over CSVs:
  - `./scripts/analyze_shuffle_csv.py build/jni_shuffle_one_n$(nproc).csv build/jni_shuffle_two_n$(nproc).csv`
  - `./scripts/analyze_shuffle_csv.py build/jni_cogroup_one_n$(nproc).csv build/jni_cogroup_two_n$(nproc).csv`

Tip: Set `SHUFFLE_LOAD_ROWS` and `SHUFFLE_LOAD_ITERS` to scale load; set `IFACE` to pin to a specific NIC (e.g. `eth0`).

## Makefile Quick Targets

Common shortcuts:
- `make build` — install deps + build (APT Arrow by default)
- `make test` — run C++ unit, MPI (if enabled), Java tests
- `make test-no-mpi` — run tests without MPI
- `make nested-mpi` — run nested shuffle/cogroup tests
- `make java-jar` — build shaded Java jar
- `make thrift-bench` — run JNI vs Java Thrift decode benches
- `make shuffle-all-cores` — run JNI shuffle across cores
- `make cogroup-all-cores` — run JNI cogroup across cores
- `make load-all-cores` — run both shuffle + cogroup helpers
- `make example` — build + run the fake cogroup example

## Security & Maintenance
- Dependabot is enabled via `.github/dependabot.yml` for GitHub Actions and Maven modules.
- Report security issues privately via GitHub Security Advisories (see `.github/SECURITY.md`).

## Kafka Example (Java-native)

A lightweight Java consumer that emits Arrow batches with schema `(topic, payload)` is provided as `KafkaSourceArrow`.

Environment options for bootstrap:
- `KAFKA_SERVERSET`: path to a file listing brokers (one per line), or
- Set bootstrap directly via builder.

Example:

```java
import java.time.Duration;
import java.util.Arrays;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import com.pinterest.drsquirrel.kafka.KafkaSourceArrow;

var alloc = new RootAllocator();
var src = KafkaSourceArrow.newBuilder()
    .setAllocator(alloc)
    .setServersetPath(System.getenv("KAFKA_SERVERSET")) // or setBootstrapServers("localhost:9092")
    .setGroupId("flink-watcher-test")
    .setTopics(Arrays.asList("metrics_topic", "logs_topic"))
    .setPollTimeout(Duration.ofMillis(200))
    .build();

VectorSchemaRoot batch = src.pollOnce(1000);
try {
  // process batch (fields: topic, payload)
} finally {
  batch.close();
  src.close();
  alloc.close();
}
```

For a JNI-backed variant (`NativeKafkaConnector`), enable the native Kafka build and ensure `libsurfingkafkajni.*` is on `java.library.path`.

## MCP Server + MPI Worker (Batch Queue)

This repo provides a minimal MCP server and an MPI worker runner to execute FIFO tasks across the cluster.

- Server: `scripts/mcp_server.py` (in-memory queue, single active lease)
  - Start: `python3 scripts/mcp_server.py --port 8080`
  - Enqueue task (example):
    - `curl -sS -X POST localhost:8080/enqueue -H 'Content-Type: application/json' -d '{"taskId":"t1","mode":"shuffle","oneSided":true,"keyField":"key","leftS3":["s3://bucket/input1"],"thrift":{"dir":"s3://bucket/thrift","struct":"MyStruct","file":"schema.thrift"},"outputS3":"s3://bucket/out"}'`
  - Lease (worker): `GET /lease` returns JSON task or `NONE`
  - Complete: `POST /complete {"taskId":"t1"}`

- Worker: Java MPI runner `com.pinterest.drsquirrel.jni.McpWorkerRunner`
  - Build shaded jar: `make java-jar`
  - Run under MPI (all hosts):
    - `mpiexec -np <N> java -Djava.library.path=$PWD/build -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.McpWorkerRunner http://<server_host>:8080`
  - Behavior:
    - Rank 0 leases the next task, broadcasts to all ranks, executes shuffle/cogroup, uploads Arrow outputs to the `outputS3` prefix with filenames containing rank (e.g., `rank-3.arrow`).
    - Inputs are synced locally via `aws s3 cp --recursive`. Payload files are expected to be line-delimited Base64 Thrift Binary messages.
    - Thrift schema is pulled from `thrift.dir` and struct is chosen via `thrift.struct` (optional `thrift.file`).

Notes:
- Requires AWS CLI on each host with appropriate credentials.
- MPI tasks run strictly one-at-a-time via the server lease (FIFO).
- Environment:
  - `MCP_POLL_MS` (default 2000), `MCP_ONE_SIDED` (default true). Or pass server URL as the first arg.

### MCP Server (fastmcp, StreamableHTTP + async progress)

A fastmcp-based MCP server is available with StreamableHTTP transport and async progress reporting:

- File: `scripts/mcp_fastmcp_server.py`
- Install and run (in a venv):
  - `python3 -m venv .venv && . .venv/bin/activate`
  - `pip install fastmcp uvicorn`
  - `python scripts/mcp_fastmcp_server.py` (serves on `:8081`, path `/mcp`)
- Tool: `submit_task` (streams progress via MCP progress notifications)
  - Args: `mode (shuffle|cogroup)`, `keyField`, `outputS3`, `leftS3`, `rightS3`, `oneSided`, `thriftDir`, `thriftStruct`, `thriftFile?`, `np`, `hostfile?`, `iface?`, `javaJar?`, `javaLibPath?`
  - Behavior: acquires a global FIFO lock, spawns `mpiexec … McpWorkerRunner json:<base64>`, forwards coarse progress from process output, and returns `{taskId,status}` on completion.
- StreamingHTTP path: `/mcp` (connect with an MCP client supporting StreamableHTTP).

- Launch under MPI (two-sided cogroup)
  - `mpiexec -np 4 --mca btl_tcp_if_exclude lo,docker0 \
     java -Djava.library.path=$PWD/build \
     -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
     com.pinterest.drsquirrel.jni.JniCogroupLoadRunner two`

- Tune for local multi-core stability
  - Add flags for dense local runs: `--use-hwthread-cpus --oversubscribe --map-by core --bind-to core`
  - Prefer including your main NIC instead of excluding: `--mca btl_tcp_if_include eth0`
  - Example (24 ranks, two-sided):
    - `mpiexec -np $(nproc) --use-hwthread-cpus --oversubscribe --map-by core --bind-to core \
       --mca btl_tcp_if_include eth0 \
       java -Djava.library.path=$PWD/build \
       -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
       com.pinterest.drsquirrel.jni.JniCogroupLoadRunner two`

- Maven alternative (profile)
  - Uses the same runner and flags via `mpiexec`:
    - `mvn -f drsquirrel-java-project/pom.xml -Darrow.version=12.0.0 -P jni-cogroup-load -DskipTests \
       package verify -Dnp=4 -Drows=200000 -Diters=1 -Dout=$(pwd)/build/jni_cogroup.csv`
  - Interface pinning (optional): `-Diface=eth0` (uses `--mca btl_tcp_if_include eth0`).

- Minimal Java snippet (NativeProcessors.cogroup)
  - Example of constructing two small batches and invoking JNI cogroup:

```
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.*;
import com.pinterest.drsquirrel.jni.NativeProcessors;
import java.util.Arrays;

static VectorSchemaRoot makeBatch(BufferAllocator alloc, String valName) {
  Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
  Field val = new Field(valName, FieldType.nullable(new ArrowType.Int(32, true)), null);
  Schema schema = new Schema(Arrays.asList(key, val));
  VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
  BigIntVector keyVec = (BigIntVector) root.getVector("key");
  IntVector valVec = (IntVector) root.getVector(valName);
  root.allocateNew();
  keyVec.setSafe(0, 1); valVec.setSafe(0, 10);
  keyVec.setSafe(1, 2); valVec.setSafe(1, 20);
  keyVec.setValueCount(2); valVec.setValueCount(2);
  root.setRowCount(2);
  return root;
}

try (RootAllocator alloc = new RootAllocator()) {
  try (VectorSchemaRoot left = makeBatch(alloc, "la");
       VectorSchemaRoot right = makeBatch(alloc, "rb")) {
    boolean oneSided = true; // set false for two-sided
    int rank = 0, world = 1; // MPI rank/world (native is authoritative)
    VectorSchemaRoot[] outs = NativeProcessors.cogroup(alloc, left, right, "key", oneSided, rank, world);
    VectorSchemaRoot leftOut = outs[0];
    VectorSchemaRoot rightOut = outs[1];
    // Use outputs... then close when done
    leftOut.close();
    rightOut.close();
  }
}
```
