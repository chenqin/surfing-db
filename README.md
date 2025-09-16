# FlinkJobWatcher

GLOG_log_dir=~ mpirun -np 18 FlinkJobWatcher ../surfingdb-java.jar testgroupid

# MABS
mpirun --hostfile ~/matcha/hostfile ~/matcha/build/MABS ~/matcha/surfing-db-java.jar test4 --mca oob_tcp_port_min_v4 7337 -mca btl_tcp_if_exclude lo,docker0 -mca orte_base_help_aggregate 0

# contab check
1 * * * * ~/matcha/demon.sh MABS

created @Maui, Hawaii, U.S.A since 2021

# Build & Test (Scripts)

- Build + install deps + build project:
  - `./scripts/build_install.sh`
  - By default installs Apache Arrow C++ from APT (libarrow-dev, libparquet-dev, libarrow-dataset-dev) and builds the project.
  - Options:
    - `--no-sudo` to avoid sudo for apt
    - `--use-system-arrow` (default) to install Arrow via APT
    - `--build-arrow` to build Arrow from source
    - `--arrow-prefix <DIR>` Arrow install prefix for source build (default: `$HOME/arrow-12-install`)
    - `--arrow-version <X.Y.Z>` Arrow version for source build (default: `12.0.0`)
    
  - Options:
    - `--no-sudo` to avoid sudo for apt
    - `--arrow-prefix <DIR>` to set Arrow install prefix (default: `$HOME/arrow-12-install`)

- Run tests:
  - `./scripts/run_tests.sh`
  - Runs C++ unit tests, MPI shuffle tests (np=2,4), and Java tests.
  - For randomized MPI tests you can set `SHUFFLE_TEST_SEED=<uint64>` to make runs deterministic.
  - Nested MPI tests (shuffle + cogroup):
    - Quick: `./scripts/run_nested_mpi_tests.sh --np 2`
    - Full: `./scripts/run_nested_mpi_tests.sh --np all` (np=2 and np=4)
    - Rebuild targets: add `--build`

## How To Run Locally (Quick Guide)

Prerequisites
- Ubuntu/Debian with `apt` (for system Arrow install)
- OpenJDK 8 (installed by script)
- OpenMPI (installed by script: `openmpi-bin`, `libopenmpi-dev`)

Build
- Default (uses system Arrow packages):
  - `./scripts/build_install.sh`
- Enable MPI tests at configure time:
  - `CMAKE_EXTRA_FLAGS=-DENABLE_MPI_TESTS=ON ./scripts/build_install.sh`
  

Run all tests
- Full suite (includes MPI if enabled):
  - `./scripts/run_tests.sh`
- Quick (skip MPI):
  - `./scripts/run_tests.sh --no-mpi`
 - Nested-only:
   - `./scripts/run_nested_mpi_tests.sh --np all`

Run MPI tests manually
- From `build/`:
  - `export SHUFFLE_TEST_SEED=12345`
  - `ctest -R "MpiShuffleTest|MpiTwoSideShuffleTest_np2|MpiTwoSideShuffleTest_np4|MpiShuffleRandom_one_np4|MpiShuffleRandom_two_np4" --output-on-failure`

Load test for processors shuffle
- Enable MPI tests at configure time:
  - `CMAKE_EXTRA_FLAGS=-DENABLE_MPI_TESTS=ON ./scripts/build_install.sh`
- The load test target is registered but disabled by default in CTest. Run manually, e.g.:
  - One-sided shuffle, 2 ranks, 200k rows/rank, 3 iters (defaults):
    - `mpiexec -np 2 ./MpiShuffleLoadTest`
  - Two-sided shuffle:
    - `mpiexec -np 2 ./MpiShuffleLoadTest two`
  - Override size/iters via env:
    - `SHUFFLE_LOAD_ROWS=1000000 SHUFFLE_LOAD_ITERS=5 mpiexec -np 4 ./MpiShuffleLoadTest`
  - Output includes per-iteration throughput and best/avg seconds.

Run Java tests (JNI included)
- Ensure native libs are discoverable:
  - `export LD_LIBRARY_PATH=$PWD/build:$LD_LIBRARY_PATH`
- Run:
  - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 test`

Run nested JNI under MPI (4 ranks)
- Build shaded jar first:
  - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests package`
- Then run nested shuffle + cogroup via mpiexec (profile helper):
  - `mvn -f drsquirrel-java/pom.xml -P jni-nested -Dnp=4 -Dmode=all -DskipTests verify`
  - Or directly:
    - `mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniNestedRunner all`

MPI Java JNI Runner (2 ranks)
- Build shaded jar:
  - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests package`
- Run under MPI:
  - `mpiexec -np 2 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniFlinkJobWatcherRunner`

## Fake Cogroup Example (Java + MPI)

- Build and run the fake source/sink app that calls `NativeProcessors.cogroup` under MPI:
  - Build native + Java + example and run (np=4):
    - `bash scripts/run_fake_cogroup_example.sh --build --np 4 --mode one --rows 100000 --iters 1`
  - Two-sided mode:
    - `bash scripts/run_fake_cogroup_example.sh --np 2 --mode two --rows 50000 --iters 2`
  - Secondary sort after cogroup (by a field present in each output, e.g. `la` or `rb`):
    - `bash scripts/run_fake_cogroup_example.sh --np 2 --mode one --rows 50000 --iters 1 --sort-by la`
  - Output samples per rank are written under `build/examples/fake-cogroup-out/`.


## Kafka test (Java-native, optional)

You can exercise the Java-native Kafka consumer (KafkaSourceArrow) via the MPI Java runner or your own harness by setting:

- `KAFKA_SERVERSET`: Path to a text file with one broker per line, e.g.:
  ```
  localhost:9092
  127.0.0.1:9092
  ```
- `KAFKA_TOPICS`: Comma-separated list of topics, e.g. `metrics_topic,logs_topic`
- `KAFKA_GROUP_ID`: A consumer group id, e.g. `flink-watcher-test`

Then run the MPI Java runner:

```

## Java + MPI Shuffle (Quick Snippet)

- Minimal Java snippet (NativeProcessors.shuffle)
  - Example of creating a small batch and shuffling by a key:

```
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.*;
import com.pinterest.drsquirrel.jni.NativeProcessors;
import java.util.Arrays;

static VectorSchemaRoot makeBatch(BufferAllocator alloc) {
  Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
  Field val = new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null);
  Schema schema = new Schema(Arrays.asList(key, val));
  VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
  BigIntVector keyVec = (BigIntVector) root.getVector("key");
  IntVector valVec = (IntVector) root.getVector("val");
  root.allocateNew();
  keyVec.setSafe(0, 1); valVec.setSafe(0, 10);
  keyVec.setSafe(1, 2); valVec.setSafe(1, 20);
  keyVec.setValueCount(2); valVec.setValueCount(2);
  root.setRowCount(2);
  return root;
}

try (RootAllocator alloc = new RootAllocator()) {
  try (VectorSchemaRoot in = makeBatch(alloc)) {
    boolean oneSided = true; // set false for two-sided
    int rank = 0, world = 1; // MPI rank/world (native is authoritative)
    VectorSchemaRoot out = NativeProcessors.shuffle(alloc, in, "key", oneSided, rank, world);
    // Use output... then close when done
    out.close();
  }
}
```

## Parse .thrift and Convert to Arrow Schema

- The library provides a simple `.thrift` IDL parser that extracts a `struct` and converts it to
  a Matcha `mschema` or Apache Arrow `Schema`.

- C++ usage:

```
#include "meta/thrift_parser.h"
using namespace matcha::meta;

int main() {
  ThriftParseOptions opt;
  opt.default_string_max = 1024;     // bytes per string
  opt.default_list_len = 1024;       // elements per list
  opt.default_map_pairs = 512;       // map entries

  auto ms = ThriftSchemaParser::parseToMSchema("path/to/file.thrift", "MyStruct", opt);
  auto as = ThriftSchemaParser::parseToArrow("path/to/file.thrift", "MyStruct", opt);
  // use ms (matcha::meta::mschema) or as (std::shared_ptr<arrow::Schema>)
}
```

- CLI tool: thrift2arrow
  - Build: `ninja -C build thrift2arrow`
  - Usage:
    - `./build/thrift2arrow path/to/file.thrift StructName` (nested structs preserved)
    - `./build/thrift2arrow path/to/file.thrift StructName --flatten` (flatten direct struct fields into top-level fields with `name_child`)
    - `./build/thrift2arrow path/to/file.thrift StructName --flatten --sep=__` (custom separator for flattened names)
    - `./build/thrift2arrow path/to/file.thrift StructName --flatten --flatten-list-structs --flatten-map-structs` (also flatten list/map of struct into multiple fields)
    - `./build/thrift2arrow path/to/file.thrift StructName --raw` (print Arrow `Schema::ToString()`)

  - Examples (from this repo):
    - `./build/thrift2arrow src/mabs.thrift MabsMetrics`
      - Output (pretty):
        - `timestamp: int64`
        - `service_tags: utf8`
        - `node_tags: utf8`
        - `counters: map<utf8, int64>`
        - `gauges: map<utf8, float32>`
        - `histograms: map<utf8, utf8>`
        - `service_name: utf8`
        - `double_counters: map<utf8, float32>`

    - `./build/thrift2arrow src/mabs.thrift MabsMetrics --raw`
      - Raw Arrow schema string printed for tooling consumption.

- Thrift type mapping (subset):
  - `bool` → Arrow `bool`
  - `byte`/`i8` → Arrow `int8`
  - `i16`/`i32` → Arrow `int32`
  - `i64` → Arrow `int64`
  - `double` (and `float`) → Arrow `float32`
  - `string`/`binary` → Arrow `utf8`
  - `list<T>` → Arrow `list<T>`
  - `map<K,V>` → Arrow `map<K,V>` (nullable)

- Notes:
  - For variable-width/list/map fields, defaults are used for capacity sizing; override via `ThriftParseOptions`.
  - The parser is lightweight and handles common Thrift IDL patterns (structs with primitive/list/map fields, optional/required qualifiers, comments).
  - When flattening, direct struct fields become top-level fields with a name prefix. With `--flatten-list-structs` and/or `--flatten-map-structs`, struct elements under list/map are expanded into multiple fields with the chosen separator.

### Tests for Thrift Parser

- Focused gtests validate nested structs, lists/maps, flattening behavior, error conditions, and real repo schemas:
  - `src/meta/test/TestThriftParser.cpp`
  - `src/meta/test/TestThriftParserNested.cpp`
  - `src/meta/test/TestThriftParserFlatten.cpp`
  - `src/meta/test/TestThriftParserFlattenAdvanced.cpp`
  - `src/meta/test/TestThriftParserNestedComprehensive.cpp`
  - `src/meta/test/TestThriftMabs.cpp` (parses `src/mabs.thrift`)

- Build and run only these tests:
  - `ninja -C build ThriftParserTests`
  - `./build/ThriftParserTests --gtest_color=yes`

## Java: Thrift Payload → Arrow Batch

- The Java module provides a generic converter that takes Thrift-serialized payloads (`byte[]`) and a generated Thrift class, and returns an Apache Arrow `VectorSchemaRoot`.

- Location:
  - Interface: `surfingthriftjni/src/main/java/com/pinterest/drsquirrel/thrift/ThriftToArrowConverter.java`
  - Impl: `surfingthriftjni/src/main/java/com/pinterest/drsquirrel/thrift/GenericThriftToArrowConverter.java`

- Build the Java module:
  - `mvn -q -f surfingthriftjni/pom.xml -Darrow.version=12.0.0 -DskipTests package`
  - Or build all Java modules (aggregator): `mvn -q -DskipTests install`

- Usage example:
  - Given a generated Thrift class `com.example.MyStruct` and a list of `byte[]` payloads (Binary protocol):
    
    ```java
    import java.util.List;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.vector.VectorSchemaRoot;
    import org.apache.thrift.ext.*;
    import com.example.MyStruct; // generated by Thrift

    BufferAllocator alloc = new RootAllocator();
    ThriftToArrowConverter conv = new GenericThriftToArrowConverter();

    // Build Arrow schema from Thrift class if needed
    var schema = conv.toArrowSchema(MyStruct.class);

    // Convert a batch of messages
    VectorSchemaRoot root = conv.convert(payloads, MyStruct.class, alloc);
    try {
      // use root (numRows = payloads.size())
    } finally {
      root.close();
      alloc.close();
    }
    ```

- Mapping notes:
  - `bool` → Arrow `bool`
  - `byte/i8` → Arrow `int8`
  - `i16/i32` → Arrow `int32`
  - `i64` → Arrow `int64`
  - `double/float` → Arrow `float32`
  - `string/binary` → Arrow `utf8`
  - `list<T>`/`set<T>` → Arrow `list<T>`
  - `map<K,V>` → Arrow `map<K,V>` (as List<Struct<key,value>>)

### JNI Variant (Native C++)

- For higher throughput, a JNI bridge decodes Thrift (Binary protocol) in C++ and returns Arrow batches via the C Data interface.
  - Native lib: `libsurfingthriftjni.so` (built in `surfingthriftjni` submodule)
  - Java: `com.pinterest.drsquirrel.jni.NativeThriftDecoder`
  - Java subproject: `surfingthriftjni/` (Maven module with the JNI API, benchmarks)
  - Build: `mvn -q -f surfingthriftjni/pom.xml -Darrow.version=12.0.0 -DskipTests package` (builds native + natives JAR).
  - Benchmarks: `mvn -q -f surfingthriftjni/pom.xml -P jni-bench -Darrow.version=12.0.0 -DskipTests verify`

- Java usage:
  ```java
  import org.apache.arrow.memory.RootAllocator;
  import org.apache.arrow.vector.VectorSchemaRoot;
  import com.pinterest.drsquirrel.jni.NativeThriftDecoder;
  import com.example.thrift.MyStruct; // generated by Thrift

  var alloc = new RootAllocator();
  // New API (preferred): pass the generated Thrift class
  VectorSchemaRoot root = NativeThriftDecoder.convert(alloc, payloads, MyStruct.class);
  // Legacy API (kept for compatibility):
  // VectorSchemaRoot root = NativeThriftDecoder.convert(alloc, payloads, "path/to/schema.thrift", "MyStruct");
  ```

- Notes:
  - Current native path creates schema from `.thrift` and decodes primitive and string fields; complex collections are parsed in schema and left null until extended (follow-ups planned).
  - SIMD considerations: decoding loops are structured to allow vectorization; future work includes batch-reading of numeric fields and bulk string writes (leveraging Arrow’s optimized builders and CPU intrinsics).

mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests package
mpiexec -np 2 java -Djava.library.path=$PWD/build \
  -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
  com.pinterest.drsquirrel.jni.JniFlinkJobWatcherRunner
```

Java MPI load test (JNI shuffle)
- Build shaded jar:
  - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests package`
- One-sided shuffle, 4 ranks, defaults (200k rows/rank, 3 iters):
  - `mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniShuffleLoadRunner`
- Two-sided mode:
  - `mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniShuffleLoadRunner two`
- Override size/iters and log CSV (rank 0):
  - `SHUFFLE_LOAD_ROWS=1000000 SHUFFLE_LOAD_ITERS=2 SHUFFLE_LOAD_OUT=jni_shuffle.csv mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniShuffleLoadRunner`
- Notes:
  - The native JNI initializes MPI internally; each JVM participates in `MPI_COMM_WORLD` under `mpiexec`.
  - OpenMPI may print interface pairing/finalize warnings; add `--mca btl_tcp_if_exclude lo,docker0` or `-quiet` to reduce noise.

Java MPI load test (JNI cogroup)
- Build shaded jar:
  - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests package`
- One-sided cogroup, 4 ranks, defaults:
  - `mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniCogroupLoadRunner`
- Two-sided mode:
  - `mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniCogroupLoadRunner two`
- Maven profile (runs under mpiexec):
  - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -P jni-cogroup-load -DskipTests package verify -Dnp=4 -Dmode=two -Drows=500000 -Diters=2 -Dout=$(pwd)/build/mvn_jni_cogroup.csv`
- Env vars:
  - `SHUFFLE_LOAD_ROWS`, `SHUFFLE_LOAD_ITERS`, `SHUFFLE_LOAD_OUT`

Note: Scripts assume Ubuntu with `apt` available. Arrow C++ 12 is built from source.

## Load Test Helpers (All Cores)

- Shuffle (JNI) across all local cores
  - One- and two-sided runs, CSV + summary:
    - `./scripts/run_shuffle_all_cores.sh`
  - Env overrides:
    - `SHUFFLE_LOAD_ROWS` (default 200000)
    - `SHUFFLE_LOAD_ITERS` (default 1)
    - `IFACE` (e.g., `eth0` to include a single NIC; otherwise excludes `lo,docker0`)
    - `OUT_DIR` (default `build`)

- Cogroup (JNI) across all local cores
  - One- and two-sided runs, CSV + summary:
    - `./scripts/run_cogroup_all_cores.sh`
  - Same env overrides as above.

- Both shuffle + cogroup in one shot (all cores)
  - `./scripts/run_all_load_all_cores.sh`
  - Produces four CSVs in `build/` and a combined summary.

- Maven aggregate profile to run both (all cores)
  - Verify phase triggers both shuffle and cogroup runners via scripts:
    - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -P jni-all-load -DskipTests verify`
  - Options:
    - `-Drows=200000 -Diters=1 -Diface=eth0 -DoutDir=$(pwd)/build`
  - Summaries are printed at the end, sources at:
    - `build/jni_shuffle_one_n$(nproc).csv`, `build/jni_shuffle_two_n$(nproc).csv`
    - `build/jni_cogroup_one_n$(nproc).csv`, `build/jni_cogroup_two_n$(nproc).csv`

## Java + MPI Cogroup (How To)

- Build native and Java artifacts
  - Ensure the native JNI library is built and Java jar is packaged:
    - `CMAKE_EXTRA_FLAGS=-DENABLE_MPI_TESTS=ON ./scripts/build_install.sh`
    - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests package`

- Environment for running Java + JNI
  - Make the native lib visible to the JVM:
    - `export LD_LIBRARY_PATH=$PWD/build:$LD_LIBRARY_PATH`
    - Pass `-Djava.library.path=$PWD/build` to `java`.

- Minimal example program (NativeProcessors.cogroup)
  - The repository provides a ready-to-run example: `com.pinterest.drsquirrel.jni.JniCogroupLoadRunner`.
  - It creates two Arrow batches per rank, calls `NativeProcessors.cogroup`, and reports throughput.

- Launch under MPI (one-sided cogroup)
  - `mpiexec -np 4 --mca btl_tcp_if_exclude lo,docker0 \
     java -Djava.library.path=$PWD/build \
     -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
     com.pinterest.drsquirrel.jni.JniCogroupLoadRunner`

- Launch under MPI (two-sided cogroup)
  - `mpiexec -np 4 --mca btl_tcp_if_exclude lo,docker0 \
     java -Djava.library.path=$PWD/build \
     -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
     com.pinterest.drsquirrel.jni.JniCogroupLoadRunner two`

- Tune for local multi-core stability
  - Add flags for dense local runs: `--use-hwthread-cpus --oversubscribe --map-by core --bind-to core`
  - Prefer including your main NIC instead of excluding: `--mca btl_tcp_if_include eth0`
  - Example (24 ranks, two-sided):
    - `mpiexec -np $(nproc) --use-hwthread-cpus --oversubscribe --map-by core --bind-to core \
       --mca btl_tcp_if_include eth0 \
       java -Djava.library.path=$PWD/build \
       -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
       com.pinterest.drsquirrel.jni.JniCogroupLoadRunner two`

- Maven alternative (profile)
  - Uses the same runner and flags via `mpiexec`:
    - `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -P jni-cogroup-load -DskipTests \
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
