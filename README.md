# Surfing DB

High-performance shuffle and cogroup primitives for Apache Arrow RecordBatches with MPI, plus a lightweight Thrift IDL → Arrow schema toolkit and Java/JNI bindings.

this is originally build in 2021 in hawaii

## Features
- Arrow-native shuffle (one-sided MPI RMA and two-sided send/recv)
- Arrow-native cogroup for co-partitioning two inputs by key
- Thrift `.thrift` → Arrow Schema converter (+ Matcha `mschema`)
- Java integrations: JNI wrappers for shuffle/cogroup and a JNI Thrift decoder
- Example Java app demonstrating MPI cogroup

## Quick Start
Prerequisites (Ubuntu/Debian):
- C++: build-essential, CMake/Ninja, OpenMPI, OpenMP, OpenSSL, glog, Thrift, Apache Arrow/Parquet dev
- Java: OpenJDK 8+, Maven

One-shot build (installs Arrow via APT by default):
- `./scripts/build_install.sh`
- Enable MPI tests at configure time: `CMAKE_EXTRA_FLAGS=-DENABLE_MPI_TESTS=ON ./scripts/build_install.sh`

Run tests:
- All (C++ unit, MPI if enabled, Java): `./scripts/run_tests.sh`
- Skip MPI: `./scripts/run_tests.sh --no-mpi`
- Nested MPI only: `./scripts/run_nested_mpi_tests.sh --np all`

## Building Manually
Configure + build:
- `cmake -S . -B build -GNinja`
- `ninja -C build`

Java modules:
- `mvn -f drsquirrel-java/pom.xml -Darrow.version=12.0.0 -DskipTests package`
- JNI Thrift decoder: `mvn -f surfingthriftjni/pom.xml -Darrow.version=12.0.0 -DskipTests package`

## Running (JNI under MPI)
Set native library path:
- `export LD_LIBRARY_PATH=$PWD/build:$LD_LIBRARY_PATH`

Shuffle load (one-sided by default):
- `mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniShuffleLoadRunner`
- Two-sided: add `two`

Cogroup load:
- `mpiexec -np 4 java -Djava.library.path=$PWD/build -cp drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar com.pinterest.drsquirrel.jni.JniCogroupLoadRunner`
- Two-sided: add `two`

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
Output samples per rank land under `build/examples/fake-cogroup-out/`.

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

Note: Scripts assume Ubuntu with `apt`. Arrow C++ 12 can be installed via APT or built from source.

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
