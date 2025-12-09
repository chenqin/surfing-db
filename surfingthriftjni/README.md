# surfingthriftjni (Java)

Minimal Java API for the native Thrift → Arrow JNI bridge.

- Native library target: `libsurfingthriftjni.so` (built by this module under `target/nativebuild` and packaged into a platform-specific natives JAR)
- Java wrapper: `org.surfing.drsquirrel.jni.NativeThriftDecoder`
- Converter API (pure Java fallback):
  - `org.apache.thrift.ext.ThriftToArrowConverter`
  - `org.apache.thrift.ext.GenericThriftToArrowConverter`

Build steps
- Build (Java + native + natives JAR):
  - `mvn -q -f surfingthriftjni/pom.xml -Darrow.version=12.0.0 -DskipTests package`
  - Optional SIMD build: `SURF_SIMD=ON mvn -q -f surfingthriftjni/pom.xml -DskipTests package`

Run benchmarks
- End-to-end JNI vs Java decode benchmarks are wired under the `jni-bench` profile:
  - `mvn -q -f surfingthriftjni/pom.xml -P jni-bench -Darrow.version=12.0.0 -DskipTests verify`
  - This builds native tools, generates datasets, and runs:
    - Small payloads (array mode)
    - Large payloads (mmap ByteBuffer)
    - Large payloads (pooled direct ByteBuffer)
  - Outputs go to `target/bench_*.txt` and a summary is printed at the end.
- DeepEvent payload benchmarks (mirroring the historical Surfing DeepEvent workload):
  - `scripts/run_deep_event_bench.sh` builds the JNI artifacts, generates small and large DeepEvent payloads, runs `JniDecodeBench`, and emits results under `build/deep_event_*.txt` (including the Java baseline when the corresponding TBase class is available).
  - Advanced knobs: set `DEEP_EVENT_SMALL_ROWS`, `DEEP_EVENT_LARGE_ROWS`, `DEEP_EVENT_SMALL_PAYLOAD`, `DEEP_EVENT_LARGE_PAYLOAD`, or `DEEP_EVENT_FORCE_REGEN=1` to tweak dataset sizes, output paths, or force regeneration. Use `DEEP_EVENT_SMALL_ITERS` / `DEEP_EVENT_LARGE_ITERS` to control the number of benchmark iterations per mode.
  - The script prints a summary that includes payload byte/MB sizes alongside the best JNI/Java timings for quick comparisons.

Running with JNI
- Preferred: put the natives JAR on the classpath — the loader auto-extracts and loads `META-INF/lib/<os>-<arch>/libsurfingthriftjni.*`.
- Or, point `java.library.path` to the native build dir.
- Examples:

```
// Using natives JAR
java -cp \
  surfingthriftjni/target/surfingthriftjni-1.0-SNAPSHOT-jar-with-dependencies.jar: \
  surfingthriftjni/target/surfingthriftjni-1.0-SNAPSHOT-natives-<os>-<arch>.jar \
  com.your.app.Main

// Using java.library.path
java -Djava.library.path=$(pwd)/surfingthriftjni/target/nativebuild \
  -cp surfingthriftjni/target/surfingthriftjni-1.0-SNAPSHOT-jar-with-dependencies.jar \
  com.your.app.Main
```

Notes
- If the native library is not present, only the pure Java converter path will work.
- Package note: this module defines `org.surfing.drsquirrel.jni.NativeThriftDecoder` and
  `org.apache.thrift.ext.*`, which also exist in `drsquirrel-java-project`. Do not put both
  jars on the same classpath; prefer one or the other per application.

Concurrency and tuning
- Native decode threads (JNI): set `SURF_THRIFT_DECODE_THREADS` in the environment to control parallelism for the direct `ByteBuffer[]` path. Defaults to min(hardware_concurrency, 8).
- Byte[] to direct path: by default, when decoding `byte[][]` and the batch size is large (>= 1024), the Java wrapper copies payloads into direct `ByteBuffer`s and calls the JNI direct path to leverage multi-threaded decode. Tune via:
  - `-Dsurfing.decode.toDirect=true|false` (default true)
  - `-Dsurfing.decode.toDirectThreshold=<int>` (default 1024)
- Fast Java decoder: `org.apache.thrift.ext.FastThriftBinaryDecoder` avoids TBase object allocation for flat schemas (primitives + strings). For complex types (lists/maps/structs), it falls back to the generic converter.

SIMD and cache friendliness
- Enable SIMD-friendly build (on x86_64: AVX2 flags) by setting `SURF_SIMD=ON` when packaging. Some list-of-primitive fast paths bulk-borrow and use vectorizable loops.
- The JNI decode processes rows in micro-batches and writes column buffers in a columnar (SoA) layout to reduce cache misses when flushing to Arrow builders.
