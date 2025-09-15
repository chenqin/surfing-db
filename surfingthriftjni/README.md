# surfingthriftjni (Java)

Minimal Java API for the native Thrift → Arrow JNI bridge.

- Native library target: `libsurfingthriftjni.so` (built by CMake in repo root)
- Java wrapper: `com.pinterest.drsquirrel.jni.NativeThriftDecoder`
- Converter API (pure Java fallback):
  - `com.pinterest.drsquirrel.thrift.ThriftToArrowConverter`
  - `com.pinterest.drsquirrel.thrift.GenericThriftToArrowConverter`

Build steps
- Build native `.so` in repo root:
  - `cmake -S . -B build -G Ninja && ninja -C build surfingthriftjni`
- Build Java jar:
  - `mvn -q -f surfingthriftjni/pom.xml -Darrow.version=12.0.0 -DskipTests package`

Run benchmarks
- End-to-end JNI vs Java decode benchmarks are wired under the `jni-bench` profile:
  - `mvn -q -f surfingthriftjni/pom.xml -P jni-bench -Darrow.version=12.0.0 -DskipTests verify`
  - This builds native tools, generates datasets, and runs:
    - Small payloads (array mode)
    - Large payloads (mmap ByteBuffer)
    - Large payloads (pooled direct ByteBuffer)
  - Outputs go to `target/bench_*.txt` and a summary is printed at the end.

Running with JNI
- Ensure JVM can find `libsurfingthriftjni.so` (default `java.library.path` in this module points to `../build`).
- Example:

```
java -Djava.library.path=$(pwd)/build \
  -cp surfingthriftjni/target/surfingthriftjni-1.0-SNAPSHOT-jar-with-dependencies.jar \
  com.your.app.Main
```

Notes
- The JNI method that takes a generated Thrift class delegates to the pure Java converter today.
- If `libsurfingthriftjni.so` is not present, only the pure Java converter path will work.
- Package note: this module defines `com.pinterest.drsquirrel.jni.NativeThriftDecoder` and
  `com.pinterest.drsquirrel.thrift.*`, which also exist in `drsquirrel-java`. Do not put both
  jars on the same classpath; prefer one or the other per application.
