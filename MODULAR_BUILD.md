# Modular Build Structure

Surfing DB has been refactored into modular, independently releasable Java projects for cleaner dependency management.

## Module Structure

```
surfing-db/
├── surfingthriftjni/          # Unified JNI module (Thrift + Parquet)
│   ├── Native C++ library (libsurfingthriftjni.so)
│   ├── Java API for Thrift→Arrow decoding
│   └── Java API for Parquet I/O (JNI + Java implementations)
│
└── drsquirrel-java-project/   # MPI/Flink workflows
    └── Depends on surfingthriftjni
```

**Note:** `surfing-parquet-java` has been merged into `surfingthriftjni`. See [PARQUET_THRIFT_JNI_MERGE.md](PARQUET_THRIFT_JNI_MERGE.md) for details.

## Modules

### 1. surfingthriftjni (Unified JNI Module)
**Purpose:** Native Thrift→Arrow conversion + Parquet I/O with JNI
**Group ID:** `org.surfing`
**Artifact ID:** `surfingthriftjni`
**Dependencies:** Arrow, Parquet, Thrift, Hadoop, JNI

**Features:**
- Thrift→Arrow decoding (native JNI)
- Parquet folder read/write (native JNI)
- Java Parquet utilities (pure Java fallback)

**Build:**
```bash
mvn -f surfingthriftjni/pom.xml clean package
```

**Artifacts:**
- `surfingthriftjni-1.0-SNAPSHOT.jar` - Main JAR with JNI + Java APIs
- `libsurfingthriftjni.so` - Native library (in `target/nativebuild/`)

**APIs:**
```java
// Thrift decoding (JNI)
import org.surfing.drsquirrel.jni.NativeThriftDecoder;

// Parquet I/O (JNI - high performance)
import org.surfing.drsquirrel.jni.NativeParquetIO;

// Parquet I/O (Java - compatibility)
import org.surfing.parquet.ParquetFolderReader;
import org.surfing.parquet.ParquetFolderWriter;
```

---

### 2. drsquirrel-java-project (DEPRECATED INFO - surfing-parquet-java merged)
~~**Purpose:** Standalone Parquet folder operations~~
~~**Group ID:** `org.surfing`~~
~~**Artifact ID:** `surfing-parquet-java`~~

**This module has been merged into `surfingthriftjni`.**
- `surfing-parquet-java-1.0-SNAPSHOT.jar`
- `surfing-parquet-java-1.0-SNAPSHOT-jar-with-dependencies.jar`
- `surfing-parquet-java-1.0-SNAPSHOT-sources.jar`
- `surfing-parquet-java-1.0-SNAPSHOT-javadoc.jar`

**Usage:**
```java
import org.surfing.parquet.*;

// Read Parquet folder
List<VectorSchemaRoot> roots = ParquetFolderReader.readFolder(
    "/path/to/folder", allocator);

// Write Parquet folder
ParquetFolderWriter.writeFolder(roots, "/output/path");
```

---

### 3. drsquirrel-java-project
**Purpose:** MPI/Flink workflows and benchmarks
**Group ID:** `org.surfing.drsquirrel`
**Artifact ID:** `drsquirrel-java`
**Dependencies:**
- `surfingthriftjni` (required)
- `surfing-parquet-java` (optional)
- Flink, Spark, Kafka, etc.

**Build:**
```bash
mvn -f drsquirrel-java-project/pom.xml clean package
```

**Note:** Parquet support is now optional. To use Parquet features, ensure `surfing-parquet-java` is on classpath.

---

## Building All Modules

### Option 1: Multi-Module Build (Recommended)
Build all modules in dependency order:
```bash
mvn clean install
```

This builds:
1. `surfingthriftjni`
2. `surfing-parquet-java`
3. `drsquirrel-java-project`

### Option 2: Individual Module Builds
Build specific modules:
```bash
# Just Thrift JNI
mvn -f surfingthriftjni/pom.xml clean package

# Just Parquet utilities
mvn -f surfing-parquet-java/pom.xml clean package

# Just DrSquirrel (requires others built first)
mvn -f drsquirrel-java-project/pom.xml clean package
```

### Option 3: Skip Specific Modules
Skip modules you don't need:
```bash
# Build without Parquet module
mvn clean install -pl '!surfing-parquet-java'

# Build only Parquet module
mvn clean install -pl surfing-parquet-java -am
```

---

## Dependency Graph

```
surfingthriftjni (standalone)
       ↓
surfing-parquet-java (standalone)
       ↓ (optional)
drsquirrel-java-project
```

- **surfingthriftjni:** No dependencies on other modules
- **surfing-parquet-java:** No dependencies on other modules
- **drsquirrel-java-project:** Depends on `surfingthriftjni`, optionally `surfing-parquet-java`

---

## Release Strategy

Each module can be released independently:

### Release surfingthriftjni
```bash
mvn -f surfingthriftjni/pom.xml versions:set -DnewVersion=1.0.0
mvn -f surfingthriftjni/pom.xml clean deploy
```

### Release surfing-parquet-java
```bash
mvn -f surfing-parquet-java/pom.xml versions:set -DnewVersion=1.0.0
mvn -f surfing-parquet-java/pom.xml clean deploy
```

### Release drsquirrel-java
```bash
mvn -f drsquirrel-java-project/pom.xml versions:set -DnewVersion=1.0.0
mvn -f drsquirrel-java-project/pom.xml clean deploy
```

---

## Migration Guide

### For Users of Old Parquet Classes

**Old way (deprecated):**
```java
import org.surfing.drsquirrel.arrow.ParquetFolderReader;
import org.surfing.drsquirrel.arrow.ParquetFolderWriter;
```

**New way:**
```java
import org.surfing.parquet.ParquetFolderReader;
import org.surfing.parquet.ParquetFolderWriter;
```

**Maven dependency:**
```xml
<dependency>
  <groupId>org.surfing</groupId>
  <artifactId>surfing-parquet-java</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

---

## Benefits of Modular Structure

1. **Smaller Dependencies:** Use only what you need
   - Need Parquet only? → `surfing-parquet-java` (no MPI/Flink deps)
   - Need Thrift conversion? → `surfingthriftjni` (no Parquet deps)

2. **Independent Releases:** Update modules separately
   - Fix Parquet bug? → Release `surfing-parquet-java` 1.0.1
   - No need to rebuild/release other modules

3. **Cleaner Builds:** Faster compilation
   - `surfing-parquet-java`: ~10 seconds
   - `surfingthriftjni`: ~30 seconds (native build)
   - `drsquirrel-java`: ~45 seconds

4. **Better Reusability:** Use Parquet utils in other projects
   - Drop in `surfing-parquet-java` JAR
   - No unwanted Flink/Spark/Kafka dependencies

---

## CI/CD Integration

### GitHub Actions Example
```yaml
jobs:
  build-thrift:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build surfingthriftjni
        run: mvn -f surfingthriftjni/pom.xml clean package

  build-parquet:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build surfing-parquet-java
        run: mvn -f surfing-parquet-java/pom.xml clean package

  build-drsquirrel:
    needs: [build-thrift, build-parquet]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build drsquirrel-java
        run: mvn -f drsquirrel-java-project/pom.xml clean package
```

---

## Troubleshooting

**Issue:** `surfing-parquet-java` not found when building `drsquirrel-java`
**Solution:** Build parent aggregator first: `mvn clean install`

**Issue:** Old Parquet classes not found in `org.surfing.drsquirrel.arrow`
**Solution:** Update imports to `org.surfing.parquet`

**Issue:** Want Parquet without all drsquirrel dependencies
**Solution:** Use `surfing-parquet-java` standalone JAR directly

---

## See Also
- [PARQUET_QUICKSTART.md](PARQUET_QUICKSTART.md) - Parquet usage guide
- [README.md](README.md) - Main documentation
- [pom.xml](pom.xml) - Parent aggregator POM
