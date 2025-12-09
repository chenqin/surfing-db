# Parquet Folder Operations - Quick Start Guide

Get up and running with Parquet folder read/write in 5 minutes.

## Prerequisites

```bash
# C++: Already included if you built Surfing DB
# Arrow and Parquet libraries are required dependencies

# Java: Build the standalone Parquet module
mvn -f surfing-parquet-java/pom.xml package
# Or build all modules:
mvn clean install

# Python: Install PyArrow
pip install pyarrow
```

**Note:** Parquet utilities are now in a standalone `surfing-parquet-java` module.
See [MODULAR_BUILD.md](MODULAR_BUILD.md) for details.

## Quick Demo

Run the comprehensive demo to see all three implementations:

```bash
./scripts/parquet_folder_demo.sh
```

This will:
1. Build sample Parquet files with C++
2. Read them back and display stats
3. Repeat with Java implementation
4. Repeat with Python implementation

## 30-Second Examples

### C++

```cpp
#include "table/utils.h"
using namespace matcha::table;

// Write
arrow::RecordBatchVector batches = /* your data */;
auto result = utils::WriteParquetFolder(batches, "/tmp/output");

// Read
auto read_result = utils::ReadParquetFolder("/tmp/output");
arrow::RecordBatchVector loaded = read_result.ValueOrDie();
```

**Build & Run:**
```bash
cmake --build build --target parquet_folder_tool
./build/parquet_folder_tool write /tmp/demo 3 100
./build/parquet_folder_tool read /tmp/demo
```

### Java

```java
import org.surfing.parquet.*;
import org.apache.arrow.memory.RootAllocator;

try (BufferAllocator allocator = new RootAllocator()) {
    // Write
    List<VectorSchemaRoot> roots = /* your data */;
    ParquetFolderWriter.writeFolder(roots, "/tmp/output");

    // Read
    List<VectorSchemaRoot> loaded =
        ParquetFolderReader.readFolder("/tmp/output", allocator);
}
```

**Build & Run:**
```bash
mvn -f surfing-parquet-java/pom.xml package
java -cp surfing-parquet-java/target/surfing-parquet-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
  org.surfing.parquet.ParquetFolderWriter /tmp/demo
```

### Python

```python
from examples.python.parquet_folder_example import *

# Write
tables = [create_sample_table(100, i) for i in range(3)]
write_parquet_folder(tables, "/tmp/output")

# Read
loaded = read_parquet_folder("/tmp/output")
```

**Run:**
```bash
python examples/python/parquet_folder_example.py \
  --mode roundtrip --output /tmp/demo --num-files 3 --rows-per-file 100
```

## Common Use Cases

### 1. MPI Rank Output

Each MPI rank writes its local partition:

```cpp
// C++: In your MPI program
int rank, world;
MPI_Comm_rank(MPI_COMM_WORLD, &rank);
MPI_Comm_size(MPI_COMM_WORLD, &world);

auto batches = /* process data */;
std::string output = "/shared/data/rank_" + std::to_string(rank);
utils::WriteParquetFolder(batches, output);
```

### 2. Thrift → Arrow → Parquet Pipeline

```java
// Java: Convert Thrift payloads to Parquet
List<byte[]> thriftPayloads = /* ... */;
VectorSchemaRoot root = NativeThriftDecoder.convert(
    allocator, thriftPayloads.toArray(new byte[0][]),
    "schema.thrift", "MyStruct");

ParquetFolderWriter.writeParquetFile(root, "/tmp/output.parquet");
```

### 3. Batch Processing with Python

```python
import pyarrow.parquet as pq
from pathlib import Path

# Process each file in a folder
for file in Path("/data/input").glob("*.parquet"):
    table = pq.read_table(str(file))

    # Transform...
    result = process(table)

    # Write
    output = Path("/data/output") / file.name
    pq.write_table(result, str(output), compression='snappy')
```

## Testing Your Integration

### Unit Tests (C++)

```bash
# Run all Parquet tests
./build/ParquetFolderTests

# Run specific test
./build/ParquetFolderTests --gtest_filter=ParquetFolderTest.WriteAndReadFolder

# Via CTest
ctest -R ParquetFolderTests -V
```

### Quick Validation

```bash
# Write test data
./build/parquet_folder_tool write /tmp/validation 10 1000

# Verify files exist
ls -lh /tmp/validation/

# Read and verify
./build/parquet_folder_tool read /tmp/validation

# Check with Python
python -c "
import pyarrow.parquet as pq
from pathlib import Path
files = list(Path('/tmp/validation').glob('*.parquet'))
print(f'Found {len(files)} files')
for f in files:
    meta = pq.read_metadata(str(f))
    print(f'{f.name}: {meta.num_rows} rows')
"
```

## Troubleshooting

**Build errors?**
```bash
# Ensure Arrow and Parquet are installed
cmake --build build 2>&1 | grep -i "parquet\|arrow"
```

**Java classpath issues?**
```bash
# Verify JAR was built
ls -lh drsquirrel-java-project/target/*.jar
```

**Python import errors?**
```bash
# Install/upgrade PyArrow
pip install --upgrade pyarrow
```

**Empty folder reads?**
```bash
# Check file extensions (must be .parquet)
ls -la /path/to/folder/
```

## Performance Tips

1. **Batch size**: Write 100K-1M rows per file for optimal performance
2. **Compression**: Snappy (default) is fast; use GZIP for better compression
3. **Parallel I/O**: Read/write multiple files concurrently when possible
4. **Memory**: Use streaming for large datasets to avoid OOM

## Next Steps

- Full API docs: [PARQUET_FOLDER_USAGE.md](PARQUET_FOLDER_USAGE.md)
- Implementation details: [PARQUET_FOLDER_CHANGES.md](PARQUET_FOLDER_CHANGES.md)
- Main README: [README.md](README.md)

## Need Help?

Check existing code examples:
- C++: `src/table/test/TestParquetFolder.cpp`
- Java: `ParquetFolderReader.java` and `ParquetFolderWriter.java`
- Python: `examples/python/parquet_folder_example.py`
