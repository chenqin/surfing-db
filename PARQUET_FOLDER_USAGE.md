# Parquet Folder Read/Write Support

This document describes the Parquet folder read/write functionality added to Surfing DB, allowing you to efficiently read and write collections of Parquet files across C++, Java, and Python.

## Overview

The Parquet folder utilities provide:
- **Bulk read**: Load all Parquet files from a directory into Arrow RecordBatches/Tables
- **Bulk write**: Write multiple RecordBatches/Tables to separate Parquet files
- **Single file operations**: Read/write individual Parquet files
- **Cross-language support**: Consistent API across C++, Java, and Python

## C++ API

### Include
```cpp
#include "table/utils.h"
using namespace matcha::table;
```

### Reading a Parquet Folder
```cpp
// Read all .parquet files from a directory
auto result = utils::ReadParquetFolder("/path/to/parquet/folder");
if (result.ok()) {
    arrow::RecordBatchVector batches = result.ValueOrDie();
    LOG(INFO) << "Read " << batches.size() << " batches";
}
```

### Writing to a Parquet Folder
```cpp
arrow::RecordBatchVector batches = /* your data */;

// Write each batch to a separate file (part_00000.parquet, part_00001.parquet, ...)
auto write_result = utils::WriteParquetFolder(batches, "/path/to/output");
if (write_result.ok()) {
    std::vector<std::string> files = write_result.ValueOrDie();
    LOG(INFO) << "Wrote " << files.size() << " Parquet files";
}

// Custom prefix
auto custom_result = utils::WriteParquetFolder(batches, "/path/to/output", "custom");
// Creates: custom_00000.parquet, custom_00001.parquet, ...
```

### Single File Operations
```cpp
// Read single file
auto read_result = utils::ReadParquetFile("/path/to/file.parquet");
arrow::RecordBatchVector batches = read_result.ValueOrDie();

// Write single file
std::shared_ptr<arrow::RecordBatch> batch = /* your data */;
auto status = utils::WriteParquetFile(batch, "/path/to/output.parquet");
```

### Building and Testing
```bash
# Build with Parquet support
cmake --build build --target ParquetFolderTests

# Run tests
./build/ParquetFolderTests
# Or via CTest
ctest -R ParquetFolderTests
```

## Java API

### Maven Dependency
The utilities are now in a standalone `surfing-parquet-java` module with minimal dependencies.

```xml
<dependency>
  <groupId>org.surfing</groupId>
  <artifactId>surfing-parquet-java</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

**Build:**
```bash
mvn -f surfing-parquet-java/pom.xml package
# Or build all modules:
mvn clean install
```

### Reading a Parquet Folder
```java
import org.surfing.parquet.ParquetFolderReader;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

try (BufferAllocator allocator = new RootAllocator()) {
    List<VectorSchemaRoot> roots = ParquetFolderReader.readFolder(
        "/path/to/parquet/folder",
        allocator
    );

    System.out.println("Read " + roots.size() + " Parquet files");

    // Process data...

    // Clean up
    for (VectorSchemaRoot root : roots) {
        root.close();
    }
}
```

### Writing to a Parquet Folder
```java
import org.surfing.parquet.ParquetFolderWriter;

List<VectorSchemaRoot> roots = /* your data */;

List<String> files = ParquetFolderWriter.writeFolder(
    roots,
    "/path/to/output",
    "part"  // optional prefix
);

System.out.println("Wrote " + files.size() + " files");
```

### Single File Operations
```java
import org.apache.hadoop.conf.Configuration;

Configuration conf = new Configuration();

// Read single file
VectorSchemaRoot root = ParquetFolderReader.readParquetFile(
    "/path/to/file.parquet",
    allocator,
    conf
);

// Write single file
ParquetFolderWriter.writeParquetFile(root, "/path/to/output.parquet");
```

### Counting Rows
```java
long totalRows = ParquetFolderReader.countRows("/path/to/parquet/folder");
System.out.println("Total rows: " + totalRows);
```

### Running the Example
```bash
# Read a folder
java -cp surfing-parquet-java/target/surfing-parquet-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
  org.surfing.parquet.ParquetFolderReader /path/to/folder

# Write sample data
java -cp surfing-parquet-java/target/surfing-parquet-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
  org.surfing.parquet.ParquetFolderWriter /path/to/output
```

## Python API

### Installation
```bash
pip install pyarrow
```

### Reading a Parquet Folder
```python
from examples.python.parquet_folder_example import read_parquet_folder
import pyarrow as pa

# Read all .parquet files from a directory
tables = read_parquet_folder("/path/to/parquet/folder")
print(f"Read {len(tables)} tables")

# Combine into single table
if tables:
    combined = pa.concat_tables(tables)
    print(f"Total rows: {combined.num_rows}")
```

### Writing to a Parquet Folder
```python
from examples.python.parquet_folder_example import write_parquet_folder

tables = [...]  # Your PyArrow Tables

written_files = write_parquet_folder(
    tables,
    "/path/to/output",
    prefix="part"  # optional
)
print(f"Wrote {len(written_files)} files")
```

### Running the Example
```bash
# Full roundtrip (write then read)
python examples/python/parquet_folder_example.py \
  --mode roundtrip \
  --output /tmp/parquet_test \
  --num-files 5 \
  --rows-per-file 1000

# Write only
python examples/python/parquet_folder_example.py \
  --mode write \
  --output /tmp/parquet_output \
  --num-files 10 \
  --rows-per-file 5000

# Read only
python examples/python/parquet_folder_example.py \
  --mode read \
  --input /tmp/parquet_output
```

### Direct PyArrow Usage
```python
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

# Read all Parquet files
folder = Path("/path/to/parquet")
tables = [pq.read_table(str(f)) for f in folder.glob("*.parquet")]

# Write tables
for i, table in enumerate(tables):
    pq.write_table(
        table,
        f"/path/to/output/part_{i:05d}.parquet",
        compression='snappy'
    )
```

## Features

### Compression
All implementations use **Snappy compression** by default for optimal balance of speed and compression ratio.

### File Naming
- Default prefix: `part`
- Format: `{prefix}_{index:05d}.parquet`
- Example: `part_00000.parquet`, `part_00001.parquet`, etc.

### Schema Consistency
All files in a folder should have compatible schemas for proper reading.

### Performance Tips

1. **Batch Size**: For large datasets, write multiple smaller files rather than one huge file
   ```cpp
   // C++: Split large batches
   int64_t max_rows_per_file = 1000000;
   utils::WriteParquetFolder(batches, output_dir, "data");
   ```

2. **Parallel Reading**: Use multiple threads to read files in parallel
   ```python
   # Python: Concurrent reads
   from concurrent.futures import ThreadPoolExecutor

   def read_file(path):
       return pq.read_table(str(path))

   with ThreadPoolExecutor() as executor:
       tables = list(executor.map(read_file, parquet_files))
   ```

3. **Memory Management**: Close/free resources when done
   ```java
   // Java: Always close VectorSchemaRoots
   try (VectorSchemaRoot root = ...) {
       // Use root
   }  // Automatically closed
   ```

## Integration with MPI Workflows

Use Parquet folder operations with MPI shuffle/cogroup:

```cpp
// C++: Each rank writes its partition
auto batches = /* process data */;
std::string output_dir = "/shared/data/rank_" + std::to_string(rank);
utils::WriteParquetFolder(batches, output_dir);

MPI_Barrier(MPI_COMM_WORLD);

// Rank 0 aggregates
if (rank == 0) {
    for (int r = 0; r < world; ++r) {
        std::string dir = "/shared/data/rank_" + std::to_string(r);
        auto result = utils::ReadParquetFolder(dir);
        // Aggregate...
    }
}
```

## Troubleshooting

### Error: "Path does not exist or is not a directory"
Ensure the folder exists and is readable. Create it first:
```cpp
std::filesystem::create_directories("/path/to/folder");
```

### Error: "Empty or null RecordBatch"
Check that your batches contain data before writing:
```cpp
if (batch && batch->num_rows() > 0) {
    utils::WriteParquetFile(batch, path);
}
```

### Schema Mismatch
When reading multiple files, ensure schemas are compatible:
```python
# Check schemas before concatenating
schemas = [t.schema for t in tables]
assert all(s.equals(schemas[0]) for s in schemas), "Schema mismatch!"
```

## See Also
- [Arrow Parquet Documentation](https://arrow.apache.org/docs/python/parquet.html)
- [Existing ThriftToParquet tool](drsquirrel-java-project/src/main/java/com/pinterest/drsquirrel/tools/ThriftToParquet.java)
- [MPI Shuffle/Cogroup examples](README.md#2-mpi-shuffle--cogroup)
