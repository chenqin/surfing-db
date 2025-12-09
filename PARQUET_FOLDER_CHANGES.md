# Parquet Folder Support - Summary of Changes

This document summarizes the additions made to support Parquet folder read/write operations.

## Files Added

### C++ Implementation
1. **src/table/utils.h** (modified)
   - Added `ReadParquetFolder()` - Read all .parquet files from directory
   - Added `WriteParquetFolder()` - Write RecordBatchVector to separate files
   - Added `ReadParquetFile()` - Read single Parquet file
   - Added `WriteParquetFile()` - Write single RecordBatch to file
   - Added necessary headers: `parquet/arrow/reader.h`, `parquet/arrow/writer.h`, `<iomanip>`

2. **src/table/test/TestParquetFolder.cpp** (new)
   - Comprehensive GTest suite for Parquet folder operations
   - Tests: write/read roundtrip, single file, empty folder, custom prefix, error handling

3. **src/table/tools/parquet_folder_tool.cpp** (new)
   - Standalone CLI tool for Parquet operations
   - Commands: `read <folder>`, `write <folder> [num_files] [rows_per_file]`
   - Demonstrates usage and provides debugging utility

### Java Implementation
4. **drsquirrel-java-project/src/main/java/com/pinterest/drsquirrel/arrow/ParquetFolderReader.java** (new)
   - `readFolder()` - Read all Parquet files from directory
   - `readParquetFile()` - Read single file
   - `countRows()` - Count total rows in folder
   - Includes runnable main() for CLI usage

5. **drsquirrel-java-project/src/main/java/com/pinterest/drsquirrel/arrow/ParquetFolderWriter.java** (new)
   - `writeFolder()` - Write VectorSchemaRoots to directory
   - `writeParquetFile()` - Write single file
   - Configurable compression (Snappy default)
   - Includes sample data generator in main()

### Python Implementation
6. **examples/python/parquet_folder_example.py** (new)
   - `read_parquet_folder()` - Read all Parquet files
   - `write_parquet_folder()` - Write PyArrow Tables
   - `count_total_rows()` - Count rows across files
   - `create_sample_table()` - Generate test data
   - Full CLI with modes: write, read, roundtrip

### Build System
7. **CMakeLists.txt** (modified)
   - Added `ParquetFolderTests` target (lines 388-401)
   - Added `parquet_folder_tool` target (lines 419-426)
   - Linked against surfingdb_core, Arrow, Parquet, GLog

### Scripts
8. **scripts/parquet_folder_demo.sh** (new)
   - Unified demo script for all three languages
   - Tests C++, Java, and Python implementations
   - Creates sample data and validates roundtrip

### Documentation
9. **PARQUET_FOLDER_USAGE.md** (new)
   - Comprehensive usage guide for all three languages
   - API reference with code examples
   - Integration patterns with MPI workflows
   - Performance tips and troubleshooting

10. **README.md** (modified)
    - Added Parquet folder operations to "Tools & Integrations"
    - Added `parquet_folder_tool` to build targets list

## API Summary

### C++ API
```cpp
// Read folder
auto result = utils::ReadParquetFolder("/path/to/folder");
arrow::RecordBatchVector batches = result.ValueOrDie();

// Write folder
utils::WriteParquetFolder(batches, "/path/to/output", "prefix");

// Single file operations
utils::ReadParquetFile("/path/to/file.parquet");
utils::WriteParquetFile(batch, "/path/to/file.parquet");
```

### Java API
```java
// Read folder
List<VectorSchemaRoot> roots = ParquetFolderReader.readFolder(
    "/path/to/folder", allocator);

// Write folder
List<String> files = ParquetFolderWriter.writeFolder(
    roots, "/path/to/output", "prefix");

// Count rows
long totalRows = ParquetFolderReader.countRows("/path/to/folder");
```

### Python API
```python
# Read folder
tables = read_parquet_folder("/path/to/folder")

# Write folder
files = write_parquet_folder(tables, "/path/to/output", "prefix")

# Count rows
total = count_total_rows("/path/to/folder")
```

## Key Features

- **Multi-language support**: Consistent API across C++, Java, and Python
- **Snappy compression**: Default compression for all implementations
- **Flexible naming**: Customizable file prefix with zero-padded indices
- **Error handling**: Proper validation and error reporting
- **Test coverage**: GTest suite for C++, runnable examples for Java/Python
- **CLI tools**: Standalone utilities for testing and debugging

## Building and Testing

```bash
# Build C++ components
cmake --build build --target parquet_folder_tool
cmake --build build --target ParquetFolderTests

# Run C++ tests
./build/ParquetFolderTests
# Or via CTest
ctest -R ParquetFolderTests

# Build Java components
mvn -q -f drsquirrel-java-project/pom.xml package

# Run comprehensive demo
./scripts/parquet_folder_demo.sh
```

## Usage Examples

### C++ CLI Tool
```bash
# Write 5 files with 1000 rows each
./build/parquet_folder_tool write /tmp/test 5 1000

# Read all files
./build/parquet_folder_tool read /tmp/test
```

### Java CLI
```bash
JAR=drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar

# Write sample files
java -cp $JAR org.surfing.drsquirrel.arrow.ParquetFolderWriter /tmp/test

# Read files
java -cp $JAR org.surfing.drsquirrel.arrow.ParquetFolderReader /tmp/test
```

### Python CLI
```bash
# Roundtrip test
python examples/python/parquet_folder_example.py \
  --mode roundtrip --output /tmp/test --num-files 3 --rows-per-file 1000

# Write only
python examples/python/parquet_folder_example.py \
  --mode write --output /tmp/test --num-files 10 --rows-per-file 5000

# Read only
python examples/python/parquet_folder_example.py \
  --mode read --input /tmp/test
```

## Integration Points

The Parquet folder utilities integrate with:
- **MPI workflows**: Each rank can write its partition to a separate folder
- **Thrift conversion**: Convert Thrift → Arrow → Parquet in batch
- **Shuffle/Cogroup**: Persist intermediate results to Parquet
- **Benchmark harness**: Store test datasets for reproducible benchmarks

## Next Steps

To use the Parquet folder functionality:
1. Read [PARQUET_FOLDER_USAGE.md](PARQUET_FOLDER_USAGE.md) for detailed API docs
2. Run `./scripts/parquet_folder_demo.sh` to see it in action
3. Build the test suite: `cmake --build build --target ParquetFolderTests`
4. Integrate into your workflow following the examples
