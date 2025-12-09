// JNI implementation for Parquet folder read/write operations
// Similar pattern to thrift_decode_jni.cpp

#include <jni.h>
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <filesystem>
#include <vector>
#include <string>
#include <sstream>
#include <iomanip>
#include "simd_utils.h"      // SIMD optimizations
#include "memory_manager.h"  // Memory management with spilling

namespace {

// Read all Parquet files from a directory into Arrow RecordBatch
// Uses memory-aware spilling for large datasets
arrow::Result<std::shared_ptr<arrow::RecordBatch>> ReadParquetFolderInternal(
    const std::string& folder_path) {
  namespace fs = std::filesystem;
  arrow::RecordBatchVector batches;

  if (!fs::exists(folder_path) || !fs::is_directory(folder_path)) {
    return arrow::Status::Invalid("Path does not exist or is not a directory: " + folder_path);
  }

  std::vector<fs::path> parquet_files;
  for (const auto& entry : fs::directory_iterator(folder_path)) {
    if (!entry.is_regular_file()) continue;

    std::string path_str = entry.path().string();
    if (path_str.size() >= 8 && path_str.substr(path_str.size() - 8) == ".parquet") {
      parquet_files.push_back(entry.path());
    }
  }

  // Sort files for deterministic ordering
  std::sort(parquet_files.begin(), parquet_files.end());

  // Initialize memory manager for spilling
  surfing::memory::SpillManager spill_manager;
  std::vector<std::string> spilled_paths;
  bool used_spilling = false;

  for (const auto& file_path : parquet_files) {
    ARROW_ASSIGN_OR_RAISE(auto infile, arrow::io::ReadableFile::Open(file_path.string()));
    std::unique_ptr<parquet::arrow::FileReader> reader;

    // Use optimized reader properties for SIMD-friendly data access
    parquet::ReaderProperties reader_props = parquet::default_reader_properties();
#ifdef SURFING_AVX2
    // Enable buffered stream for better SIMD utilization
    reader_props.enable_buffered_stream();
    reader_props.set_buffer_size(256 * 1024);  // 256KB buffer for vectorized reads
#endif

    ARROW_RETURN_NOT_OK(
        parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

    // Convert table to record batches
    arrow::TableBatchReader batch_reader(*table);
    std::shared_ptr<arrow::RecordBatch> batch;
    while (true) {
      ARROW_RETURN_NOT_OK(batch_reader.ReadNext(&batch));
      if (batch == nullptr) break;

      // Check if batch should be spilled to disk
      if (spill_manager.should_spill(batch)) {
        ARROW_ASSIGN_OR_RAISE(auto paths, spill_manager.split_and_spill(batch));
        spilled_paths.insert(spilled_paths.end(), paths.begin(), paths.end());
        used_spilling = true;
      } else {
        batches.push_back(batch);
      }
    }
  }

  if (batches.empty() && spilled_paths.empty()) {
    return arrow::Status::Invalid("No Parquet files found or all files are empty");
  }

  // If we used spilling, we need to combine in-memory and on-disk batches
  if (used_spilling) {
    // Spill any remaining in-memory batches
    for (const auto& batch : batches) {
      ARROW_ASSIGN_OR_RAISE(auto path, spill_manager.spill_batch(batch));
      spilled_paths.push_back(path);
    }
    batches.clear();

    // Read all spilled batches back
    ARROW_ASSIGN_OR_RAISE(auto combined_batch,
        spill_manager.read_all_spilled(spilled_paths));

    return combined_batch;
  }

  // No spilling needed - combine in memory
  ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches(batches));
  ARROW_ASSIGN_OR_RAISE(auto combined_batch, table->CombineChunksToBatch());
  return combined_batch;
}

// Write Arrow RecordBatch to Parquet folder (multiple files)
arrow::Status WriteParquetFolderInternal(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::string& folder_path,
    const std::string& prefix,
    int num_files) {
  namespace fs = std::filesystem;

  if (!batch || batch->num_rows() == 0) {
    return arrow::Status::Invalid("Empty or null RecordBatch");
  }

  fs::create_directories(folder_path);

  // Calculate rows per file
  int64_t total_rows = batch->num_rows();
  int64_t rows_per_file = (total_rows + num_files - 1) / num_files;

  for (int i = 0; i < num_files; ++i) {
    int64_t start_row = i * rows_per_file;
    if (start_row >= total_rows) break;

    int64_t end_row = std::min(start_row + rows_per_file, total_rows);
    int64_t length = end_row - start_row;

    // Slice the batch
    auto sliced_batch = batch->Slice(start_row, length);

    // Generate filename
    std::ostringstream oss;
    oss << prefix << "_" << std::setfill('0') << std::setw(5) << i << ".parquet";
    fs::path file_path = fs::path(folder_path) / oss.str();

    // Write to file
    ARROW_ASSIGN_OR_RAISE(auto outfile,
        arrow::io::FileOutputStream::Open(file_path.string()));

    std::shared_ptr<arrow::Table> table =
        arrow::Table::FromRecordBatches({sliced_batch}).ValueOrDie();

    parquet::WriterProperties::Builder props_builder;
    props_builder.compression(parquet::Compression::SNAPPY);
    auto props = props_builder.build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder().build();

    ARROW_RETURN_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                   outfile, sliced_batch->num_rows(), props, arrow_props));

    ARROW_RETURN_NOT_OK(outfile->Close());
  }

  return arrow::Status::OK();
}

} // namespace

// JNI: Read Parquet folder
extern "C" JNIEXPORT void JNICALL
Java_com_pinterest_drsquirrel_jni_NativeParquetIO_readFolder(
    JNIEnv* env, jclass,
    jstring jfolderPath,
    jlong schema_out_addr, jlong array_out_addr) {

  const char* cpath = env->GetStringUTFChars(jfolderPath, nullptr);
  std::string folder_path(cpath ? cpath : "");
  if (cpath) env->ReleaseStringUTFChars(jfolderPath, cpath);

  auto result = ReadParquetFolderInternal(folder_path);
  if (!result.ok()) {
    // Throw Java exception
    jclass exceptionClass = env->FindClass("java/io/IOException");
    env->ThrowNew(exceptionClass, result.status().ToString().c_str());
    return;
  }

  auto batch = result.ValueOrDie();
  auto* schema_out = reinterpret_cast<ArrowSchema*>(schema_out_addr);
  auto* array_out = reinterpret_cast<ArrowArray*>(array_out_addr);

  arrow::ExportSchema(*batch->schema().get(), schema_out);
  arrow::ExportRecordBatch(*batch.get(), array_out, schema_out);
}

// JNI: Write Parquet folder
extern "C" JNIEXPORT void JNICALL
Java_com_pinterest_drsquirrel_jni_NativeParquetIO_writeFolder(
    JNIEnv* env, jclass,
    jlong schema_addr, jlong array_addr,
    jstring jfolderPath, jstring jprefix, jint numFiles) {

  const char* cpath = env->GetStringUTFChars(jfolderPath, nullptr);
  const char* cprefix = env->GetStringUTFChars(jprefix, nullptr);
  std::string folder_path(cpath ? cpath : "");
  std::string prefix(cprefix ? cprefix : "part");
  if (cpath) env->ReleaseStringUTFChars(jfolderPath, cpath);
  if (cprefix) env->ReleaseStringUTFChars(jprefix, cprefix);

  // Import Arrow data from C Data Interface
  auto* schema_ptr = reinterpret_cast<ArrowSchema*>(schema_addr);
  auto* array_ptr = reinterpret_cast<ArrowArray*>(array_addr);

  auto import_result = arrow::ImportRecordBatch(array_ptr, schema_ptr);
  if (!import_result.ok()) {
    jclass exceptionClass = env->FindClass("java/io/IOException");
    env->ThrowNew(exceptionClass, import_result.status().ToString().c_str());
    return;
  }

  auto batch = import_result.ValueOrDie();

  auto status = WriteParquetFolderInternal(batch, folder_path, prefix, numFiles);
  if (!status.ok()) {
    jclass exceptionClass = env->FindClass("java/io/IOException");
    env->ThrowNew(exceptionClass, status.ToString().c_str());
    return;
  }
}
