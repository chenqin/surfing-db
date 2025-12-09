// Memory management with disk spilling for large Arrow batches
// Automatically splits large batches to disk when memory threshold is exceeded

#ifndef SURFING_MEMORY_MANAGER_H
#define SURFING_MEMORY_MANAGER_H

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <filesystem>
#include <vector>
#include <string>
#include <cstdlib>
#include <memory>
#include <random>

namespace surfing {
namespace memory {

// Memory configuration
struct MemoryConfig {
  // Maximum memory per batch (bytes) - default 512MB
  int64_t max_batch_memory = 512 * 1024 * 1024;

  // Temporary directories for spilled files (can specify multiple)
  std::vector<std::string> temp_dirs = {"/tmp/surfing_spill"};

  // Whether to enable memory spilling
  bool enable_spilling = true;

  // Cleanup temp files on destruction
  bool auto_cleanup = true;

  // Load balancing strategy for multiple directories
  enum class LoadBalancing {
    ROUND_ROBIN,   // Distribute files evenly across directories
    SPACE_AWARE,   // Prefer directories with more free space
    RANDOM         // Random selection
  };
  LoadBalancing load_balancing = LoadBalancing::ROUND_ROBIN;

  // Read from environment variables
  static MemoryConfig from_env() {
    MemoryConfig config;

    const char* max_mem = std::getenv("SURFING_MAX_BATCH_MEMORY");
    if (max_mem) {
      config.max_batch_memory = std::atoll(max_mem);
    }

    // Support both single and multiple temp directories
    const char* temp_dir = std::getenv("SURFING_TEMP_DIR");
    const char* temp_dirs = std::getenv("SURFING_TEMP_DIRS");

    if (temp_dirs) {
      // Parse comma-separated list of directories
      config.temp_dirs.clear();
      std::string dirs_str(temp_dirs);
      size_t start = 0;
      size_t end = dirs_str.find(',');

      while (end != std::string::npos) {
        std::string dir = dirs_str.substr(start, end - start);
        // Trim whitespace
        dir.erase(0, dir.find_first_not_of(" \t"));
        dir.erase(dir.find_last_not_of(" \t") + 1);
        if (!dir.empty()) {
          config.temp_dirs.push_back(dir);
        }
        start = end + 1;
        end = dirs_str.find(',', start);
      }

      // Add last directory
      std::string dir = dirs_str.substr(start);
      dir.erase(0, dir.find_first_not_of(" \t"));
      dir.erase(dir.find_last_not_of(" \t") + 1);
      if (!dir.empty()) {
        config.temp_dirs.push_back(dir);
      }
    } else if (temp_dir) {
      // Single directory (backward compatible)
      config.temp_dirs = {temp_dir};
    }

    const char* enable_spill = std::getenv("SURFING_ENABLE_SPILLING");
    if (enable_spill && std::string(enable_spill) == "0") {
      config.enable_spilling = false;
    }

    const char* load_balance = std::getenv("SURFING_LOAD_BALANCING");
    if (load_balance) {
      std::string lb(load_balance);
      if (lb == "SPACE_AWARE") {
        config.load_balancing = LoadBalancing::SPACE_AWARE;
      } else if (lb == "RANDOM") {
        config.load_balancing = LoadBalancing::RANDOM;
      }
    }

    return config;
  }
};

// Manages spilled batches on disk
class SpillManager {
public:
  explicit SpillManager(const MemoryConfig& config = MemoryConfig::from_env())
      : config_(config), next_file_id_(0), current_dir_index_(0) {
    if (config_.enable_spilling) {
      // Create all temp directories
      for (const auto& dir : config_.temp_dirs) {
        std::filesystem::create_directories(dir);
      }
    }
  }

  ~SpillManager() {
    if (config_.auto_cleanup) {
      cleanup();
    }
  }

  // Check if batch exceeds memory limit
  bool should_spill(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    if (!config_.enable_spilling) return false;
    int64_t batch_size = estimate_memory_usage(batch);
    return batch_size > config_.max_batch_memory;
  }

  // Spill batch to disk, return file path
  arrow::Result<std::string> spill_batch(
      const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::string file_path = generate_spill_path();

    // Write batch to Arrow IPC file
    ARROW_ASSIGN_OR_RAISE(auto outfile,
        arrow::io::FileOutputStream::Open(file_path));

    ARROW_ASSIGN_OR_RAISE(auto writer,
        arrow::ipc::MakeFileWriter(outfile, batch->schema()));

    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());
    ARROW_RETURN_NOT_OK(outfile->Close());

    spilled_files_.push_back(file_path);
    return file_path;
  }

  // Split large batch into smaller chunks and spill
  arrow::Result<std::vector<std::string>> split_and_spill(
      const std::shared_ptr<arrow::RecordBatch>& batch) {
    std::vector<std::string> spilled_paths;

    int64_t total_rows = batch->num_rows();
    int64_t batch_size = estimate_memory_usage(batch);

    if (batch_size <= config_.max_batch_memory) {
      // No need to split
      ARROW_ASSIGN_OR_RAISE(auto path, spill_batch(batch));
      spilled_paths.push_back(path);
      return spilled_paths;
    }

    // Calculate how many chunks we need
    int64_t num_chunks = (batch_size + config_.max_batch_memory - 1) / config_.max_batch_memory;
    int64_t rows_per_chunk = total_rows / num_chunks;

    for (int64_t i = 0; i < num_chunks; ++i) {
      int64_t start_row = i * rows_per_chunk;
      int64_t chunk_rows = (i == num_chunks - 1)
          ? (total_rows - start_row)  // Last chunk gets remainder
          : rows_per_chunk;

      auto chunk = batch->Slice(start_row, chunk_rows);
      ARROW_ASSIGN_OR_RAISE(auto path, spill_batch(chunk));
      spilled_paths.push_back(path);
    }

    return spilled_paths;
  }

  // Read spilled batch from disk
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> read_spilled_batch(
      const std::string& file_path) {
    ARROW_ASSIGN_OR_RAISE(auto infile, arrow::io::ReadableFile::Open(file_path));

    ARROW_ASSIGN_OR_RAISE(auto reader,
        arrow::ipc::RecordBatchFileReader::Open(infile));

    if (reader->num_record_batches() == 0) {
      return arrow::Status::Invalid("Spilled file contains no batches");
    }

    return reader->ReadRecordBatch(0);
  }

  // Read all spilled batches and concatenate
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> read_all_spilled(
      const std::vector<std::string>& file_paths) {
    if (file_paths.empty()) {
      return arrow::Status::Invalid("No spilled files to read");
    }

    if (file_paths.size() == 1) {
      return read_spilled_batch(file_paths[0]);
    }

    // Read all batches
    arrow::RecordBatchVector batches;
    for (const auto& path : file_paths) {
      ARROW_ASSIGN_OR_RAISE(auto batch, read_spilled_batch(path));
      batches.push_back(batch);
    }

    // Concatenate batches into a single RecordBatch
    ARROW_ASSIGN_OR_RAISE(auto table, arrow::Table::FromRecordBatches(batches));
    ARROW_ASSIGN_OR_RAISE(auto combined_batch, table->CombineChunksToBatch());
    return combined_batch;
  }

  // Cleanup all spilled files
  void cleanup() {
    for (const auto& file_path : spilled_files_) {
      std::filesystem::remove(file_path);
    }
    spilled_files_.clear();

    // Try to remove temp directories if empty
    for (const auto& dir : config_.temp_dirs) {
      try {
        std::filesystem::remove(dir);
      } catch (...) {
        // Directory not empty or other error - ignore
      }
    }
  }

  // Get number of spilled files
  size_t num_spilled() const { return spilled_files_.size(); }

  // Get total size of spilled files
  int64_t total_spilled_size() const {
    int64_t total = 0;
    for (const auto& path : spilled_files_) {
      try {
        total += std::filesystem::file_size(path);
      } catch (...) {
        // File may have been deleted
      }
    }
    return total;
  }

private:
  MemoryConfig config_;
  std::vector<std::string> spilled_files_;
  int next_file_id_;
  size_t current_dir_index_;

  // Select directory based on load balancing strategy
  std::string select_directory() {
    if (config_.temp_dirs.empty()) {
      return "/tmp/surfing_spill";  // Fallback
    }

    if (config_.temp_dirs.size() == 1) {
      return config_.temp_dirs[0];
    }

    std::string selected_dir;

    switch (config_.load_balancing) {
      case MemoryConfig::LoadBalancing::ROUND_ROBIN: {
        selected_dir = config_.temp_dirs[current_dir_index_];
        current_dir_index_ = (current_dir_index_ + 1) % config_.temp_dirs.size();
        break;
      }

      case MemoryConfig::LoadBalancing::SPACE_AWARE: {
        // Select directory with most free space
        int64_t max_space = -1;
        for (const auto& dir : config_.temp_dirs) {
          try {
            auto space_info = std::filesystem::space(dir);
            if (space_info.available > max_space) {
              max_space = space_info.available;
              selected_dir = dir;
            }
          } catch (...) {
            // Ignore directories we can't check
          }
        }
        if (selected_dir.empty()) {
          selected_dir = config_.temp_dirs[0];  // Fallback to first
        }
        break;
      }

      case MemoryConfig::LoadBalancing::RANDOM: {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, config_.temp_dirs.size() - 1);
        selected_dir = config_.temp_dirs[dis(gen)];
        break;
      }

      default:
        selected_dir = config_.temp_dirs[0];
    }

    return selected_dir;
  }

  std::string generate_spill_path() {
    std::string dir = select_directory();
    std::ostringstream oss;
    oss << dir << "/surfing_spill_"
        << ::getpid() << "_"
        << next_file_id_++ << ".arrow";
    return oss.str();
  }

  int64_t estimate_memory_usage(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    // Estimate memory usage by summing buffer sizes
    int64_t total = 0;

    for (const auto& array : batch->columns()) {
      auto data = array->data();
      for (const auto& buffer : data->buffers) {
        if (buffer) {
          total += buffer->size();
        }
      }

      // Account for child arrays (nested types)
      for (const auto& child : data->child_data) {
        total += estimate_array_memory(child);
      }
    }

    return total;
  }

  int64_t estimate_array_memory(const std::shared_ptr<arrow::ArrayData>& data) const {
    int64_t total = 0;
    for (const auto& buffer : data->buffers) {
      if (buffer) {
        total += buffer->size();
      }
    }
    for (const auto& child : data->child_data) {
      total += estimate_array_memory(child);
    }
    return total;
  }
};

// RAII wrapper for automatic cleanup
class ScopedSpillManager {
public:
  explicit ScopedSpillManager(const MemoryConfig& config = MemoryConfig::from_env())
      : manager_(std::make_unique<SpillManager>(config)) {}

  SpillManager* operator->() { return manager_.get(); }
  SpillManager& operator*() { return *manager_; }

private:
  std::unique_ptr<SpillManager> manager_;
};

} // namespace memory
} // namespace surfing

#endif // SURFING_MEMORY_MANAGER_H
