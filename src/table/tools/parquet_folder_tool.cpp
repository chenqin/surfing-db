/*
 * Standalone tool for Parquet folder read/write operations
 *
 * Usage:
 *   parquet_folder_tool read <folder_path>
 *   parquet_folder_tool write <folder_path> [num_files] [rows_per_file]
 */

#include <iostream>
#include <filesystem>
#include "../utils.h"

using namespace matcha::table;

void print_usage() {
    std::cout << "Parquet Folder Tool\n\n";
    std::cout << "Usage:\n";
    std::cout << "  parquet_folder_tool read <folder_path>\n";
    std::cout << "  parquet_folder_tool write <folder_path> [num_files] [rows_per_file]\n";
    std::cout << "\nExamples:\n";
    std::cout << "  parquet_folder_tool read /tmp/parquet_data\n";
    std::cout << "  parquet_folder_tool write /tmp/parquet_output 5 1000\n";
}

arrow::RecordBatchVector create_sample_batches(int num_batches, int rows_per_batch) {
    arrow::RecordBatchVector batches;

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::float64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("active", arrow::boolean())
    });

    for (int batch_idx = 0; batch_idx < num_batches; ++batch_idx) {
        arrow::Int64Builder id_builder;
        arrow::DoubleBuilder value_builder;
        arrow::StringBuilder name_builder;
        arrow::BooleanBuilder active_builder;

        for (int row = 0; row < rows_per_batch; ++row) {
            int64_t id = batch_idx * rows_per_batch + row;
            CHECK(id_builder.Append(id).ok());
            CHECK(value_builder.Append(static_cast<double>(id) * 3.14).ok());
            CHECK(name_builder.Append("item_" + std::to_string(id)).ok());
            CHECK(active_builder.Append(id % 2 == 0).ok());
        }

        std::shared_ptr<arrow::Array> id_array, value_array, name_array, active_array;
        CHECK(id_builder.Finish(&id_array).ok());
        CHECK(value_builder.Finish(&value_array).ok());
        CHECK(name_builder.Finish(&name_array).ok());
        CHECK(active_builder.Finish(&active_array).ok());

        auto batch = arrow::RecordBatch::Make(
            schema, rows_per_batch, {id_array, value_array, name_array, active_array});
        batches.push_back(batch);
    }

    return batches;
}

int cmd_read(const std::string& folder_path) {
    std::cout << "Reading Parquet files from: " << folder_path << "\n";

    auto result = utils::ReadParquetFolder(folder_path);
    if (!result.ok()) {
        std::cerr << "Error reading folder: " << result.status().ToString() << "\n";
        return 1;
    }

    auto batches = result.ValueOrDie();
    std::cout << "Read " << batches.size() << " file(s)\n\n";

    int64_t total_rows = 0;
    for (size_t i = 0; i < batches.size(); ++i) {
        const auto& batch = batches[i];
        total_rows += batch->num_rows();
        std::cout << "Batch " << i << ": "
                  << batch->num_rows() << " rows, "
                  << batch->num_columns() << " columns\n";
    }

    std::cout << "\nTotal rows: " << total_rows << "\n";

    if (!batches.empty()) {
        std::cout << "\nSchema:\n" << batches[0]->schema()->ToString() << "\n";

        // Show first few rows from first batch
        if (batches[0]->num_rows() > 0) {
            std::cout << "\nFirst batch preview (up to 5 rows):\n";
            auto preview = batches[0]->Slice(0, std::min<int64_t>(5, batches[0]->num_rows()));
            std::cout << preview->ToString() << "\n";
        }
    }

    return 0;
}

int cmd_write(const std::string& folder_path, int num_files, int rows_per_file) {
    std::cout << "Writing " << num_files << " Parquet file(s) to: " << folder_path << "\n";
    std::cout << "Rows per file: " << rows_per_file << "\n";

    // Create sample data
    auto batches = create_sample_batches(num_files, rows_per_file);

    auto result = utils::WriteParquetFolder(batches, folder_path);
    if (!result.ok()) {
        std::cerr << "Error writing folder: " << result.status().ToString() << "\n";
        return 1;
    }

    auto written_files = result.ValueOrDie();
    std::cout << "\nWrote " << written_files.size() << " file(s):\n";
    for (const auto& file : written_files) {
        auto file_size = std::filesystem::file_size(file);
        std::cout << "  - " << std::filesystem::path(file).filename().string()
                  << " (" << file_size << " bytes)\n";
    }

    int64_t total_rows = num_files * rows_per_file;
    std::cout << "\nTotal rows written: " << total_rows << "\n";

    return 0;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        print_usage();
        return 1;
    }

    std::string command = argv[1];
    std::string folder_path = argv[2];

    try {
        if (command == "read") {
            return cmd_read(folder_path);
        } else if (command == "write") {
            int num_files = argc > 3 ? std::atoi(argv[3]) : 3;
            int rows_per_file = argc > 4 ? std::atoi(argv[4]) : 1000;
            return cmd_write(folder_path, num_files, rows_per_file);
        } else {
            std::cerr << "Unknown command: " << command << "\n\n";
            print_usage();
            return 1;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
