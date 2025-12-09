/*
 * Test program for Parquet folder read/write utilities
 */

#include <gtest/gtest.h>
#include <arrow/testing/gtest_util.h>
#include <filesystem>
#include "../utils.h"

using namespace matcha::table;

class ParquetFolderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_folder_ = "/tmp/surfingdb_parquet_test";
    std::filesystem::remove_all(test_folder_);
    std::filesystem::create_directories(test_folder_);
  }

  void TearDown() override {
    std::filesystem::remove_all(test_folder_);
  }

  arrow::RecordBatchVector CreateSampleBatches(int num_batches, int rows_per_batch) {
    arrow::RecordBatchVector batches;

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::float64()),
        arrow::field("name", arrow::utf8())
    });

    for (int batch_idx = 0; batch_idx < num_batches; ++batch_idx) {
      arrow::Int64Builder id_builder;
      arrow::DoubleBuilder value_builder;
      arrow::StringBuilder name_builder;

      for (int row = 0; row < rows_per_batch; ++row) {
        int64_t id = batch_idx * rows_per_batch + row;
        ARROW_EXPECT_OK(id_builder.Append(id));
        ARROW_EXPECT_OK(value_builder.Append(static_cast<double>(id) * 1.5));
        ARROW_EXPECT_OK(name_builder.Append("row_" + std::to_string(id)));
      }

      std::shared_ptr<arrow::Array> id_array, value_array, name_array;
      ARROW_EXPECT_OK(id_builder.Finish(&id_array));
      ARROW_EXPECT_OK(value_builder.Finish(&value_array));
      ARROW_EXPECT_OK(name_builder.Finish(&name_array));

      auto batch = arrow::RecordBatch::Make(
          schema, rows_per_batch, {id_array, value_array, name_array});
      batches.push_back(batch);
    }

    return batches;
  }

  std::string test_folder_;
};

TEST_F(ParquetFolderTest, WriteAndReadFolder) {
  // Create sample data
  int num_batches = 3;
  int rows_per_batch = 100;
  auto batches = CreateSampleBatches(num_batches, rows_per_batch);

  // Write to folder
  auto write_result = utils::WriteParquetFolder(batches, test_folder_);
  ASSERT_TRUE(write_result.ok());
  auto written_files = write_result.ValueOrDie();
  EXPECT_EQ(written_files.size(), num_batches);

  // Verify files exist
  for (const auto& file : written_files) {
    EXPECT_TRUE(std::filesystem::exists(file));
  }

  // Read back from folder
  auto read_result = utils::ReadParquetFolder(test_folder_);
  ASSERT_TRUE(read_result.ok());
  auto read_batches = read_result.ValueOrDie();

  // Verify we got the same number of batches
  EXPECT_EQ(read_batches.size(), num_batches);

  // Verify total row count
  int total_rows = 0;
  for (const auto& batch : read_batches) {
    total_rows += batch->num_rows();
  }
  EXPECT_EQ(total_rows, num_batches * rows_per_batch);
}

TEST_F(ParquetFolderTest, WriteSingleFile) {
  auto batches = CreateSampleBatches(1, 50);
  std::string file_path = test_folder_ + "/single.parquet";

  auto status = utils::WriteParquetFile(batches[0], file_path);
  ASSERT_TRUE(status.ok());
  EXPECT_TRUE(std::filesystem::exists(file_path));

  // Read it back
  auto read_result = utils::ReadParquetFile(file_path);
  ASSERT_TRUE(read_result.ok());
  auto read_batches = read_result.ValueOrDie();
  EXPECT_GT(read_batches.size(), 0);

  int total_rows = 0;
  for (const auto& batch : read_batches) {
    total_rows += batch->num_rows();
  }
  EXPECT_EQ(total_rows, 50);
}

TEST_F(ParquetFolderTest, EmptyFolder) {
  // Try to read from empty folder
  auto read_result = utils::ReadParquetFolder(test_folder_);
  ASSERT_TRUE(read_result.ok());
  auto batches = read_result.ValueOrDie();
  EXPECT_EQ(batches.size(), 0);
}

TEST_F(ParquetFolderTest, NonExistentFolder) {
  auto read_result = utils::ReadParquetFolder("/nonexistent/folder");
  EXPECT_FALSE(read_result.ok());
}

TEST_F(ParquetFolderTest, CustomPrefix) {
  auto batches = CreateSampleBatches(2, 10);

  auto write_result = utils::WriteParquetFolder(batches, test_folder_, "custom");
  ASSERT_TRUE(write_result.ok());
  auto written_files = write_result.ValueOrDie();

  for (const auto& file : written_files) {
    std::string filename = std::filesystem::path(file).filename().string();
    EXPECT_TRUE(filename.find("custom_") == 0);
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
