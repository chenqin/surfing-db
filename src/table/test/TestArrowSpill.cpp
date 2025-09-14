/*
 * Tests for spilling Arrow RecordBatches to disk and loading them back
 */
#include <gtest/gtest.h>
#include <filesystem>
#include <random>

#include "table/utils.h"

using namespace matcha::table;

TEST(ArrowSpillTest, SpillAndLoadIpcChunks) {
  namespace fs = std::filesystem;

  // Build a simple RecordBatch with 7 rows (to force 3 chunks with max_rows=3)
  auto schema = arrow::schema({arrow::field("key", arrow::int64()),
                               arrow::field("val", arrow::utf8())});

  arrow::Int64Builder kb;
  arrow::StringBuilder vb;
  for (int64_t i = 0; i < 7; ++i) {
    ASSERT_TRUE(kb.Append(i).ok());
    ASSERT_TRUE(vb.Append(std::to_string(i)).ok());
  }
  std::shared_ptr<arrow::Array> ka, va;
  ASSERT_TRUE(kb.Finish(&ka).ok());
  ASSERT_TRUE(vb.Finish(&va).ok());
  auto rb = arrow::RecordBatch::Make(schema, 7, {ka, va});

  // Temp directory
  fs::path tmp = fs::temp_directory_path() / fs::path("matcha_spill_test");
  // Ensure clean
  fs::create_directories(tmp);

  // Spill with chunk size 3
  auto maybe_paths = utils::SpillRecordBatchToIpcFiles(rb, tmp.string(), 3);
  ASSERT_TRUE(maybe_paths.ok());
  auto paths = maybe_paths.ValueOrDie();
  // Expect 3 files: 3,3,1
  ASSERT_EQ(paths.size(), 3u);

  // Load chunks back
  auto maybe_batches = utils::LoadRecordBatchesFromIpcFiles(paths);
  ASSERT_TRUE(maybe_batches.ok());
  auto chunks = maybe_batches.ValueOrDie();
  ASSERT_EQ(chunks.size(), 3u);
  int64_t total = 0;
  for (auto& c : chunks) total += c->num_rows();
  ASSERT_EQ(total, rb->num_rows());

  // Verify concatenated keys are in order and match original
  int64_t idx = 0;
  for (auto& c : chunks) {
    auto col = c->GetColumnByName("key");
    for (int64_t i = 0; i < c->num_rows(); ++i) {
      auto s = col->GetScalar(i).ValueOrDie();
      int64_t v = std::static_pointer_cast<arrow::Int64Scalar>(s)->value;
      ASSERT_EQ(v, idx++);
    }
  }

  // Cleanup spill files
  for (auto& p : paths) {
    std::error_code ec; fs::remove(p, ec);
  }
  std::error_code ec; fs::remove(tmp, ec);
}

