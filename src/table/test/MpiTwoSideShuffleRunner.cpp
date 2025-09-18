/*
 * MPI runner to validate two-sided shuffle across ranks.
 */
#include <mpi.h>
#include <arrow/api.h>
#include <iostream>
#include <algorithm>
#include <stdexcept>

#include "table/processors.h"

using namespace matcha::table;

namespace {
void ThrowIfArrowError(const arrow::Status& st) {
  if (!st.ok()) {
    throw std::runtime_error(std::string("Arrow error: ") + st.ToString());
  }
}
}  // namespace

static std::shared_ptr<arrow::RecordBatch> make_batch_for_rank(int rank, int world) {
  arrow::Int64Builder key_builder;
  arrow::Int32Builder val_builder;
  // Create keys that likely spread across ranks
  for (int i = 0; i < 4; ++i) {
    ThrowIfArrowError(key_builder.Append(static_cast<int64_t>(rank * 100 + i * 7)));
    ThrowIfArrowError(val_builder.Append(rank * 1000 + i));
  }
  std::shared_ptr<arrow::Array> keys, vals;
  ThrowIfArrowError(key_builder.Finish(&keys));
  ThrowIfArrowError(val_builder.Finish(&vals));
  auto schema = arrow::schema({arrow::field("key", arrow::int64()),
                               arrow::field("val", arrow::int32())});
  return arrow::RecordBatch::Make(schema, keys->length(), {keys, vals});
}

int main(int argc, char** argv) {
  int provided = 0;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);
  int rank = 0, world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world);

  auto local = make_batch_for_rank(rank, world);

  auto partitioner = [](size_t key_hash, int r, int w) {
    return key_hash % w;
  };

  // two-sided shuffle: pass false to use the send/recv path
  auto shuffled = processors::shuffle(local, "key", partitioner, false, rank, world);

  auto key_col = shuffled->GetColumnByName("key");
  for (int64_t i = 0; i < shuffled->num_rows(); ++i) {
    auto s = key_col->GetScalar(i).ValueOrDie();
    if ((s->hash() % world) != static_cast<size_t>(rank)) {
      std::cerr << "Two-sided partition validation failed on rank " << rank << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 2);
      return 2;
    }
  }

  size_t local_rows = shuffled->num_rows();
  size_t global_rows = 0;
  MPI_Allreduce(&local_rows, &global_rows, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
  // Expect 4 rows per rank
  if (global_rows != static_cast<size_t>(4 * world)) {
    std::cerr << "Two-sided global row count mismatch: " << global_rows << std::endl;
    MPI_Abort(MPI_COMM_WORLD, 3);
    return 3;
  }

  // Global verification: gather keys and compare to expected set
  std::vector<int64_t> local_keys;
  local_keys.reserve(shuffled->num_rows());
  for (int64_t i = 0; i < shuffled->num_rows(); ++i) {
    auto s = key_col->GetScalar(i).ValueOrDie();
    int64_t k = std::static_pointer_cast<arrow::Int64Scalar>(s)->value;
    local_keys.push_back(k);
  }
  int local_n = static_cast<int>(local_keys.size());
  std::vector<int> counts(world, 0);
  MPI_Gather(&local_n, 1, MPI_INT, counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
  std::vector<int> displs; int total = 0;
  if (rank == 0) { displs.resize(world, 0); for (int i = 0; i < world; ++i) { displs[i] = total; total += counts[i]; } }
  std::vector<int64_t> all_keys; if (rank == 0) all_keys.resize(total);
  MPI_Gatherv(local_keys.data(), local_n, MPI_LONG_LONG,
              rank == 0 ? all_keys.data() : nullptr,
              rank == 0 ? counts.data() : nullptr,
              rank == 0 ? displs.data() : nullptr,
              MPI_LONG_LONG, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    std::vector<int64_t> expected; expected.reserve(4 * world);
    for (int r = 0; r < world; ++r) { for (int i = 0; i < 4; ++i) expected.push_back(static_cast<int64_t>(r) * 100 + i * 7); }
    std::sort(expected.begin(), expected.end()); std::sort(all_keys.begin(), all_keys.end());
    if (expected != all_keys) { std::cerr << "Two-sided global key set mismatch after shuffle" << std::endl; MPI_Abort(MPI_COMM_WORLD, 4); return 4; }
  }

  MPI_Finalize();
  return 0;
}
