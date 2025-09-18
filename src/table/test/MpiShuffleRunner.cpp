/*
 * Minimal MPI runner to validate processors::shuffle across ranks.
 */
#include <mpi.h>
#include <arrow/api.h>
#include <iostream>
#include <algorithm>
#include <stdexcept>

#include "table/processors.h"
#include "table/utils.h"

using namespace matcha::table;

namespace {
void ThrowIfArrowError(const arrow::Status& st) {
  if (!st.ok()) {
    throw std::runtime_error(std::string("Arrow error: ") + st.ToString());
  }
}
}  // namespace

static std::shared_ptr<arrow::RecordBatch> make_local_batch(int rank) {
  arrow::Int64Builder key_builder;
  arrow::Int32Builder val_builder;
  // two rows per rank
  ThrowIfArrowError(key_builder.Append(rank));
  ThrowIfArrowError(val_builder.Append(rank * 10 + 1));
  ThrowIfArrowError(key_builder.Append(rank + 2));
  ThrowIfArrowError(val_builder.Append(rank * 10 + 2));

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

  auto local = make_local_batch(rank);
  // Combine chunks (no-op here but keeps shape uniform)
  auto batch = local;

  // partitioner uses the hash (as processors::shuffle passes v->hash())
  auto partitioner = [](size_t key_hash, int r, int w) {
    return key_hash % w;
  };

  auto shuffled = processors::shuffle(batch, "key", partitioner, true, rank, world);

  // Validate: all rows received by this rank should satisfy
  // hash(key) % world == this rank
  auto key_col = shuffled->GetColumnByName("key");
  for (int64_t i = 0; i < shuffled->num_rows(); ++i) {
    auto s = key_col->GetScalar(i).ValueOrDie();
    if ((s->hash() % world) != static_cast<size_t>(rank)) {
      std::cerr << "Partition validation failed on rank " << rank << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 1);
      return 1;
    }
  }

  // Also check global row count equals 2 * world
  size_t local_rows = shuffled->num_rows();
  size_t global_rows = 0;
  MPI_Allreduce(&local_rows, &global_rows, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
  if (global_rows != static_cast<size_t>(2 * world)) {
    std::cerr << "Global row count mismatch: " << global_rows << std::endl;
    MPI_Abort(MPI_COMM_WORLD, 1);
    return 1;
  }

  // Global verification: gather all keys and compare to expected set
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
  std::vector<int> displs;
  int total = 0;
  if (rank == 0) {
    displs.resize(world, 0);
    for (int i = 0; i < world; ++i) { displs[i] = total; total += counts[i]; }
  }
  std::vector<int64_t> all_keys;
  if (rank == 0) all_keys.resize(total);
  MPI_Gatherv(local_keys.data(), local_n, MPI_LONG_LONG,
              rank == 0 ? all_keys.data() : nullptr,
              rank == 0 ? counts.data() : nullptr,
              rank == 0 ? displs.data() : nullptr,
              MPI_LONG_LONG, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    std::vector<int64_t> expected;
    expected.reserve(2 * world);
    for (int r = 0; r < world; ++r) { expected.push_back(r); expected.push_back(r + 2); }
    std::sort(expected.begin(), expected.end());
    std::sort(all_keys.begin(), all_keys.end());
    if (expected != all_keys) { std::cerr << "Global key set mismatch after shuffle" << std::endl; MPI_Abort(MPI_COMM_WORLD, 2); return 2; }
  }

  MPI_Finalize();
  return 0;
}
