/*
 * MPI runner to validate shuffle distribution with randomized keys.
 */
#include <mpi.h>
#include <arrow/api.h>
#include <chrono>
#include <iostream>
#include <random>
#include <stdexcept>
#include <vector>

#include "table/processors.h"

using namespace matcha::table;

namespace {
void ThrowIfArrowError(const arrow::Status& st) {
  if (!st.ok()) {
    throw std::runtime_error(std::string("Arrow error: ") + st.ToString());
  }
}
}  // namespace

int main(int argc, char** argv) {
  int provided = 0;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);
  int rank = 0, world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world);

  bool use_two_sided = false;
  if (argc > 1 && std::string(argv[1]) == "two") {
    use_two_sided = true;
  }

  // rows per rank and seed
  const int rows_per_rank = 10000;
  uint64_t seed = static_cast<uint64_t>(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  // allow override via env for deterministic runs
  const char* env_seed = std::getenv("SHUFFLE_TEST_SEED");
  if (env_seed) {
    seed = std::strtoull(env_seed, nullptr, 10);
  }
  std::mt19937_64 gen(seed + rank);
  std::uniform_int_distribution<long long> dist(0, std::numeric_limits<long long>::max());

  arrow::Int64Builder key_builder;
  arrow::Int32Builder val_builder;
  for (int i = 0; i < rows_per_rank; ++i) {
    auto k = static_cast<int64_t>(dist(gen));
    ThrowIfArrowError(key_builder.Append(k));
    ThrowIfArrowError(val_builder.Append(rank * rows_per_rank + i));
  }

  std::shared_ptr<arrow::Array> keys, vals;
  ThrowIfArrowError(key_builder.Finish(&keys));
  ThrowIfArrowError(val_builder.Finish(&vals));
  auto schema = arrow::schema({arrow::field("key", arrow::int64()),
                               arrow::field("val", arrow::int32())});
  auto local = arrow::RecordBatch::Make(schema, keys->length(), {keys, vals});

  auto partitioner = [](size_t key_hash, int r, int w) {
    return key_hash % w;
  };

  auto shuffled = processors::shuffle(local, "key", partitioner, !use_two_sided, rank, world);

  // correctness: check partition rule
  auto key_col = shuffled->GetColumnByName("key");
  for (int64_t i = 0; i < shuffled->num_rows(); ++i) {
    auto s = key_col->GetScalar(i).ValueOrDie();
    if ((s->hash() % world) != static_cast<size_t>(rank)) {
      std::cerr << "Partition mismatch on rank " << rank << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 10);
      return 10;
    }
  }

  // distribution: each rank should have roughly total/world rows
  size_t local_rows = shuffled->num_rows();
  std::vector<size_t> all_rows(world, 0);
  MPI_Allgather(&local_rows, 1, MPI_UNSIGNED_LONG, all_rows.data(), 1, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

  const size_t total_rows = static_cast<size_t>(rows_per_rank) * static_cast<size_t>(world);
  const double expected = static_cast<double>(total_rows) / static_cast<double>(world);
  const double tol = 0.10; // 10% tolerance
  for (int i = 0; i < world; ++i) {
    double cnt = static_cast<double>(all_rows[i]);
    if (cnt < expected * (1.0 - tol) || cnt > expected * (1.0 + tol)) {
      std::cerr << "Distribution out of tolerance on rank " << i
                << ": got=" << cnt << ", expected=" << expected << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 11);
      return 11;
    }
  }

  MPI_Finalize();
  return 0;
}
