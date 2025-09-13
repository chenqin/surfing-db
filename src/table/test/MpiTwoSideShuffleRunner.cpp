/*
 * MPI runner to validate two-sided shuffle across ranks.
 */
#include <mpi.h>
#include <arrow/api.h>
#include <iostream>

#include "table/processors.h"

using namespace matcha::table;

static std::shared_ptr<arrow::RecordBatch> make_batch_for_rank(int rank, int world) {
  arrow::Int64Builder key_builder;
  arrow::Int32Builder val_builder;
  // Create keys that likely spread across ranks
  for (int i = 0; i < 4; ++i) {
    key_builder.Append(static_cast<int64_t>(rank * 100 + i * 7));
    val_builder.Append(rank * 1000 + i);
  }
  std::shared_ptr<arrow::Array> keys, vals;
  key_builder.Finish(&keys);
  val_builder.Finish(&vals);
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

  MPI_Finalize();
  return 0;
}

