/*
 * Minimal MPI runner to validate processors::shuffle across ranks.
 */
#include <mpi.h>
#include <arrow/api.h>
#include <iostream>

#include "table/processors.h"
#include "table/utils.h"

using namespace matcha::table;

static std::shared_ptr<arrow::RecordBatch> make_local_batch(int rank) {
  arrow::Int64Builder key_builder;
  arrow::Int32Builder val_builder;
  // two rows per rank
  key_builder.Append(rank);
  val_builder.Append(rank * 10 + 1);
  key_builder.Append(rank + 2);
  val_builder.Append(rank * 10 + 2);

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

  MPI_Finalize();
  return 0;
}

