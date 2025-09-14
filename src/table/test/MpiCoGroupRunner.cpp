/*
 * MPI runner to validate processors::cogroup across ranks.
 */
#include <mpi.h>
#include <arrow/api.h>
#include <iostream>

#include "table/processors.h"

using namespace matcha::table;

static std::shared_ptr<arrow::RecordBatch> make_left(int rank) {
  arrow::Int64Builder key;
  arrow::Int32Builder la;
  key.Append(rank);
  la.Append(rank * 10 + 1);
  key.Append(rank + 2);
  la.Append(rank * 10 + 2);
  std::shared_ptr<arrow::Array> k, a;
  key.Finish(&k);
  la.Finish(&a);
  auto schema = arrow::schema({arrow::field("key", arrow::int64()),
                               arrow::field("la", arrow::int32())});
  return arrow::RecordBatch::Make(schema, k->length(), {k, a});
}

static std::shared_ptr<arrow::RecordBatch> make_right(int rank) {
  arrow::Int64Builder key;
  arrow::Int32Builder rb;
  key.Append(rank);
  rb.Append(rank * 100 + 1);
  key.Append(rank + 3);
  rb.Append(rank * 100 + 2);
  std::shared_ptr<arrow::Array> k, b;
  key.Finish(&k);
  rb.Finish(&b);
  auto schema = arrow::schema({arrow::field("key", arrow::int64()),
                               arrow::field("rb", arrow::int32())});
  return arrow::RecordBatch::Make(schema, k->length(), {k, b});
}

int main(int argc, char** argv) {
  int provided = 0;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);
  int rank = 0, world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world);

  bool singleside = true;
  if (argc > 1 && std::string(argv[1]) == std::string("two")) singleside = false;

  auto L = make_left(rank);
  auto R = make_right(rank);

  auto partitioner = [](size_t key_hash, int r, int w) { return key_hash % w; };

  auto out = processors::cogroup({L}, {R}, "key", partitioner, singleside, rank, world);
  auto Lp = out.first;
  auto Rp = out.second;

  // Validate both outputs follow the partition rule
  for (auto batch : {Lp, Rp}) {
    if (!batch) continue;
    auto key_col = batch->GetColumnByName("key");
    for (int64_t i = 0; i < batch->num_rows(); ++i) {
      auto s = key_col->GetScalar(i).ValueOrDie();
      if ((s->hash() % world) != static_cast<size_t>(rank)) {
        std::cerr << "Partition validation failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
      }
    }
  }

  // Check global row counts equal input totals per side
  size_t local_rows_L = Lp ? Lp->num_rows() : 0;
  size_t local_rows_R = Rp ? Rp->num_rows() : 0;
  size_t global_rows_L = 0, global_rows_R = 0;
  MPI_Allreduce(&local_rows_L, &global_rows_L, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
  MPI_Allreduce(&local_rows_R, &global_rows_R, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
  // Expect 2 rows per rank on each side
  if (global_rows_L != static_cast<size_t>(2 * world) ||
      global_rows_R != static_cast<size_t>(2 * world)) {
    std::cerr << "Global row count mismatch: L=" << global_rows_L
              << " R=" << global_rows_R << std::endl;
    MPI_Abort(MPI_COMM_WORLD, 2);
    return 2;
  }

  // Check schemas contain all expected fields on each side
  if (Lp) {
    if (Lp->num_columns() != 2 ||
        !Lp->schema()->GetFieldByName("key") ||
        !Lp->schema()->GetFieldByName("la")) {
      std::cerr << "Left partition schema missing expected fields on rank " << rank << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 3);
      return 3;
    }
  }
  if (Rp) {
    if (Rp->num_columns() != 2 ||
        !Rp->schema()->GetFieldByName("key") ||
        !Rp->schema()->GetFieldByName("rb")) {
      std::cerr << "Right partition schema missing expected fields on rank " << rank << std::endl;
      MPI_Abort(MPI_COMM_WORLD, 4);
      return 4;
    }
  }

  MPI_Finalize();
  return 0;
}
