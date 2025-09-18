/*
 * MPI runner to validate processors::cogroup across ranks.
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

static std::shared_ptr<arrow::RecordBatch> make_left(int rank) {
  arrow::Int64Builder key;
  arrow::Int32Builder la;
  ThrowIfArrowError(key.Append(rank));
  ThrowIfArrowError(la.Append(rank * 10 + 1));
  ThrowIfArrowError(key.Append(rank + 2));
  ThrowIfArrowError(la.Append(rank * 10 + 2));
  std::shared_ptr<arrow::Array> k, a;
  ThrowIfArrowError(key.Finish(&k));
  ThrowIfArrowError(la.Finish(&a));
  auto schema = arrow::schema({arrow::field("key", arrow::int64()),
                               arrow::field("la", arrow::int32())});
  return arrow::RecordBatch::Make(schema, k->length(), {k, a});
}

static std::shared_ptr<arrow::RecordBatch> make_right(int rank) {
  arrow::Int64Builder key;
  arrow::Int32Builder rb;
  ThrowIfArrowError(key.Append(rank));
  ThrowIfArrowError(rb.Append(rank * 100 + 1));
  ThrowIfArrowError(key.Append(rank + 3));
  ThrowIfArrowError(rb.Append(rank * 100 + 2));
  std::shared_ptr<arrow::Array> k, b;
  ThrowIfArrowError(key.Finish(&k));
  ThrowIfArrowError(rb.Finish(&b));
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

  // Global verification: gather and compare left keys
  auto gather_keys = [&](std::shared_ptr<arrow::RecordBatch> b) {
    std::vector<int64_t> local;
    if (b) {
      auto kc = b->GetColumnByName("key");
      for (int64_t i = 0; i < b->num_rows(); ++i) {
        auto s = kc->GetScalar(i).ValueOrDie();
        local.push_back(std::static_pointer_cast<arrow::Int64Scalar>(s)->value);
      }
    }
    int n = static_cast<int>(local.size());
    std::vector<int> counts(world, 0);
    MPI_Gather(&n, 1, MPI_INT, counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    std::vector<int> displs; int total = 0;
    if (rank == 0) { displs.resize(world, 0); for (int i = 0; i < world; ++i) { displs[i] = total; total += counts[i]; } }
    std::vector<int64_t> all; if (rank == 0) all.resize(total);
    MPI_Gatherv(local.data(), n, MPI_LONG_LONG,
                rank == 0 ? all.data() : nullptr,
                rank == 0 ? counts.data() : nullptr,
                rank == 0 ? displs.data() : nullptr,
                MPI_LONG_LONG, 0, MPI_COMM_WORLD);
    return all;
  };

  auto all_L = gather_keys(Lp);
  auto all_R = gather_keys(Rp);
  if (rank == 0) {
    std::vector<int64_t> expL, expR;
    expL.reserve(2 * world); expR.reserve(2 * world);
    for (int r = 0; r < world; ++r) { expL.push_back(r); expL.push_back(r + 2); }
    for (int r = 0; r < world; ++r) { expR.push_back(r); expR.push_back(r + 3); }
    std::sort(expL.begin(), expL.end()); std::sort(expR.begin(), expR.end());
    std::sort(all_L.begin(), all_L.end()); std::sort(all_R.begin(), all_R.end());
    if (expL != all_L) { std::cerr << "Global key set mismatch (left) after cogroup" << std::endl; MPI_Abort(MPI_COMM_WORLD, 5); return 5; }
    if (expR != all_R) { std::cerr << "Global key set mismatch (right) after cogroup" << std::endl; MPI_Abort(MPI_COMM_WORLD, 6); return 6; }
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
