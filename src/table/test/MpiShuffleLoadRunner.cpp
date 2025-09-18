/*
 * MPI load test to stress processors::shuffle under large row counts.
 * Allows one-sided or two-sided shuffle and configurable rows/iterations.
 */

#include <mpi.h>
#include <arrow/api.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <memory>
#include <random>
#include <string>
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

static int64_t get_env_ll(const char* name, int64_t def) {
  const char* v = std::getenv(name);
  if (!v) return def;
  char* end = nullptr;
  long long val = std::strtoll(v, &end, 10);
  if (end == v) return def;
  return static_cast<int64_t>(val);
}

static std::string get_env_str(const char* name) {
  const char* v = std::getenv(name);
  return v ? std::string(v) : std::string();
}

static std::shared_ptr<arrow::RecordBatch> make_random_batch(int64_t rows, int rank) {
  std::mt19937_64 gen(static_cast<uint64_t>(rank) ^ 0x9e3779b97f4a7c15ULL);
  std::uniform_int_distribution<long long> dist(0, std::numeric_limits<long long>::max());

  arrow::Int64Builder key_builder;
  arrow::Int32Builder val_builder;
  ThrowIfArrowError(key_builder.Reserve(rows));
  ThrowIfArrowError(val_builder.Reserve(rows));
  for (int64_t i = 0; i < rows; ++i) {
    ThrowIfArrowError(key_builder.Append(static_cast<int64_t>(dist(gen))));
    ThrowIfArrowError(val_builder.Append(static_cast<int32_t>(i)));
  }
  std::shared_ptr<arrow::Array> keys, vals;
  {
    ThrowIfArrowError(key_builder.Finish(&keys));
  }
  {
    ThrowIfArrowError(val_builder.Finish(&vals));
  }

  auto schema = arrow::schema({arrow::field("key", arrow::int64()),
                               arrow::field("val", arrow::int32())});
  return arrow::RecordBatch::Make(schema, rows, {keys, vals});
}

int main(int argc, char** argv) {
  int provided = 0;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_SINGLE, &provided);
  int rank = 0, world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world);

  // Config via args/env:
  // argv[1] = "one" (default) or "two" to choose shuffle path.
  // env SHUFFLE_LOAD_ROWS sets rows per rank (default 200000).
  // env SHUFFLE_LOAD_ITERS sets iterations (default 3).
  bool two_sided = (argc > 1 && std::string(argv[1]) == "two");
  const int64_t rows_per_rank = get_env_ll("SHUFFLE_LOAD_ROWS", 200000);
  const int64_t iters = get_env_ll("SHUFFLE_LOAD_ITERS", 3);
  const std::string out_csv = get_env_str("SHUFFLE_LOAD_OUT");
  const std::string mode_str = two_sided ? "two-sided" : "one-sided";

  // CSV logging from rank 0 if requested
  std::ofstream csv;
  bool csv_enabled = false;
  if (!out_csv.empty() && rank == 0) {
    // append mode
    csv.open(out_csv, std::ios::out | std::ios::app);
    if (csv.good()) {
      // write header marker for visibility per run
      csv << "run_id,ts_epoch,mode,world,rows_per_rank,iter,rows,time_s,rows_per_sec" << '\n';
      csv_enabled = true;
    }
  }

  if (rank == 0) {
    std::cout << "[LoadTest] world=" << world
              << " rows_per_rank=" << rows_per_rank
              << " iters=" << iters
              << " mode=" << mode_str
              << std::endl;
  }

  // Prepare local batch once (keys randomized to spread across ranks)
  auto local = make_random_batch(rows_per_rank, rank);

  auto partitioner = [](size_t key_hash, int r, int w) {
    (void)r; return key_hash % w;
  };

  // warm-up
  {
    auto shuffled = processors::shuffle(local, "key", partitioner, !two_sided, rank, world);
    (void)shuffled;
    MPI_Barrier(MPI_COMM_WORLD);
  }

  double best = 1e300, total = 0.0;
  for (int64_t i = 0; i < iters; ++i) {
    MPI_Barrier(MPI_COMM_WORLD);
    double t0 = MPI_Wtime();
    auto shuffled = processors::shuffle(local, "key", partitioner, !two_sided, rank, world);
    double t1 = MPI_Wtime();

    // Validate partition rule locally
    auto key_col = shuffled->GetColumnByName("key");
    for (int64_t r = 0; r < shuffled->num_rows(); ++r) {
      auto s = key_col->GetScalar(r).ValueOrDie();
      if ((s->hash() % world) != static_cast<size_t>(rank)) {
        std::cerr << "Partition mismatch on rank " << rank << " at iter " << i << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 100);
        return 100;
      }
    }

    double dt = t1 - t0;
    best = std::min(best, dt);
    total += dt;

    // throughput reporting
    size_t local_rows = shuffled->num_rows();
    size_t global_rows = 0;
    MPI_Allreduce(&local_rows, &global_rows, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
    if (rank == 0) {
      double rps = static_cast<double>(global_rows) / dt;
      std::cout << "[LoadTest] iter=" << i
                << " rows=" << global_rows
                << " time_s=" << dt
                << " rows_per_sec=" << rps
                << std::endl;
      if (csv_enabled) {
        auto now = std::chrono::system_clock::now();
        auto epoch = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
        // simple run id = epoch
        csv << epoch << ',' << epoch << ',' << mode_str << ',' << world << ','
            << rows_per_rank << ',' << i << ',' << global_rows << ',' << dt << ',' << rps << '\n';
        csv.flush();
      }
    }
  }

  double avg = total / static_cast<double>(iters);
  if (rank == 0) {
    std::cout << "[LoadTest] best_s=" << best << " avg_s=" << avg << std::endl;
  }

  if (csv_enabled) {
    csv.close();
  }
  MPI_Finalize();
  return 0;
}
