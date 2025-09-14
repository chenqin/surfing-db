#include <mpi.h>
#include <vector>
#include <cmath>
#include <cstdio>

#include "cuda/cuda_utils.h"

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);
  int rank = 0, world = 1;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world);

  const int n = 1 << 12; // 4096
  std::vector<float> a(n), b(n), c(n);
  for (int i = 0; i < n; ++i) {
    // vary inputs by rank to ensure each rank does work
    a[i] = static_cast<float>(i + rank) * 0.5f;
    b[i] = static_cast<float>(i + world - rank) * 0.25f;
  }

  int local_status = 0;
  try {
    cuda_vector_add(a.data(), b.data(), c.data(), n);
    for (int i = 0; i < n; ++i) {
      float expected = a[i] + b[i];
      if (std::fabs(expected - c[i]) > 1e-6f) {
        local_status = 1; break;
      }
    }
  } catch (...) {
    local_status = 2;
  }

  int global_status = 0;
  MPI_Allreduce(&local_status, &global_status, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

  if (rank == 0) {
    if (global_status == 0) {
      std::printf("CudaMpiTests: OK on %d ranks\n", world);
    } else {
      std::printf("CudaMpiTests: FAILED on some rank(s)\n");
    }
  }

  MPI_Finalize();
  return global_status;
}

