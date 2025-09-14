#include <vector>
#include <cmath>
// Simple self-checking CUDA test without gtest.
// Returns 0 on success, non-zero on failure.

#include "cuda/cuda_utils.h"

int main() {
  const int n = 1 << 12; // 4096
  std::vector<float> a(n), b(n), c(n);
  for (int i = 0; i < n; ++i) {
    a[i] = static_cast<float>(i) * 0.5f;
    b[i] = static_cast<float>(i) * 0.25f;
  }
  cuda_vector_add(a.data(), b.data(), c.data(), n);
  for (int i = 0; i < n; ++i) {
    float expected = a[i] + b[i];
    if (std::fabs(expected - c[i]) > 1e-6f) {
      return 1;
    }
  }
  return 0;
}
