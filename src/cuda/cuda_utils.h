// Simple CUDA helpers for tests
#pragma once

#include <cstddef>

// Launches a simple vector addition on GPU: c[i] = a[i] + b[i]
// n = number of elements
void cuda_vector_add(const float* a, const float* b, float* c, int n);

