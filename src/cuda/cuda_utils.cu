#include "cuda_utils.h"

#include <cuda_runtime.h>
#include <stdexcept>
#include <string>

namespace {

inline void checkCuda(cudaError_t err, const char* what) {
  if (err != cudaSuccess) {
    throw std::runtime_error(std::string(what) + ": " + cudaGetErrorString(err));
  }
}

__global__ void vecAddKernel(const float* a, const float* b, float* c, int n) {
  int i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i < n) {
    c[i] = a[i] + b[i];
  }
}

} // namespace

void cuda_vector_add(const float* a, const float* b, float* c, int n) {
  if (n <= 0) return;

  float *d_a = nullptr, *d_b = nullptr, *d_c = nullptr;
  size_t bytes = static_cast<size_t>(n) * sizeof(float);

  checkCuda(cudaMalloc(&d_a, bytes), "cudaMalloc d_a");
  checkCuda(cudaMalloc(&d_b, bytes), "cudaMalloc d_b");
  checkCuda(cudaMalloc(&d_c, bytes), "cudaMalloc d_c");

  checkCuda(cudaMemcpy(d_a, a, bytes, cudaMemcpyHostToDevice), "cudaMemcpy H2D a");
  checkCuda(cudaMemcpy(d_b, b, bytes, cudaMemcpyHostToDevice), "cudaMemcpy H2D b");

  int threads = 256;
  int blocks = (n + threads - 1) / threads;
  vecAddKernel<<<blocks, threads>>>(d_a, d_b, d_c, n);
  checkCuda(cudaGetLastError(), "kernel launch");
  checkCuda(cudaDeviceSynchronize(), "kernel sync");

  checkCuda(cudaMemcpy(c, d_c, bytes, cudaMemcpyDeviceToHost), "cudaMemcpy D2H c");

  cudaFree(d_a);
  cudaFree(d_b);
  cudaFree(d_c);
}

