/*
 * CUDA kernel for computing moving average on float arrays
 *
 * This kernel processes multiple float arrays in parallel on GPU,
 * computing sliding window averages efficiently using shared memory.
 */

#include <cuda_runtime.h>
#include <stdio.h>

// CUDA kernel for moving average computation
__global__ void movingAverageKernel(
    const float* __restrict__ input,
    float* __restrict__ output,
    int numArrays,
    int arraySize,
    int windowSize
) {
    // Global thread index
    int arrayIdx = blockIdx.x * blockDim.x + threadIdx.x;

    if (arrayIdx >= numArrays) return;

    // Calculate offset for this array
    int offset = arrayIdx * arraySize;
    const float* inArray = input + offset;
    float* outArray = output + offset;

    // Compute moving average for each element in the array
    for (int i = 0; i < arraySize; i++) {
        int start = max(0, i - windowSize + 1);
        int count = i - start + 1;

        float sum = 0.0f;
        for (int j = start; j <= i; j++) {
            sum += inArray[j];
        }
        outArray[i] = sum / count;
    }
}

// Optimized kernel using shared memory for better performance
__global__ void movingAverageKernelOptimized(
    const float* __restrict__ input,
    float* __restrict__ output,
    int numArrays,
    int arraySize,
    int windowSize
) {
    extern __shared__ float sharedData[];

    int arrayIdx = blockIdx.x;
    if (arrayIdx >= numArrays) return;

    int tid = threadIdx.x;
    int offset = arrayIdx * arraySize;

    // Load data into shared memory
    for (int i = tid; i < arraySize; i += blockDim.x) {
        sharedData[i] = input[offset + i];
    }
    __syncthreads();

    // Compute moving average
    for (int i = tid; i < arraySize; i += blockDim.x) {
        int start = max(0, i - windowSize + 1);
        int count = i - start + 1;

        float sum = 0.0f;
        #pragma unroll 4
        for (int j = start; j <= i; j++) {
            sum += sharedData[j];
        }
        output[offset + i] = sum / count;
    }
}

extern "C" {

// Host function to launch kernel
void launchMovingAverageKernel(
    const float* d_input,
    float* d_output,
    int numArrays,
    int arraySize,
    int windowSize,
    cudaStream_t stream
) {
    // Simple kernel configuration
    int threadsPerBlock = 256;
    int blocksPerGrid = (numArrays + threadsPerBlock - 1) / threadsPerBlock;

    movingAverageKernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(
        d_input, d_output, numArrays, arraySize, windowSize
    );
}

// Host function to launch optimized kernel
void launchMovingAverageKernelOptimized(
    const float* d_input,
    float* d_output,
    int numArrays,
    int arraySize,
    int windowSize,
    cudaStream_t stream
) {
    int threadsPerBlock = 256;
    int sharedMemSize = arraySize * sizeof(float);

    // Limit blocks based on shared memory availability
    if (sharedMemSize < 48 * 1024) { // 48KB shared memory per SM
        movingAverageKernelOptimized<<<numArrays, threadsPerBlock, sharedMemSize, stream>>>(
            d_input, d_output, numArrays, arraySize, windowSize
        );
    } else {
        // Fall back to simple kernel for large arrays
        int blocksPerGrid = (numArrays + threadsPerBlock - 1) / threadsPerBlock;
        movingAverageKernel<<<blocksPerGrid, threadsPerBlock, 0, stream>>>(
            d_input, d_output, numArrays, arraySize, windowSize
        );
    }
}

// Complete GPU moving average computation
int gpuMovingAverage(
    const float* h_input,
    float* h_output,
    int numArrays,
    int arraySize,
    int windowSize,
    bool useOptimized
) {
    size_t totalSize = (size_t)numArrays * arraySize;
    size_t bytes = totalSize * sizeof(float);

    float *d_input = nullptr, *d_output = nullptr;
    cudaError_t err;

    // Allocate device memory
    err = cudaMalloc(&d_input, bytes);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMalloc failed for input: %s\n", cudaGetErrorString(err));
        return -1;
    }

    err = cudaMalloc(&d_output, bytes);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMalloc failed for output: %s\n", cudaGetErrorString(err));
        cudaFree(d_input);
        return -1;
    }

    // Copy input to device
    err = cudaMemcpy(d_input, h_input, bytes, cudaMemcpyHostToDevice);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMemcpy H2D failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_input);
        cudaFree(d_output);
        return -1;
    }

    // Launch kernel
    if (useOptimized) {
        launchMovingAverageKernelOptimized(d_input, d_output, numArrays, arraySize, windowSize, 0);
    } else {
        launchMovingAverageKernel(d_input, d_output, numArrays, arraySize, windowSize, 0);
    }

    // Check for kernel errors
    err = cudaGetLastError();
    if (err != cudaSuccess) {
        fprintf(stderr, "Kernel launch failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_input);
        cudaFree(d_output);
        return -1;
    }

    // Wait for kernel to complete
    err = cudaDeviceSynchronize();
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaDeviceSynchronize failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_input);
        cudaFree(d_output);
        return -1;
    }

    // Copy result back to host
    err = cudaMemcpy(h_output, d_output, bytes, cudaMemcpyDeviceToHost);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMemcpy D2H failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_input);
        cudaFree(d_output);
        return -1;
    }

    // Cleanup
    cudaFree(d_input);
    cudaFree(d_output);

    return 0;
}

} // extern "C"
