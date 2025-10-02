/*
 * Benchmark for CUDA-accelerated moving average computation
 * Compares CPU vs GPU performance on large datasets
 */

#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <cmath>
#include <iomanip>

// CUDA kernel interface
extern "C" {
    int gpuMovingAverage(
        const float* h_input,
        float* h_output,
        int numArrays,
        int arraySize,
        int windowSize,
        bool useOptimized
    );
}

class Timer {
    using Clock = std::chrono::high_resolution_clock;
    Clock::time_point start_time;
public:
    Timer() : start_time(Clock::now()) {}

    double elapsed() const {
        auto end = Clock::now();
        return std::chrono::duration<double>(end - start_time).count();
    }

    void reset() {
        start_time = Clock::now();
    }
};

// CPU implementation for comparison
void cpuMovingAverage(
    const float* input,
    float* output,
    int numArrays,
    int arraySize,
    int windowSize
) {
    for (int arr = 0; arr < numArrays; arr++) {
        int offset = arr * arraySize;
        const float* inArray = input + offset;
        float* outArray = output + offset;

        for (int i = 0; i < arraySize; i++) {
            int start = std::max(0, i - windowSize + 1);
            int count = i - start + 1;

            float sum = 0.0f;
            for (int j = start; j <= i; j++) {
                sum += inArray[j];
            }
            outArray[i] = sum / count;
        }
    }
}

// Generate random test data
void generateRandomData(float* data, int totalSize, unsigned int seed) {
    std::mt19937 gen(seed);
    std::uniform_real_distribution<float> dist(0.0f, 100.0f);

    for (int i = 0; i < totalSize; i++) {
        data[i] = dist(gen);
    }
}

// Verify results match
bool verifyResults(
    const float* cpuOutput,
    const float* gpuOutput,
    int totalSize,
    float tolerance = 1e-4f
) {
    int errors = 0;
    int maxErrors = 10;

    for (int i = 0; i < totalSize; i++) {
        float diff = std::abs(cpuOutput[i] - gpuOutput[i]);
        if (diff > tolerance) {
            if (errors < maxErrors) {
                std::cerr << "Mismatch at index " << i
                          << ": CPU=" << cpuOutput[i]
                          << ", GPU=" << gpuOutput[i]
                          << ", diff=" << diff << std::endl;
            }
            errors++;
        }
    }

    if (errors > 0) {
        std::cerr << "Total errors: " << errors << " / " << totalSize << std::endl;
        return false;
    }
    return true;
}

// Print benchmark results
void printResults(
    int numArrays,
    int arraySize,
    int windowSize,
    double cpuTime,
    double gpuTime,
    double gpuOptTime,
    bool verified
) {
    long long totalElements = (long long)numArrays * arraySize;
    double memoryMB = totalElements * sizeof(float) / (1024.0 * 1024.0);

    std::cout << "\n";
    std::cout << "================================================================\n";
    std::cout << "GPU Moving Average Benchmark Results\n";
    std::cout << "================================================================\n";
    std::cout << "Dataset:\n";
    std::cout << "  Arrays:          " << std::setw(12) << numArrays << "\n";
    std::cout << "  Array size:      " << std::setw(12) << arraySize << " floats\n";
    std::cout << "  Window size:     " << std::setw(12) << windowSize << "\n";
    std::cout << "  Total elements:  " << std::setw(12) << totalElements << "\n";
    std::cout << "  Memory:          " << std::fixed << std::setprecision(2)
              << std::setw(12) << memoryMB << " MB\n";
    std::cout << "\n";

    std::cout << "Performance:\n";
    std::cout << "  CPU time:        " << std::fixed << std::setprecision(3)
              << std::setw(12) << cpuTime << " sec\n";
    std::cout << "  GPU time:        " << std::fixed << std::setprecision(3)
              << std::setw(12) << gpuTime << " sec\n";
    std::cout << "  GPU optimized:   " << std::fixed << std::setprecision(3)
              << std::setw(12) << gpuOptTime << " sec\n";
    std::cout << "\n";

    std::cout << "Speedup:\n";
    std::cout << "  GPU vs CPU:      " << std::fixed << std::setprecision(2)
              << std::setw(12) << cpuTime / gpuTime << "x\n";
    std::cout << "  GPU-Opt vs CPU:  " << std::fixed << std::setprecision(2)
              << std::setw(12) << cpuTime / gpuOptTime << "x\n";
    std::cout << "\n";

    std::cout << "Throughput:\n";
    double cpuThroughput = totalElements / cpuTime / 1e6;
    double gpuThroughput = totalElements / gpuTime / 1e6;
    double gpuOptThroughput = totalElements / gpuOptTime / 1e6;

    std::cout << "  CPU:             " << std::fixed << std::setprecision(2)
              << std::setw(12) << cpuThroughput << " M elements/sec\n";
    std::cout << "  GPU:             " << std::fixed << std::setprecision(2)
              << std::setw(12) << gpuThroughput << " M elements/sec\n";
    std::cout << "  GPU optimized:   " << std::fixed << std::setprecision(2)
              << std::setw(12) << gpuOptThroughput << " M elements/sec\n";
    std::cout << "\n";

    std::cout << "Verification:      " << (verified ? "PASSED ✓" : "FAILED ✗") << "\n";
    std::cout << "================================================================\n";
}

int main(int argc, char** argv) {
    // Parse command-line arguments
    int numArrays = argc > 1 ? std::atoi(argv[1]) : 1000000;
    int arraySize = argc > 2 ? std::atoi(argv[2]) : 100;
    int windowSize = argc > 3 ? std::atoi(argv[3]) : 10;

    std::cout << "================================================================\n";
    std::cout << "CUDA Moving Average Benchmark\n";
    std::cout << "================================================================\n";
    std::cout << "Initializing...\n";

    // Allocate memory
    size_t totalSize = (size_t)numArrays * arraySize;
    std::vector<float> input(totalSize);
    std::vector<float> cpuOutput(totalSize);
    std::vector<float> gpuOutput(totalSize);
    std::vector<float> gpuOptOutput(totalSize);

    // Generate test data
    std::cout << "Generating " << totalSize << " random floats...\n";
    Timer timer;
    generateRandomData(input.data(), totalSize, 42);
    std::cout << "Data generation: " << timer.elapsed() << " sec\n";

    // CPU benchmark
    std::cout << "\nRunning CPU benchmark...\n";
    timer.reset();
    cpuMovingAverage(input.data(), cpuOutput.data(), numArrays, arraySize, windowSize);
    double cpuTime = timer.elapsed();
    std::cout << "CPU time: " << cpuTime << " sec\n";

    // GPU benchmark (simple kernel)
    std::cout << "\nRunning GPU benchmark (simple kernel)...\n";
    timer.reset();
    int result = gpuMovingAverage(
        input.data(), gpuOutput.data(),
        numArrays, arraySize, windowSize, false
    );
    double gpuTime = timer.elapsed();

    if (result != 0) {
        std::cerr << "GPU computation failed!\n";
        return 1;
    }
    std::cout << "GPU time: " << gpuTime << " sec\n";

    // GPU benchmark (optimized kernel)
    std::cout << "\nRunning GPU benchmark (optimized kernel)...\n";
    timer.reset();
    result = gpuMovingAverage(
        input.data(), gpuOptOutput.data(),
        numArrays, arraySize, windowSize, true
    );
    double gpuOptTime = timer.elapsed();

    if (result != 0) {
        std::cerr << "GPU optimized computation failed!\n";
        return 1;
    }
    std::cout << "GPU optimized time: " << gpuOptTime << " sec\n";

    // Verify results
    std::cout << "\nVerifying results...\n";
    bool verified = verifyResults(cpuOutput.data(), gpuOutput.data(), totalSize);
    if (verified) {
        verified = verifyResults(cpuOutput.data(), gpuOptOutput.data(), totalSize);
    }

    // Print results
    printResults(numArrays, arraySize, windowSize, cpuTime, gpuTime, gpuOptTime, verified);

    return verified ? 0 : 1;
}
