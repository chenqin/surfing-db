package org.surfing.drsquirrel.cuda;

import java.util.Random;

/**
 * Standalone CUDA-accelerated moving average computation.
 * This is a demonstration class that would use CUDA JNI for GPU acceleration.
 *
 * For actual GPU acceleration, this would interface with a native CUDA kernel.
 * The Spark RAPIDS version (RapidsMovingAverage.scala) provides production-ready
 * GPU acceleration through the RAPIDS framework.
 */
public class CudaMovingAverage {

    /**
     * Compute moving average on CPU (fallback implementation)
     */
    public static float[][] computeMovingAverage(float[][] arrays, int windowSize) {
        long startTime = System.nanoTime();

        int numArrays = arrays.length;
        float[][] result = new float[numArrays][];

        for (int i = 0; i < numArrays; i++) {
            result[i] = computeSingleArrayMovingAverage(arrays[i], windowSize);
        }

        long elapsed = System.nanoTime() - startTime;
        System.out.printf("CPU computation time: %.3f seconds%n", elapsed / 1e9);

        return result;
    }

    private static float[] computeSingleArrayMovingAverage(float[] array, int windowSize) {
        int n = array.length;
        float[] result = new float[n];

        for (int i = 0; i < n; i++) {
            int start = Math.max(0, i - windowSize + 1);
            int count = i - start + 1;

            float sum = 0.0f;
            for (int j = start; j <= i; j++) {
                sum += array[j];
            }
            result[i] = sum / count;
        }

        return result;
    }

    /**
     * Generate random float arrays for testing
     */
    public static float[][] generateRandomArrays(int numArrays, int arraySize, long seed) {
        Random rand = new Random(seed);
        float[][] arrays = new float[numArrays][arraySize];

        for (int i = 0; i < numArrays; i++) {
            for (int j = 0; j < arraySize; j++) {
                arrays[i][j] = rand.nextFloat() * 100.0f;
            }
        }

        return arrays;
    }

    /**
     * Verify moving average computation correctness
     */
    public static boolean verify(float[][] input, float[][] output, int windowSize) {
        for (int i = 0; i < input.length; i++) {
            for (int j = 0; j < input[i].length; j++) {
                int start = Math.max(0, j - windowSize + 1);
                int count = j - start + 1;

                float expectedSum = 0.0f;
                for (int k = start; k <= j; k++) {
                    expectedSum += input[i][k];
                }
                float expected = expectedSum / count;

                if (Math.abs(output[i][j] - expected) > 1e-5) {
                    System.err.printf("Mismatch at [%d][%d]: expected %.6f, got %.6f%n",
                            i, j, expected, output[i][j]);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Main method for standalone testing
     */
    public static void main(String[] args) {
        int numArrays = args.length > 0 ? Integer.parseInt(args[0]) : 100000;
        int arraySize = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        int windowSize = args.length > 2 ? Integer.parseInt(args[2]) : 5;

        System.out.println("================================================================");
        System.out.println("CUDA Moving Average Demo (CPU Fallback)");
        System.out.println("================================================================");
        System.out.printf("Number of arrays:  %,d%n", numArrays);
        System.out.printf("Array size:        %d floats%n", arraySize);
        System.out.printf("Window size:       %d%n", windowSize);
        System.out.printf("Total elements:    %,d%n", (long) numArrays * arraySize);
        System.out.printf("Memory per array:  %.2f KB%n", arraySize * 4.0 / 1024);
        System.out.printf("Total memory:      %.2f MB%n", (long) numArrays * arraySize * 4.0 / (1024 * 1024));
        System.out.println("================================================================");
        System.out.println();

        // Generate test data
        System.out.println("Generating random data...");
        long genStart = System.nanoTime();
        float[][] input = generateRandomArrays(numArrays, arraySize, 42);
        long genElapsed = System.nanoTime() - genStart;
        System.out.printf("Data generation: %.3f seconds%n%n", genElapsed / 1e9);

        // Compute moving average
        System.out.println("Computing moving averages on CPU...");
        float[][] output = computeMovingAverage(input, windowSize);
        System.out.println();

        // Verify first few results
        System.out.println("Verifying results (first array)...");
        boolean correct = verify(new float[][]{input[0]}, new float[][]{output[0]}, windowSize);
        System.out.printf("Verification: %s%n%n", correct ? "PASSED" : "FAILED");

        // Print sample results
        System.out.println("Sample results (first array, first 10 elements):");
        System.out.println("Index | Input    | Moving Avg");
        System.out.println("------|----------|------------");
        for (int i = 0; i < Math.min(10, arraySize); i++) {
            System.out.printf("%5d | %8.3f | %10.3f%n", i, input[0][i], output[0][i]);
        }
        System.out.println();

        // Performance summary
        long totalOps = (long) numArrays * arraySize;
        double totalTimeSec = (System.nanoTime() - genStart - genElapsed) / 1e9;
        double throughput = totalOps / totalTimeSec / 1e6;

        System.out.println("================================================================");
        System.out.println("Performance Summary:");
        System.out.println("================================================================");
        System.out.printf("Throughput: %.2f M elements/sec%n", throughput);
        System.out.printf("Note: For GPU acceleration, use RapidsMovingAverage with Spark%n");
        System.out.println("================================================================");
    }
}
