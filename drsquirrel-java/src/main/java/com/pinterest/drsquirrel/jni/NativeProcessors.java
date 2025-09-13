package com.pinterest.drsquirrel.jni;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/** JNI wrapper for C++ processors APIs. */
public final class NativeProcessors {
    static {
        System.loadLibrary("surfingprocessorsjni");
    }

    private NativeProcessors() {}

    private static native void shuffle(long schemaInAddr, long arrayInAddr,
                                       String fieldName, boolean oneSided,
                                       int rank, int world,
                                       long schemaOutAddr, long arrayOutAddr);

    /**
     * Shuffle the input RecordBatch by <fieldName>, using hash-based partitioning.
     * Note: Effective MPI world size is determined by the native runtime. For single-JVM runs,
     * it behaves as world=1.
     */
    public static VectorSchemaRoot shuffle(BufferAllocator allocator,
                                           VectorSchemaRoot input,
                                           String fieldName,
                                           boolean oneSided,
                                           int rank,
                                           int world) {
        if (allocator == null) allocator = new RootAllocator();
        try (ArrowArray inArray = ArrowArray.allocateNew(allocator);
             ArrowSchema inSchema = ArrowSchema.allocateNew(allocator);
             ArrowArray outArray = ArrowArray.allocateNew(allocator);
             ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {

            Data.exportVectorSchemaRoot(allocator, input, null, inArray, inSchema);
            shuffle(inSchema.memoryAddress(), inArray.memoryAddress(), fieldName, oneSided, rank, world,
                    outSchema.memoryAddress(), outArray.memoryAddress());
            return Data.importVectorSchemaRoot(allocator, outArray, outSchema, null);
        }
    }
}

