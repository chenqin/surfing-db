package org.surfing.drsquirrel.jni;

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
        // Ensure MPI is finalized on JVM shutdown to avoid prterun warnings
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { finalizeMPI(); } catch (Throwable t) { /* ignore */ }
        }));
    }

    private NativeProcessors() {}

    private static native void shuffle(long schemaInAddr, long arrayInAddr,
                                       String fieldName, boolean oneSided,
                                       int rank, int world,
                                       long schemaOutAddr, long arrayOutAddr);

    private static native void cogroup(long schemaLeftIn, long arrayLeftIn,
                                       long schemaRightIn, long arrayRightIn,
                                       String fieldName, boolean oneSided,
                                       int rank, int world,
                                       long schemaLeftOut, long arrayLeftOut,
                                       long schemaRightOut, long arrayRightOut);

    private static native void finalizeMPI();
    private static native int mpiRank();
    private static native int mpiWorld();
    private static native String broadcastString(String message);
    private static native void barrier();

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

    /**
     * Co-shuffle two inputs by key and return local partitions for both.
     */
    public static VectorSchemaRoot[] cogroup(BufferAllocator allocator,
                                             VectorSchemaRoot left,
                                             VectorSchemaRoot right,
                                             String fieldName,
                                             boolean oneSided,
                                             int rank,
                                             int world) {
        if (allocator == null) allocator = new RootAllocator();
        try (ArrowArray inLeft = ArrowArray.allocateNew(allocator);
             ArrowSchema inLeftSchema = ArrowSchema.allocateNew(allocator);
             ArrowArray inRight = ArrowArray.allocateNew(allocator);
             ArrowSchema inRightSchema = ArrowSchema.allocateNew(allocator);
             ArrowArray outLeft = ArrowArray.allocateNew(allocator);
             ArrowSchema outLeftSchema = ArrowSchema.allocateNew(allocator);
             ArrowArray outRight = ArrowArray.allocateNew(allocator);
             ArrowSchema outRightSchema = ArrowSchema.allocateNew(allocator)) {

            Data.exportVectorSchemaRoot(allocator, left, null, inLeft, inLeftSchema);
            Data.exportVectorSchemaRoot(allocator, right, null, inRight, inRightSchema);
            cogroup(inLeftSchema.memoryAddress(), inLeft.memoryAddress(),
                    inRightSchema.memoryAddress(), inRight.memoryAddress(),
                    fieldName, oneSided, rank, world,
                    outLeftSchema.memoryAddress(), outLeft.memoryAddress(),
                    outRightSchema.memoryAddress(), outRight.memoryAddress());

            VectorSchemaRoot leftOut = Data.importVectorSchemaRoot(allocator, outLeft, outLeftSchema, null);
            VectorSchemaRoot rightOut = Data.importVectorSchemaRoot(allocator, outRight, outRightSchema, null);
            return new VectorSchemaRoot[]{ leftOut, rightOut };
        }
    }

    /** Return the rank (0..world-1) of this JVM in the native MPI world. */
    public static int getMpiRank() { return mpiRank(); }
    /** Return the native MPI world size. */
    public static int getMpiWorld() { return mpiWorld(); }

    /** Broadcast a UTF-8 string from rank 0 to all ranks and return it. */
    public static String bcast(String messageFromRank0) { return broadcastString(messageFromRank0); }
    /** Barrier across MPI ranks. */
    public static void mpiBarrier() { barrier(); }
}
