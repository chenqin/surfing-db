package org.surfing.drsquirrel.jni;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * JNI access to native KafkaConnector producing Arrow batches (topic, payload).
 */
public final class NativeKafkaConnector implements AutoCloseable {
    static {
        // Ensure libsurfingkafkajni.so is discoverable via java.library.path
        System.loadLibrary("surfingkafkajni");
    }

    private long handle = 0L;

    public NativeKafkaConnector(String name,
                                int batch,
                                int intervalMs,
                                String[] topics,
                                String serverset,
                                String groupId,
                                boolean pii) {
        this.handle = create(name, batch, intervalMs, topics, serverset, groupId, pii);
        if (this.handle == 0L) throw new IllegalStateException("Failed to create native connector");
    }

    private static native long create(String name, int batch, int intervalMs,
                                      String[] topics, String serverset, String groupId, boolean pii);
    private static native void destroy(long handle);
    private static native void pollOnce(long handle, long schemaOutAddr, long arrayOutAddr);

    public VectorSchemaRoot poll(BufferAllocator allocator) {
        if (allocator == null) allocator = new RootAllocator();
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
             ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
            pollOnce(this.handle, outSchema.memoryAddress(), outArray.memoryAddress());
            return Data.importVectorSchemaRoot(allocator, outArray, outSchema, null);
        }
    }

    @Override
    public void close() {
        if (this.handle != 0L) {
            destroy(this.handle);
            this.handle = 0L;
        }
    }
}

