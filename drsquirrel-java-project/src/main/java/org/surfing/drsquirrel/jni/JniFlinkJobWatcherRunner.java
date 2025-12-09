package org.surfing.drsquirrel.jni;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;

/**
 * Minimal Java runner that exercises JNI processors shuffle under MPI.
 * Optionally polls Kafka via JNI when KAFKA_* env variables are set.
 */
public class JniFlinkJobWatcherRunner {
    private static VectorSchemaRoot makeBatch(BufferAllocator alloc) {
        Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field val = new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(key, val));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        BigIntVector keyVec = (BigIntVector) root.getVector("key");
        IntVector valVec = (IntVector) root.getVector("val");
        root.allocateNew();
        for (int i = 0; i < 5; i++) {
            keyVec.setSafe(i, i);
            valVec.setSafe(i, i * 10);
        }
        keyVec.setValueCount(5);
        valVec.setValueCount(5);
        root.setRowCount(5);
        return root;
    }

    public static void main(String[] args) {
        try (RootAllocator alloc = new RootAllocator()) {
            try (VectorSchemaRoot in = makeBatch(alloc)) {
                // One-sided shuffle
                VectorSchemaRoot out1 = NativeProcessors.shuffle(alloc, in, "key", true, 0, 1);
                out1.close();
                // Two-sided shuffle
                VectorSchemaRoot out2 = NativeProcessors.shuffle(alloc, in, "key", false, 0, 1);
                out2.close();
            }

            // Optional: Java-native Kafka poll once (preferred over JNI)
            String serverset = System.getenv("KAFKA_SERVERSET");
            String topicsCsv = System.getenv("KAFKA_TOPICS");
            String groupId = System.getenv("KAFKA_GROUP_ID");
            if (serverset != null && topicsCsv != null && groupId != null) {
                String[] topics = topicsCsv.split(",");
                org.surfing.drsquirrel.kafka.KafkaSourceArrow src =
                        org.surfing.drsquirrel.kafka.KafkaSourceArrow.newBuilder()
                                .setAllocator(alloc)
                                .setServersetPath(serverset)
                                .setGroupId(groupId)
                                .setTopics(Arrays.asList(topics))
                                .build();
                try (VectorSchemaRoot batch = src.pollOnce(50)) {
                    // Optionally shuffle or map further via JNI processors
                    if (batch.getRowCount() > 0) {
                        VectorSchemaRoot out = NativeProcessors.shuffle(alloc, batch, "key", true, 0, 1);
                        if (out != null) out.close();
                    }
                }
                src.close();
            }
        }
        System.out.println("JNI runner completed OK");
    }
}
