package com.pinterest.drsquirrel.jni;

import junit.framework.TestCase;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Optional JNI test for Kafka connector. Skips unless environment variables are set:
 * - KAFKA_SERVERSET: path to serverset file (one broker per line host:port)
 * - KAFKA_TOPICS: comma-separated list of topics
 * - KAFKA_GROUP_ID: consumer group id
 */
public class NativeKafkaConnectorTest extends TestCase {
    public void testPollOnceIfAvailable() {
        final String serverset = System.getenv("KAFKA_SERVERSET");
        final String topicsCsv = System.getenv("KAFKA_TOPICS");
        final String groupId = System.getenv("KAFKA_GROUP_ID");
        if (serverset == null || topicsCsv == null || groupId == null) {
            System.out.println("[SKIP] NativeKafkaConnectorTest: KAFKA_SERVERSET/KAFKA_TOPICS/KAFKA_GROUP_ID not set");
            return; // skip test
        }
        String[] topics = topicsCsv.split(",");

        // JNI lib loaded by NativeKafkaConnector class
        try (BufferAllocator alloc = new RootAllocator();
             NativeKafkaConnector connector = new NativeKafkaConnector(
                 "jni-kafka", 100, 200, topics, serverset, groupId, false)) {
            VectorSchemaRoot batch = connector.poll(alloc);
            assertNotNull(batch);
            assertEquals(2, batch.getFieldVectors().size()); // topic, payload
            // batch.close(); // poll() returns a new root each time, caller owns close()
            batch.close();
        }
    }
}

