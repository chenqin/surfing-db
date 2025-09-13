package com.pinterest.drsquirrel.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Java-native Kafka consumer that produces Arrow batches (topic, payload).
 */
public final class KafkaSourceArrow implements AutoCloseable {
    public static final Schema SCHEMA = new Schema(Arrays.asList(
            new Field("topic", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("payload", FieldType.nullable(new ArrowType.Utf8()), null)
    ));

    private final BufferAllocator allocator;
    private final KafkaConsumer<String, String> consumer;
    private final Duration pollTimeout;

    private KafkaSourceArrow(BufferAllocator allocator, KafkaConsumer<String, String> consumer, Duration pollTimeout) {
        this.allocator = allocator != null ? allocator : new RootAllocator();
        this.consumer = consumer;
        this.pollTimeout = pollTimeout != null ? pollTimeout : Duration.ofMillis(200);
    }

    public static Builder newBuilder() { return new Builder(); }

    public static final class Builder {
        private BufferAllocator allocator;
        private String bootstrapServers;
        private String serversetPath;
        private String groupId = "flink-watcher";
        private List<String> topics = new ArrayList<>();
        private Properties extraProps = new Properties();
        private Duration pollTimeout = Duration.ofMillis(200);

        public Builder setAllocator(BufferAllocator alloc) { this.allocator = alloc; return this; }
        public Builder setBootstrapServers(String bs) { this.bootstrapServers = bs; return this; }
        public Builder setServersetPath(String path) { this.serversetPath = path; return this; }
        public Builder setGroupId(String gid) { this.groupId = gid; return this; }
        public Builder setTopics(List<String> t) { this.topics = t; return this; }
        public Builder putKafkaProperty(String k, String v) { this.extraProps.put(k, v); return this; }
        public Builder setPollTimeout(Duration d) { this.pollTimeout = d; return this; }

        private static String buildBootstrapFromServerset(String serversetPath) {
            if (serversetPath == null) return null;
            List<String> endpoints = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(serversetPath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) continue;
                    endpoints.add(line);
                }
            } catch (Exception ignore) {}
            return endpoints.isEmpty() ? null : String.join(",", endpoints);
        }

        public KafkaSourceArrow build() {
            Properties props = new Properties();
            String bs = bootstrapServers != null ? bootstrapServers : buildBootstrapFromServerset(serversetPath);
            if (bs == null) throw new IllegalArgumentException("bootstrapServers or serversetPath required");
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, Objects.requireNonNull(groupId, "groupId"));
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.putAll(extraProps);

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            if (topics == null || topics.isEmpty()) throw new IllegalArgumentException("topics required");
            consumer.subscribe(topics);
            return new KafkaSourceArrow(allocator, consumer, pollTimeout);
        }
    }

    /** Poll once up to maxRecords with timeout. */
    public VectorSchemaRoot pollOnce(int maxRecords) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        try (VarCharVector topic = (VarCharVector) root.getVector("topic");
             VarCharVector payload = (VarCharVector) root.getVector("payload")) {
            root.allocateNew();
            int count = 0;
            ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
            for (ConsumerRecord<String, String> rec : records) {
                if (count >= maxRecords) break;
                topic.setSafe(count, new Text(rec.topic()));
                byte[] bytes = rec.value() != null ? rec.value().getBytes(StandardCharsets.UTF_8) : new byte[0];
                payload.setSafe(count, bytes);
                count++;
            }
            topic.setValueCount(count);
            payload.setValueCount(count);
            root.setRowCount(count);
        }
        return root;
    }

    @Override
    public void close() {
        try { consumer.close(); } catch (Exception ignore) {}
        // allocator is external if provided, so do not close here
    }
}

