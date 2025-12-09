package org.surfing.drsquirrel.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

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
import java.time.Duration;
import java.util.*;
import org.apache.thrift.TBase;
import org.surfing.drsquirrel.jni.NativeThriftDecoder;
import org.apache.thrift.ext.GenericThriftToArrowConverter;

/**
 * Java-native Kafka consumer that produces Arrow batches (topic, payload).
 */
public final class KafkaSourceArrow implements AutoCloseable {
    public static final Schema SCHEMA = new Schema(Arrays.asList(
            new Field("topic", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("payload", FieldType.nullable(new ArrowType.Binary()), null)
    ));

    private final BufferAllocator allocator;
    private final KafkaConsumer<String, byte[]> consumer;
    private final Duration pollTimeout;
    private String decodeStrategy = "auto";
    private String thriftSchemaResource;

    private KafkaSourceArrow(BufferAllocator allocator, KafkaConsumer<String, byte[]> consumer, Duration pollTimeout) {
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
        private String thriftDecodeStrategy = "auto"; // auto | native | java
        private String thriftSchemaResource; // optional classpath resource to .thrift

        public Builder setAllocator(BufferAllocator alloc) { this.allocator = alloc; return this; }
        public Builder setBootstrapServers(String bs) { this.bootstrapServers = bs; return this; }
        public Builder setServersetPath(String path) { this.serversetPath = path; return this; }
        public Builder setGroupId(String gid) { this.groupId = gid; return this; }
        public Builder setTopics(List<String> t) { this.topics = t; return this; }
        public Builder putKafkaProperty(String k, String v) { this.extraProps.put(k, v); return this; }
        public Builder setPollTimeout(Duration d) { this.pollTimeout = d; return this; }
        public Builder setThriftDecodeStrategy(String strategy) {
            if (strategy == null) return this;
            String s = strategy.toLowerCase(Locale.ROOT);
            if (!s.equals("auto") && !s.equals("native") && !s.equals("java")) {
                throw new IllegalArgumentException("strategy must be one of: auto,native,java");
            }
            this.thriftDecodeStrategy = s; return this;
        }
        public Builder setThriftSchemaResource(String resourcePath) { this.thriftSchemaResource = resourcePath; return this; }

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
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.putAll(extraProps);

            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
            if (topics == null || topics.isEmpty()) throw new IllegalArgumentException("topics required");
            consumer.subscribe(topics);
            KafkaSourceArrow k = new KafkaSourceArrow(allocator, consumer, pollTimeout);
            k.decodeStrategy = this.thriftDecodeStrategy;
            k.thriftSchemaResource = this.thriftSchemaResource;
            return k;
        }
    }

    /** Poll once up to maxRecords with timeout. */
    public VectorSchemaRoot pollOnce(int maxRecords) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        try (VarCharVector topic = (VarCharVector) root.getVector("topic");
             org.apache.arrow.vector.VarBinaryVector payload = (org.apache.arrow.vector.VarBinaryVector) root.getVector("payload")) {
            root.allocateNew();
            int count = 0;
            ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
            for (ConsumerRecord<String, byte[]> rec : records) {
                if (count >= maxRecords) break;
                topic.setSafe(count, new Text(rec.topic()));
                byte[] bytes = rec.value() != null ? rec.value() : new byte[0];
                payload.setSafe(count, bytes);
                count++;
            }
            topic.setValueCount(count);
            payload.setValueCount(count);
            root.setRowCount(count);
        }
        return root;
    }

    /**
     * Poll once and decode Kafka value payloads as Thrift Binary protocol into Arrow using
     * GenericThriftToArrowConverter. Returns an Arrow batch matching the Thrift schema.
     */
    public VectorSchemaRoot pollOnceAsThrift(int maxRecords, Class<? extends TBase<?, ?>> thriftClass) {
        // Collect payloads
        List<byte[]> payloads = new ArrayList<>();
        ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
        int count = 0;
        for (ConsumerRecord<String, byte[]> rec : records) {
            if (count >= maxRecords) break;
            payloads.add(rec.value() != null ? rec.value() : new byte[0]);
            count++;
        }
        GenericThriftToArrowConverter conv = new GenericThriftToArrowConverter();
        return conv.convert(payloads, thriftClass, allocator);
    }

    /**
     * Poll once and decode via JNI fast path. Provide either:
     * - thriftPath + structName (native schema discovery), or
     * - null/null, plus a thriftClass to trigger Java fallback.
     * Falls back to GenericThriftToArrowConverter if JNI fails.
     */
    public VectorSchemaRoot pollOnceAsThriftNative(int maxRecords,
                                                   String thriftPath,
                                                   String structName,
                                                   Class<? extends TBase<?, ?>> thriftClass) {
        List<byte[]> payloads = new ArrayList<>();
        ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeout);
        int count = 0;
        for (ConsumerRecord<String, byte[]> rec : records) {
            if (count >= maxRecords) break;
            payloads.add(rec.value() != null ? rec.value() : new byte[0]);
            count++;
        }
        byte[][] arr = payloads.toArray(new byte[0][]);
        try {
            if (thriftPath != null && structName != null) {
                return NativeThriftDecoder.convert(allocator, arr, thriftPath, structName);
            }
        } catch (Throwable t) {
            // fall through to Java converter
        }
        // Fallback to Java converter
        GenericThriftToArrowConverter conv = new GenericThriftToArrowConverter();
        if (thriftClass == null) {
            throw new IllegalArgumentException("thriftClass required when JNI thriftPath/structName not provided");
        }
        return conv.convert(payloads, thriftClass, allocator);
    }

    public VectorSchemaRoot pollOnceAsThriftNative(int maxRecords,
                                                   Class<? extends TBase<?, ?>> thriftClass) {
        String structName = thriftClass != null ? thriftClass.getSimpleName() : null;
        try {
            return pollOnceAsThriftNative(maxRecords, null, structName, thriftClass);
        } catch (Throwable t) {
            return pollOnceAsThrift(maxRecords, thriftClass);
        }
    }

    public VectorSchemaRoot pollOnceAsThriftNative(int maxRecords,
                                                   Class<? extends TBase<?, ?>> thriftClass,
                                                   String thriftResourcePath) {
        java.net.URL res = Thread.currentThread().getContextClassLoader().getResource(thriftResourcePath);
        if (res == null) {
            return pollOnceAsThrift(maxRecords, thriftClass);
        }
        try {
            java.nio.file.Path tmp = java.nio.file.Files.createTempFile("thrift-schema-", ".thrift");
            tmp.toFile().deleteOnExit();
            try (java.io.InputStream in = res.openStream()) {
                java.nio.file.Files.copy(in, tmp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            return pollOnceAsThriftNative(maxRecords, tmp.toAbsolutePath().toString(),
                    thriftClass != null ? thriftClass.getSimpleName() : null, thriftClass);
        } catch (Exception e) {
            return pollOnceAsThrift(maxRecords, thriftClass);
        }
    }

    public VectorSchemaRoot pollOnceToArrow(int maxRecords,
                                            Class<? extends TBase<?, ?>> thriftClass,
                                            String thriftResourcePathIfAny) {
        switch (decodeStrategy) {
            case "java":
                return pollOnceAsThrift(maxRecords, thriftClass);
            case "native":
                return (thriftResourcePathIfAny != null ? thriftResourcePathIfAny : this.thriftSchemaResource) != null
                        ? pollOnceAsThriftNative(maxRecords, thriftClass, (thriftResourcePathIfAny != null ? thriftResourcePathIfAny : this.thriftSchemaResource))
                        : pollOnceAsThriftNative(maxRecords, thriftClass);
            default:
                try {
                    String res = thriftResourcePathIfAny != null ? thriftResourcePathIfAny : this.thriftSchemaResource;
                    return res != null
                            ? pollOnceAsThriftNative(maxRecords, thriftClass, res)
                            : pollOnceAsThriftNative(maxRecords, thriftClass);
                } catch (Throwable t) {
                    return pollOnceAsThrift(maxRecords, thriftClass);
                }
        }
    }

    @Override
    public void close() {
        try { consumer.close(); } catch (Exception ignore) {}
        // allocator is external if provided, so do not close here
    }
}
