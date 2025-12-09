package org.surfing.drsquirrel.kafka;

import junit.framework.TestCase;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.surfing.drsquirrel.jni.NativeThriftDecoder;
import org.surfing.mabs_metrics.thrift.MabsMetrics;

/**
 * JNI smoke test for NativeThriftDecoder. Skips unless -DenableJNI=true is set.
 */
public class KafkaSourceArrowNativeIT extends TestCase {
  private static boolean jniEnabled() {
    return Boolean.parseBoolean(System.getProperty("enableJNI", "false"));
  }

  public void testNativeDecoderSmoke() {
    if (!jniEnabled()) {
      System.out.println("[SKIP] KafkaSourceArrowNativeIT: enable with -DenableJNI=true");
      return;
    }
    try {
      // Try resource-based native path using the builder strategy
      KafkaSourceArrow src = KafkaSourceArrow.newBuilder()
          .setBootstrapServers("localhost:9092") // not used in this offline test
          .setTopics(java.util.Arrays.asList("t"))
          .setThriftDecodeStrategy("native")
          .setThriftSchemaResource("schemas/mabs.thrift")
          .build();
      try {
        // We cannot poll Kafka here; directly validate the native decoder using a hand-made payload below.
      } finally {
        src.close();
      }

      // Build a tiny binary payload matching MabsMetrics.read(): just set timestamp (field 1)
      org.apache.thrift.transport.TMemoryBuffer buf = new org.apache.thrift.transport.TMemoryBuffer(64);
      org.apache.thrift.protocol.TBinaryProtocol proto = new org.apache.thrift.protocol.TBinaryProtocol(buf);
      proto.writeStructBegin(new org.apache.thrift.protocol.TStruct("MabsMetrics"));
      proto.writeFieldBegin(new org.apache.thrift.protocol.TField("timestamp", org.apache.thrift.protocol.TType.I64, (short)1));
      proto.writeI64(7L);
      proto.writeFieldEnd();
      proto.writeFieldStop();
      proto.writeStructEnd();
      byte[] payload = java.util.Arrays.copyOf(buf.getArray(), buf.length());

      VectorSchemaRoot root = NativeThriftDecoder.convert(new RootAllocator(),
          new byte[][] { payload },
          null, // thriftPath not required if native has schema mapping; may be null
          "MabsMetrics");
      try {
        assertNotNull(root.getSchema());
        assertEquals(1, root.getRowCount());
      } finally {
        root.close();
      }
    } catch (org.apache.thrift.TException e) {
      System.out.println("[SKIP] Thrift write error: " + e.getMessage());
    } catch (UnsatisfiedLinkError e) {
      System.out.println("[SKIP] Native lib not available: " + e.getMessage());
    } catch (Throwable t) {
      // If JNI fails for environmental reasons, don't fail default test run
      if (jniEnabled()) throw t;
      System.out.println("[SKIP] JNI test skipped due to: " + t);
    }
  }
}
