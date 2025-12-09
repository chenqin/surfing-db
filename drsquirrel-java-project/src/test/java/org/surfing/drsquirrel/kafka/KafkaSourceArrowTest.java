package org.surfing.drsquirrel.kafka;

import junit.framework.TestCase;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.surfing.mabs_metrics.thrift.MabsMetrics;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Minimal offline test that exercises schema and decoding API shape without a live Kafka cluster.
 * We directly construct a small batch mimicking pollOnce() output and run Generic converter via
 * KafkaSourceArrow#pollOnceAsThrift by simulating payloads.
 */
public class KafkaSourceArrowTest extends TestCase {

  public void testBinarySchemaShape() {
    Field payload = KafkaSourceArrow.SCHEMA.findField("payload");
    assertNotNull(payload);
    assertTrue(payload.getType() instanceof ArrowType.Binary);
  }

  public void testThriftDecodePath() throws Exception {
    // Create one MabsMetrics thrift instance and serialize via Binary protocol
    // Build Binary-protocol payload that sets a couple of fields according to MabsMetrics.read()
    // struct begin
    TMemoryBuffer buf = new TMemoryBuffer(256);
    TBinaryProtocol proto = new TBinaryProtocol(buf);
    proto.writeStructBegin(new org.apache.thrift.protocol.TStruct("MabsMetrics"));
    // field 1: timestamp (i64)
    proto.writeFieldBegin(new org.apache.thrift.protocol.TField("timestamp", org.apache.thrift.protocol.TType.I64, (short)1));
    proto.writeI64(12345L);
    proto.writeFieldEnd();
    // field 7: service_name (string)
    proto.writeFieldBegin(new org.apache.thrift.protocol.TField("service_name", org.apache.thrift.protocol.TType.STRING, (short)7));
    proto.writeString("metric");
    proto.writeFieldEnd();
    // stop
    proto.writeFieldStop();
    proto.writeStructEnd();
    byte[] payload = Arrays.copyOf(buf.getArray(), buf.length());

    // Use converter directly to ensure classpath works; this mirrors pollOnceAsThrift batching
    KafkaSourceArrow dummy = KafkaSourceArrow.newBuilder()
      .setBootstrapServers("localhost:9092") // not used in this test
      .setTopics(Arrays.asList("t"))
      .build();
    try {
      VectorSchemaRoot root = new org.apache.thrift.ext.GenericThriftToArrowConverter()
        .convert(Arrays.asList(payload), MabsMetrics.class, new RootAllocator());
      try {
        assertEquals(1, root.getRowCount());
        assertNotNull(root.getSchema());
      } finally {
        root.close();
      }
    } finally {
      dummy.close();
    }
  }
}
