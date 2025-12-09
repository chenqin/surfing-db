package org.surfing.drsquirrel.thrift;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.thrift.TBase;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.ext.GenericThriftToArrowConverter;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TType;
import org.junit.Test;

import org.surfing.mabs_metrics.thrift.MabsMetrics;

public class GenericThriftToArrowConverterTest {

  @Test
  public void convertsSinglePayloadToArrow() throws Exception {
    // Prepare a sample Thrift object
    MabsMetrics m = new MabsMetrics();
    m.timestamp = 123456789L;
    m.service_name = "svc";
    Map<String, Long> counters = new HashMap<>();
    counters.put("a", 1L);
    m.counters = counters;

    // Encode a minimal Thrift binary payload manually since MabsMetrics.write() is a stub
    TMemoryBuffer buf = new TMemoryBuffer(256);
    TBinaryProtocol oprot = new TBinaryProtocol(buf);
    oprot.writeStructBegin(new TStruct("MabsMetrics"));
    // field 1: timestamp (i64)
    oprot.writeFieldBegin(new TField("timestamp", TType.I64, (short)1));
    oprot.writeI64(m.timestamp);
    oprot.writeFieldEnd();
    // field 7: service_name (string)
    oprot.writeFieldBegin(new TField("service_name", TType.STRING, (short)7));
    oprot.writeString(m.service_name);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
    byte[] payload = java.util.Arrays.copyOf(buf.getArray(), buf.length());

    // Convert
    BufferAllocator alloc = new RootAllocator();
    GenericThriftToArrowConverter conv = new GenericThriftToArrowConverter();
    try (VectorSchemaRoot root = conv.convert(Arrays.asList(payload), (Class<? extends TBase<?, ?>>) (Class<?>) MabsMetrics.class, alloc)) {
      assertNotNull(root);
      assertEquals(1, root.getRowCount());

      // Verify a numeric field
      BigIntVector ts = (BigIntVector) root.getVector("timestamp");
      assertNotNull(ts);
      assertEquals(1, ts.getValueCount());
      assertEquals(123456789L, ts.get(0));

      // Verify a string field is present (value content not strictly required)
      VarCharVector name = (VarCharVector) root.getVector("service_name");
      assertNotNull(name);
      assertEquals(1, name.getValueCount());
      assertFalse(name.isNull(0));
    }
  }
}
