package com.pinterest.drsquirrel.thrift;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.ext.GenericThriftToArrowConverter;
import org.junit.Test;

import com.pinterest.deep.bench.DeepEvent;

public class GenericThriftToArrowConverterDeepEventTest {

  private static byte[] encodeDeepEvent() throws Exception {
    TMemoryBuffer buf = new TMemoryBuffer(8192);
    TBinaryProtocol op = new TBinaryProtocol(buf);
    op.writeStructBegin(new TStruct("DeepEvent"));

    // event_id
    op.writeFieldBegin(new TField("event_id", TType.I64, (short)1));
    op.writeI64(42L);
    op.writeFieldEnd();

    // source
    op.writeFieldBegin(new TField("source", TType.STRING, (short)2));
    op.writeString("deep-source");
    op.writeFieldEnd();

    // metrics: list<list<i32>>
    op.writeFieldBegin(new TField("metrics", TType.LIST, (short)3));
    op.writeListBegin(new TList(TType.LIST, 2));
    op.writeListBegin(new TList(TType.I32, 2));
    op.writeI32(10);
    op.writeI32(11);
    op.writeListEnd();
    op.writeListBegin(new TList(TType.I32, 1));
    op.writeI32(12);
    op.writeListEnd();
    op.writeListEnd();
    op.writeFieldEnd();

    // label_ids: set<i64>
    op.writeFieldBegin(new TField("label_ids", TType.SET, (short)4));
    op.writeSetBegin(new TSet(TType.I64, 2));
    op.writeI64(101L);
    op.writeI64(202L);
    op.writeSetEnd();
    op.writeFieldEnd();

    // counts_by_key: map<string, list<i64>>
    op.writeFieldBegin(new TField("counts_by_key", TType.MAP, (short)5));
    op.writeMapBegin(new TMap(TType.STRING, TType.LIST, 1));
    op.writeString("alpha");
    op.writeListBegin(new TList(TType.I64, 2));
    op.writeI64(1000L);
    op.writeI64(2000L);
    op.writeListEnd();
    op.writeMapEnd();
    op.writeFieldEnd();

    // meta
    op.writeFieldBegin(new TField("meta", TType.STRUCT, (short)6));
    op.writeStructBegin(new TStruct("Meta"));
    op.writeFieldBegin(new TField("labels", TType.MAP, (short)1));
    op.writeMapBegin(new TMap(TType.STRING, TType.STRING, 1));
    op.writeString("env");
    op.writeString("prod");
    op.writeMapEnd();
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("kvs", TType.LIST, (short)2));
    op.writeListBegin(new TList(TType.STRUCT, 1));
    op.writeStructBegin(new TStruct("Attr"));
    op.writeFieldBegin(new TField("key", TType.STRING, (short)1));
    op.writeString("foo");
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("val", TType.STRING, (short)2));
    op.writeString("bar");
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    op.writeListEnd();
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    op.writeFieldEnd();

    // readings: list<Reading>
    op.writeFieldBegin(new TField("readings", TType.LIST, (short)7));
    op.writeListBegin(new TList(TType.STRUCT, 1));
    op.writeStructBegin(new TStruct("Reading"));
    op.writeFieldBegin(new TField("ts", TType.I64, (short)1));
    op.writeI64(1L);
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("value", TType.DOUBLE, (short)2));
    op.writeDouble(1.5);
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("notes", TType.LIST, (short)3));
    op.writeListBegin(new TList(TType.STRING, 2));
    op.writeString("note-a");
    op.writeString("note-b");
    op.writeListEnd();
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    op.writeListEnd();
    op.writeFieldEnd();

    // bundles: map<string, Bundle>
    op.writeFieldBegin(new TField("bundles", TType.MAP, (short)8));
    op.writeMapBegin(new TMap(TType.STRING, TType.STRUCT, 1));
    op.writeString("bundle-1");
    op.writeStructBegin(new TStruct("Bundle"));
    op.writeFieldBegin(new TField("items", TType.LIST, (short)1));
    op.writeListBegin(new TList(TType.STRUCT, 1));
    op.writeStructBegin(new TStruct("Reading"));
    op.writeFieldBegin(new TField("ts", TType.I64, (short)1));
    op.writeI64(5L);
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("value", TType.DOUBLE, (short)2));
    op.writeDouble(9.9);
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("notes", TType.LIST, (short)3));
    op.writeListBegin(new TList(TType.STRING, 0));
    op.writeListEnd();
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    op.writeListEnd();
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("extras", TType.MAP, (short)2));
    op.writeMapBegin(new TMap(TType.STRING, TType.LIST, 1));
    op.writeString("extra");
    op.writeListBegin(new TList(TType.STRING, 1));
    op.writeString("val");
    op.writeListEnd();
    op.writeMapEnd();
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    op.writeMapEnd();
    op.writeFieldEnd();

    // attr_maps: list<map<string, list<Attr>>>
    op.writeFieldBegin(new TField("attr_maps", TType.LIST, (short)9));
    op.writeListBegin(new TList(TType.MAP, 1));
    op.writeMapBegin(new TMap(TType.STRING, TType.LIST, 1));
    op.writeString("attrs");
    op.writeListBegin(new TList(TType.STRUCT, 1));
    op.writeStructBegin(new TStruct("Attr"));
    op.writeFieldBegin(new TField("key", TType.STRING, (short)1));
    op.writeString("akey");
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("val", TType.STRING, (short)2));
    op.writeString("aval");
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    op.writeListEnd();
    op.writeMapEnd();
    op.writeListEnd();
    op.writeFieldEnd();

    // geo
    op.writeFieldBegin(new TField("geo", TType.STRUCT, (short)10));
    op.writeStructBegin(new TStruct("Geo"));
    op.writeFieldBegin(new TField("lat", TType.DOUBLE, (short)1));
    op.writeDouble(12.34);
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("lon", TType.DOUBLE, (short)2));
    op.writeDouble(56.78);
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("region", TType.STRUCT, (short)3));
    op.writeStructBegin(new TStruct("Region"));
    op.writeFieldBegin(new TField("country", TType.STRING, (short)1));
    op.writeString("US");
    op.writeFieldEnd();
    op.writeFieldBegin(new TField("city", TType.STRING, (short)2));
    op.writeString("NYC");
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    op.writeFieldEnd();

    op.writeFieldStop();
    op.writeStructEnd();
    return Arrays.copyOf(buf.getArray(), buf.length());
  }

  @Test
  public void deepEventConvertsWithNestedCollections() throws Exception {
    byte[] payload = encodeDeepEvent();
    BufferAllocator alloc = new RootAllocator();
    GenericThriftToArrowConverter conv = new GenericThriftToArrowConverter();
    try (VectorSchemaRoot root = conv.convert(Arrays.asList(payload), (Class<? extends TBase<?, ?>>) (Class<?>) DeepEvent.class, alloc)) {
      assertEquals(1, root.getRowCount());
      BigIntVector eventId = (BigIntVector) root.getVector("event_id");
      assertEquals(42L, eventId.get(0));

      VarCharVector source = (VarCharVector) root.getVector("source");
      assertEquals("deep-source", new String(source.get(0), StandardCharsets.UTF_8));

      ListVector metrics = (ListVector) root.getVector("metrics");
      ListVector innerMetrics = (ListVector) metrics.getDataVector();
      IntVector metricValues = (IntVector) innerMetrics.getDataVector();
      assertEquals(10, metricValues.get(0));
      assertEquals(11, metricValues.get(1));
      assertEquals(12, metricValues.get(2));

      ListVector labelIds = (ListVector) root.getVector("label_ids");
      BigIntVector labelValues = (BigIntVector) labelIds.getDataVector();
      assertEquals(101L, labelValues.get(0));
      assertEquals(202L, labelValues.get(1));

      MapVector counts = (MapVector) root.getVector("counts_by_key");
      StructVector entries = (StructVector) counts.getDataVector();
      VarCharVector countKeys = (VarCharVector) entries.getChild("key");
      assertEquals("alpha", new String(countKeys.get(0), StandardCharsets.UTF_8));
      ListVector countVals = (ListVector) entries.getChild("value");
      BigIntVector countData = (BigIntVector) countVals.getDataVector();
      assertEquals(1000L, countData.get(0));
      assertEquals(2000L, countData.get(1));

      StructVector meta = (StructVector) root.getVector("meta");
      MapVector labels = (MapVector) meta.getChild("labels");
      StructVector labelEntries = (StructVector) labels.getDataVector();
      VarCharVector labelKeys = (VarCharVector) labelEntries.getChild("key");
      assertEquals("env", new String(labelKeys.get(0), StandardCharsets.UTF_8));
      VarCharVector labelVals = (VarCharVector) labelEntries.getChild("value");
      assertEquals("prod", new String(labelVals.get(0), StandardCharsets.UTF_8));

      ListVector attrMaps = (ListVector) root.getVector("attr_maps");
      MapVector firstMap = (MapVector) attrMaps.getDataVector();
      StructVector attrEntry = (StructVector) firstMap.getDataVector();
      VarCharVector attrKey = (VarCharVector) attrEntry.getChild("key");
      assertEquals("attrs", new String(attrKey.get(0), StandardCharsets.UTF_8));
      ListVector attrVals = (ListVector) attrEntry.getChild("value");
      StructVector attrStruct = (StructVector) attrVals.getDataVector();
      VarCharVector attrName = (VarCharVector) attrStruct.getChild("key");
      assertEquals("akey", new String(attrName.get(0), StandardCharsets.UTF_8));

      StructVector geo = (StructVector) root.getVector("geo");
      VarCharVector regionCity = (VarCharVector) ((StructVector) geo.getChild("region")).getChild("city");
      assertEquals("NYC", new String(regionCity.get(0), StandardCharsets.UTF_8));
    }
  }
}
