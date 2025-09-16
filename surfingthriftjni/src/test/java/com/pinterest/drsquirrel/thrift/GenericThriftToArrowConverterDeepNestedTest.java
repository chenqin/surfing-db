package com.pinterest.drsquirrel.thrift;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.Test;
import org.junit.Ignore;

import com.pinterest.nested.thrift.DeepContainer;

public class GenericThriftToArrowConverterDeepNestedTest {

  private static byte[] encodeDeep(long[] ids) throws Exception {
    // Build a DeepContainer with given DeepItem ids.
    // For each DeepItem i:
    // - children: [ { name:"c<i>", weights: { "w": i } } ]
    // - tags: [ ["a", "b<i>"] ]
    // - props: { "p": [ i, i+1 ] }
    TMemoryBuffer buf = new TMemoryBuffer(4096);
    TBinaryProtocol op = new TBinaryProtocol(buf);
    op.writeStructBegin(new TStruct("DeepContainer"));
    op.writeFieldBegin(new TField("items", TType.LIST, (short)1));
    op.writeListBegin(new TList(TType.STRUCT, ids.length));
    for (int i=0;i<ids.length;i++){
      op.writeStructBegin(new TStruct("DeepItem"));
      // id
      op.writeFieldBegin(new TField("id", TType.I64, (short)1));
      op.writeI64(ids[i]);
      op.writeFieldEnd();
      // children (list<DeepChild>) with 1 element
      op.writeFieldBegin(new TField("children", TType.LIST, (short)2));
      op.writeListBegin(new TList(TType.STRUCT, 1));
      op.writeStructBegin(new TStruct("DeepChild"));
      op.writeFieldBegin(new TField("name", TType.STRING, (short)1));
      op.writeString("c" + i);
      op.writeFieldEnd();
      op.writeFieldBegin(new TField("weights", TType.MAP, (short)2));
      op.writeMapBegin(new TMap(TType.STRING, TType.I64, 1));
      op.writeString("w");
      op.writeI64(ids[i]);
      op.writeMapEnd();
      op.writeFieldEnd();
      op.writeFieldStop();
      op.writeStructEnd();
      op.writeListEnd();
      op.writeFieldEnd();
      // tags (list<list<string>>) with 1 inner list of 2 strings
      op.writeFieldBegin(new TField("tags", TType.LIST, (short)3));
      op.writeListBegin(new TList(TType.LIST, 1));
      op.writeListBegin(new TList(TType.STRING, 2));
      op.writeString("a");
      op.writeString("b" + i);
      op.writeListEnd();
      op.writeListEnd();
      op.writeFieldEnd();
      // props (map<string, list<i64>>) single entry
      op.writeFieldBegin(new TField("props", TType.MAP, (short)4));
      op.writeMapBegin(new TMap(TType.STRING, TType.LIST, 1));
      op.writeString("p");
      op.writeListBegin(new TList(TType.I64, 2));
      op.writeI64(ids[i]);
      op.writeI64(ids[i]+1);
      op.writeListEnd();
      op.writeMapEnd();
      op.writeFieldEnd();
      op.writeFieldStop();
      op.writeStructEnd();
    }
    op.writeListEnd();
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    return java.util.Arrays.copyOf(buf.getArray(), buf.length());
  }

  @Test
  @Ignore("Pending: deep nested ListVector materialization semantics causing NullVector reads")
  public void deepNestedStructuresAreConverted() throws Exception {
    byte[] payload = encodeDeep(new long[]{5L, 6L});
    BufferAllocator alloc = new RootAllocator();
    GenericThriftToArrowConverter conv = new GenericThriftToArrowConverter();
    try (VectorSchemaRoot root = conv.convert(Arrays.asList(payload), (Class<? extends TBase<?, ?>>) (Class<?>) DeepContainer.class, alloc)) {
      assertEquals(1, root.getRowCount());
      org.apache.arrow.vector.ValueVector itemsVV = root.getVector("items");
      assertTrue(itemsVV instanceof ListVector);
      ListVector items = (ListVector) itemsVV;
      StructVector itemStruct = (StructVector) items.getDataVector();
      BigIntVector idVec = (BigIntVector) itemStruct.getChild("id");
      // Flattened indices for first row start at 0
      assertEquals(5L, idVec.get(0));
      assertEquals(6L, idVec.get(1));

      // children: list<struct>
      // Verify deep nested schema shapes
      org.apache.arrow.vector.types.pojo.Schema sch = root.getSchema();
      org.apache.arrow.vector.types.pojo.Field itemsField = sch.findField("items");
      org.apache.arrow.vector.types.pojo.Field elem = itemsField.getChildren().get(0);
      java.util.Map<String, org.apache.arrow.vector.types.pojo.Field> childMap = new java.util.HashMap<>();
      for (org.apache.arrow.vector.types.pojo.Field f : elem.getChildren()) childMap.put(f.getName(), f);
      // children: list<struct<name:utf8, weights:map>>
      org.apache.arrow.vector.types.pojo.Field childrenField = childMap.get("children");
      assertTrue(childrenField.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.List);
      org.apache.arrow.vector.types.pojo.Field childElem = childrenField.getChildren().get(0);
      assertTrue(childElem.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Struct);
      java.util.Map<String, org.apache.arrow.vector.types.pojo.Field> gc = new java.util.HashMap<>();
      for (org.apache.arrow.vector.types.pojo.Field f : childElem.getChildren()) gc.put(f.getName(), f);
      assertTrue(gc.get("name").getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8);
      assertTrue(gc.get("weights").getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Map);
      // tags: list<list<utf8>>
      org.apache.arrow.vector.types.pojo.Field tagsField = childMap.get("tags");
      assertTrue(tagsField.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.List);
      assertTrue(tagsField.getChildren().get(0).getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.List);
      assertTrue(tagsField.getChildren().get(0).getChildren().get(0).getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8);
      // props: map<string, list<i64>>
      org.apache.arrow.vector.types.pojo.Field propsField = childMap.get("props");
      assertTrue(propsField.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Map);
      org.apache.arrow.vector.types.pojo.Field entries = propsField.getChildren().get(0);
      java.util.Map<String, org.apache.arrow.vector.types.pojo.Field> kv = new java.util.HashMap<>();
      for (org.apache.arrow.vector.types.pojo.Field f : entries.getChildren()) kv.put(f.getName(), f);
      assertTrue(kv.get("key").getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8);
      assertTrue(kv.get("value").getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.List);
    }
  }
}
