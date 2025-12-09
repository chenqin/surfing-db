package org.surfing.drsquirrel.thrift;

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
import org.apache.thrift.ext.GenericThriftToArrowConverter;
import org.junit.Test;

import org.surfing.nested.thrift.NestedContainer;

public class GenericThriftToArrowConverterNestedTest {

  private static byte[] encodeContainer(long[] ids, String[][] tags, String[][] mapKeys, long[][] mapVals) throws Exception {
    // Encodes a NestedContainer with N items, each item i has:
    // id = ids[i], tags = tags[i], props = { mapKeys[i][j] -> mapVals[i][j] }
    int n = ids.length;
    TMemoryBuffer buf = new TMemoryBuffer(4096);
    TBinaryProtocol op = new TBinaryProtocol(buf);
    op.writeStructBegin(new TStruct("NestedContainer"));
    op.writeFieldBegin(new TField("items", TType.LIST, (short)1));
    op.writeListBegin(new TList(TType.STRUCT, n));
    for (int i = 0; i < n; i++) {
      op.writeStructBegin(new TStruct("NestedItem"));
      // id
      op.writeFieldBegin(new TField("id", TType.I64, (short)1));
      op.writeI64(ids[i]);
      op.writeFieldEnd();
      // tags
      String[] tt = tags[i]; if (tt == null) tt = new String[0];
      op.writeFieldBegin(new TField("tags", TType.LIST, (short)2));
      op.writeListBegin(new TList(TType.STRING, tt.length));
      for (String s : tt) op.writeString(s);
      op.writeListEnd();
      op.writeFieldEnd();
      // props
      String[] ks = mapKeys[i]; if (ks == null) ks = new String[0];
      long[] vs = mapVals[i]; if (vs == null) vs = new long[0];
      op.writeFieldBegin(new TField("props", TType.MAP, (short)3));
      op.writeMapBegin(new TMap(TType.STRING, TType.I64, ks.length));
      for (int j = 0; j < ks.length; j++) { op.writeString(ks[j]); op.writeI64(vs[j]); }
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
  public void convertsListOfStructWithNestedListAndMap() throws Exception {
    // Build two rows (payloads)
    byte[] p1 = encodeContainer(
        new long[]{1L, 2L},
        new String[][]{ new String[]{"x","y"}, new String[]{} },
        new String[][]{ new String[]{"p"}, new String[]{"a","b"} },
        new long[][]{ new long[]{10L}, new long[]{1L, 2L} }
    );
    byte[] p2 = encodeContainer(
        new long[]{3L},
        new String[][]{ new String[]{"t"} },
        new String[][]{ new String[]{} },
        new long[][]{ new long[]{} }
    );

    BufferAllocator alloc = new RootAllocator();
    GenericThriftToArrowConverter conv = new GenericThriftToArrowConverter();
    try (VectorSchemaRoot root = conv.convert(Arrays.asList(p1, p2), (Class<? extends TBase<?, ?>>) (Class<?>) NestedContainer.class, alloc)) {
      assertEquals(2, root.getRowCount());
      ListVector items = (ListVector) root.getVector("items");
      assertNotNull(items);

      // Verify list lengths using offsets
      // First row has two items, second row has one; flattened indices start at 0

      StructVector itemStruct = (StructVector) items.getDataVector();
      BigIntVector idVec = (BigIntVector) itemStruct.getChild("id");
      assertEquals(1L, idVec.get(0));
      assertEquals(2L, idVec.get(1));
      assertEquals(3L, idVec.get(2));

      // Verify schema types for nested columns
      org.apache.arrow.vector.types.pojo.Schema sch = root.getSchema();
      org.apache.arrow.vector.types.pojo.Field itemsField = sch.findField("items");
      assertNotNull(itemsField);
      assertTrue(itemsField.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.List);
      org.apache.arrow.vector.types.pojo.Field elem = itemsField.getChildren().get(0);
      assertTrue(elem.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Struct);
      java.util.Map<String, org.apache.arrow.vector.types.pojo.Field> childMap = new java.util.HashMap<>();
      for (org.apache.arrow.vector.types.pojo.Field f : elem.getChildren()) childMap.put(f.getName(), f);
      assertTrue(childMap.get("tags").getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.List);
      org.apache.arrow.vector.types.pojo.Field tagElem = childMap.get("tags").getChildren().get(0);
      assertTrue(tagElem.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8);
      assertTrue(childMap.get("props").getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Map);
    }
  }

  @Test
  public void handlesNullNestedFields() throws Exception {
      // Encode one item with only 'id' set; omit 'tags' and 'props' entirely
    TMemoryBuffer buf = new TMemoryBuffer(512);
    TBinaryProtocol op = new TBinaryProtocol(buf);
    op.writeStructBegin(new TStruct("NestedContainer"));
    op.writeFieldBegin(new TField("items", TType.LIST, (short)1));
    op.writeListBegin(new TList(TType.STRUCT, 1));
    op.writeStructBegin(new TStruct("NestedItem"));
    op.writeFieldBegin(new TField("id", TType.I64, (short)1));
    op.writeI64(42L);
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    op.writeListEnd();
    op.writeFieldEnd();
    op.writeFieldStop();
    op.writeStructEnd();
    byte[] p = java.util.Arrays.copyOf(buf.getArray(), buf.length());

    BufferAllocator alloc = new RootAllocator();
    GenericThriftToArrowConverter conv = new GenericThriftToArrowConverter();
    try (VectorSchemaRoot root = conv.convert(Arrays.asList(p), (Class<? extends TBase<?, ?>>) (Class<?>) NestedContainer.class, alloc)) {
      assertEquals(1, root.getRowCount());
      ListVector items = (ListVector) root.getVector("items");
      StructVector itemStruct = (StructVector) items.getDataVector();
      BigIntVector idVec = (BigIntVector) itemStruct.getChild("id");
      assertEquals(42L, idVec.get(0));
      // Validate schema presence of tags/props
      org.apache.arrow.vector.types.pojo.Schema sch = root.getSchema();
      org.apache.arrow.vector.types.pojo.Field itemsField = sch.findField("items");
      org.apache.arrow.vector.types.pojo.Field elem = itemsField.getChildren().get(0);
      java.util.Map<String, org.apache.arrow.vector.types.pojo.Field> childMap = new java.util.HashMap<>();
      for (org.apache.arrow.vector.types.pojo.Field f : elem.getChildren()) childMap.put(f.getName(), f);
      assertNotNull(childMap.get("tags"));
      assertNotNull(childMap.get("props"));
    }
  }

  @Test
  public void largeListStress() throws Exception {
    int n = 128;
    long[] ids = new long[n];
    String[][] tags = new String[n][];
    String[][] mks = new String[n][];
    long[][] mvs = new long[n][];
    for (int i = 0; i < n; i++) {
      ids[i] = i;
      tags[i] = new String[]{"t" + i};
      mks[i] = new String[]{"k" + i};
      mvs[i] = new long[]{i};
    }
    byte[] p = encodeContainer(ids, tags, mks, mvs);

    BufferAllocator alloc = new RootAllocator();
    GenericThriftToArrowConverter conv = new GenericThriftToArrowConverter();
    try (VectorSchemaRoot root = conv.convert(Arrays.asList(p), (Class<? extends TBase<?, ?>>) (Class<?>) NestedContainer.class, alloc)) {
      assertEquals(1, root.getRowCount());
      ListVector items = (ListVector) root.getVector("items");
      StructVector itemStruct = (StructVector) items.getDataVector();
      BigIntVector idVec = (BigIntVector) itemStruct.getChild("id");
      // Verify first, middle, last ids
      assertEquals(0L, idVec.get(0));
      assertEquals((long)(n/2), idVec.get(n/2));
      assertEquals((long)(n-1), idVec.get(n - 1));
    }
  }
}
