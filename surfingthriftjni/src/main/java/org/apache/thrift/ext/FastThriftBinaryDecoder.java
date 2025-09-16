package org.apache.thrift.ext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.meta_data.FieldMetaData;

public final class FastThriftBinaryDecoder {
  public static VectorSchemaRoot convert(List<byte[]> payloads,
                                         Class<? extends TBase<?, ?>> thriftClass,
                                         BufferAllocator allocator) {
    Map<?, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(thriftClass);
    List<Map.Entry<?, FieldMetaData>> ordered = new ArrayList<>(meta.entrySet());
    ordered.sort(Comparator.comparingInt(e -> ((org.apache.thrift.TFieldIdEnum) e.getKey()).getThriftFieldId()));
    for (Map.Entry<?, FieldMetaData> e : ordered) {
      byte t = e.getValue().valueMetaData.type;
      if (t == org.apache.thrift.protocol.TType.LIST || t == org.apache.thrift.protocol.TType.SET || t == org.apache.thrift.protocol.TType.MAP || t == org.apache.thrift.protocol.TType.STRUCT) {
        return new GenericThriftToArrowConverter().convert(payloads, thriftClass, allocator);
      }
    }
    List<Field> fields = new ArrayList<>(ordered.size());
    for (Map.Entry<?, FieldMetaData> e : ordered) fields.add(new Field(e.getValue().fieldName, FieldType.nullable(toArrowType(e.getValue().valueMetaData.type)), null));
    Schema schema = new Schema(fields);
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    List<org.apache.arrow.vector.FieldVector> vecs = root.getFieldVectors();
    for (int row = 0; row < payloads.size(); row++) {
      byte[] data = payloads.get(row);
      TMemoryBuffer buf = new TMemoryBuffer(Math.max(128, data.length + 8));
      buf.write(data, 0, data.length);
      TBinaryProtocol iprot = new TBinaryProtocol(buf);
      try {
        iprot.readStructBegin();
        while (true) {
          org.apache.thrift.protocol.TField f = iprot.readFieldBegin();
          if (f.type == org.apache.thrift.protocol.TType.STOP) break;
          int idx = indexOf(ordered, f.id);
          if (idx >= 0) writeValue(vecs.get(idx), f.type, iprot, row);
          else org.apache.thrift.protocol.TProtocolUtil.skip(iprot, f.type);
          iprot.readFieldEnd();
        }
        iprot.readStructEnd();
      } catch (Exception ex) {
        root.close(); throw new RuntimeException("Fast decode failed", ex);
      }
    }
    root.setRowCount(payloads.size()); for (org.apache.arrow.vector.FieldVector v : vecs) v.setValueCount(payloads.size());
    return root;
  }
  private static int indexOf(List<Map.Entry<?, FieldMetaData>> ordered, short thriftId) { for (int i=0;i<ordered.size();i++) if (((org.apache.thrift.TFieldIdEnum) ordered.get(i).getKey()).getThriftFieldId()==thriftId) return i; return -1; }
  private static ArrowType toArrowType(byte ttype) {
    switch (ttype) {
      case org.apache.thrift.protocol.TType.BOOL: return ArrowType.Bool.INSTANCE;
      case org.apache.thrift.protocol.TType.BYTE: return new ArrowType.Int(8, true);
      case org.apache.thrift.protocol.TType.I16: return new ArrowType.Int(32, true);
      case org.apache.thrift.protocol.TType.I32: return new ArrowType.Int(32, true);
      case org.apache.thrift.protocol.TType.I64: return new ArrowType.Int(64, true);
      case org.apache.thrift.protocol.TType.DOUBLE: return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
      case org.apache.thrift.protocol.TType.STRING: return ArrowType.Utf8.INSTANCE;
      default: return ArrowType.Null.INSTANCE;
    }
  }
  private static void writeValue(org.apache.arrow.vector.FieldVector vector, byte ttype, TBinaryProtocol iprot, int row) throws Exception {
    switch (ttype) {
      case org.apache.thrift.protocol.TType.BOOL: ((BitVector) vector).setSafe(row, iprot.readBool()?1:0); break;
      case org.apache.thrift.protocol.TType.BYTE: ((TinyIntVector) vector).setSafe(row, iprot.readByte()); break;
      case org.apache.thrift.protocol.TType.I16: ((IntVector) vector).setSafe(row, (int) iprot.readI16()); break;
      case org.apache.thrift.protocol.TType.I32: ((IntVector) vector).setSafe(row, iprot.readI32()); break;
      case org.apache.thrift.protocol.TType.I64: ((BigIntVector) vector).setSafe(row, iprot.readI64()); break;
      case org.apache.thrift.protocol.TType.DOUBLE: ((Float4Vector) vector).setSafe(row, (float) iprot.readDouble()); break;
      case org.apache.thrift.protocol.TType.STRING: ((VarCharVector) vector).setSafe(row, iprot.readString().getBytes(java.nio.charset.StandardCharsets.UTF_8)); break;
      default: org.apache.thrift.protocol.TProtocolUtil.skip(iprot, ttype); break;
    }
  }
}

