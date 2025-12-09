package org.surfing.drsquirrel.thrift;

import java.lang.reflect.Constructor;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;

/**
 * Convert an Arrow VectorSchemaRoot (RecordBatch) into a list of Thrift Binary payloads
 * for the provided generated Thrift TBase class.
 *
 * Supported types: bool, byte, i16 (as int32), i32, i64, double (from float32),
 * string, list<primitive|string>, set<primitive|string>, map<string, primitive|string>, struct.
 */
public final class GenericArrowToThriftConverter {

  public List<byte[]> convert(VectorSchemaRoot root, Class<? extends TBase<?, ?>> thriftClass) {
    try {
      @SuppressWarnings("unchecked")
      Constructor<? extends TBase<?, ?>> ctor = (Constructor<? extends TBase<?, ?>>) thriftClass.getDeclaredConstructor();
      ctor.setAccessible(true);

      Map<? extends org.apache.thrift.TFieldIdEnum, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(thriftClass);
      // Map field name -> (key enum, metadata)
      Map<String, Map.Entry<? extends org.apache.thrift.TFieldIdEnum, FieldMetaData>> byName = new HashMap<>();
      for (Map.Entry<? extends org.apache.thrift.TFieldIdEnum, FieldMetaData> e : meta.entrySet()) {
        byName.put(e.getValue().fieldName, e);
      }

      List<byte[]> out = new ArrayList<>(root.getRowCount());
      TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
      List<FieldVector> vectors = root.getFieldVectors();
      List<Field> fields = root.getSchema().getFields();

      for (int row = 0; row < root.getRowCount(); row++) {
        TBase<?, ?> obj = ctor.newInstance();
        for (int i = 0; i < fields.size(); i++) {
          Field f = fields.get(i);
          Map.Entry<? extends org.apache.thrift.TFieldIdEnum, FieldMetaData> entry = byName.get(f.getName());
          if (entry == null) continue; // unknown field
          FieldVector vec = vectors.get(i);
          if (vec.isNull(row)) continue;
          FieldValueMetaData vmd = entry.getValue().valueMetaData;
          Object value = readValue(vec, vmd, row);
          if (value != null) ((TBase) obj).setFieldValue(entry.getKey(), value);
        }
        out.add(ser.serialize(obj));
      }
      return out;
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize Arrow to Thrift", e);
    }
  }

  private Object readValue(FieldVector vec, FieldValueMetaData vmd, int row) throws Exception {
    switch (vmd.type) {
      case org.apache.thrift.protocol.TType.BOOL: {
        BitVector v = (BitVector) vec; return v.get(row) == 0 ? Boolean.FALSE : Boolean.TRUE;
      }
      case org.apache.thrift.protocol.TType.BYTE: {
        TinyIntVector v = (TinyIntVector) vec; return Byte.valueOf((byte) v.get(row));
      }
      case org.apache.thrift.protocol.TType.I16: {
        IntVector v = (IntVector) vec; return Short.valueOf((short) v.get(row));
      }
      case org.apache.thrift.protocol.TType.I32: {
        IntVector v = (IntVector) vec; return Integer.valueOf(v.get(row));
      }
      case org.apache.thrift.protocol.TType.I64: {
        BigIntVector v = (BigIntVector) vec; return Long.valueOf(v.get(row));
      }
      case org.apache.thrift.protocol.TType.DOUBLE: {
        // Our Arrow mapping uses float32 for thrift double; widen back to double
        Float4Vector v = (Float4Vector) vec; return Double.valueOf(v.get(row));
      }
      case org.apache.thrift.protocol.TType.STRING: {
        VarCharVector v = (VarCharVector) vec; byte[] bytes = v.get(row); return Text.decode(bytes); // UTF-8
      }
      case org.apache.thrift.protocol.TType.LIST: {
        ListMetaData lmd = (ListMetaData) vmd;
        ListVector lv = (ListVector) vec;
        int start = lv.getOffsetBuffer().getInt((long) row * ListVector.OFFSET_WIDTH);
        int end = lv.getOffsetBuffer().getInt((long) (row + 1) * ListVector.OFFSET_WIDTH);
        FieldVector data = lv.getDataVector();
        List<Object> out = new ArrayList<>(Math.max(0, end - start));
        for (int i = start; i < end; i++) out.add(readElement(data, lmd.elemMetaData, i));
        return out;
      }
      case org.apache.thrift.protocol.TType.SET: {
        SetMetaData smd = (SetMetaData) vmd;
        ListVector lv = (ListVector) vec;
        int start = lv.getOffsetBuffer().getInt((long) row * ListVector.OFFSET_WIDTH);
        int end = lv.getOffsetBuffer().getInt((long) (row + 1) * ListVector.OFFSET_WIDTH);
        FieldVector data = lv.getDataVector();
        java.util.Set<Object> out = new java.util.HashSet<>(Math.max(0, end - start));
        for (int i = start; i < end; i++) out.add(readElement(data, smd.elemMetaData, i));
        return out;
      }
      case org.apache.thrift.protocol.TType.MAP: {
        MapMetaData mmd = (MapMetaData) vmd;
        MapVector mv = (MapVector) vec;
        int start = mv.getOffsetBuffer().getInt((long) row * MapVector.OFFSET_WIDTH);
        int end = mv.getOffsetBuffer().getInt((long) (row + 1) * MapVector.OFFSET_WIDTH);
        StructVector entry = (StructVector) mv.getDataVector();
        FieldVector keyVec = entry.getChild("key");
        FieldVector valVec = entry.getChild("value");
        Map<Object, Object> out = new HashMap<>(Math.max(0, end - start));
        for (int i = start; i < end; i++) {
          Object k = readElement(keyVec, mmd.keyMetaData, i);
          Object v = readElement(valVec, mmd.valueMetaData, i);
          out.put(k, v);
        }
        return out;
      }
      case org.apache.thrift.protocol.TType.STRUCT: {
        StructMetaData smd = (StructMetaData) vmd;
        StructVector sv = (StructVector) vec;
        @SuppressWarnings("unchecked")
        Constructor<? extends TBase<?, ?>> ctor = (Constructor<? extends TBase<?, ?>>) smd.structClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        TBase<?, ?> nested = ctor.newInstance();
        Map<? extends org.apache.thrift.TFieldIdEnum, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(smd.structClass);
        for (Map.Entry<? extends org.apache.thrift.TFieldIdEnum, FieldMetaData> e : meta.entrySet()) {
          FieldVector child = sv.getChild(e.getValue().fieldName);
          if (child == null) continue;
          if (child.isNull(row)) continue;
          Object val = readValue(child, e.getValue().valueMetaData, row);
          if (val != null) ((TBase) nested).setFieldValue(e.getKey(), val);
        }
        return nested;
      }
      default: return null;
    }
  }

  private Object readElement(FieldVector data, FieldValueMetaData vmd, int index) throws Exception {
    switch (vmd.type) {
      case org.apache.thrift.protocol.TType.BOOL: return ((BitVector) data).get(index) != 0;
      case org.apache.thrift.protocol.TType.BYTE: return (byte) ((TinyIntVector) data).get(index);
      case org.apache.thrift.protocol.TType.I16: return (short) ((IntVector) data).get(index);
      case org.apache.thrift.protocol.TType.I32: return ((IntVector) data).get(index);
      case org.apache.thrift.protocol.TType.I64: return ((BigIntVector) data).get(index);
      case org.apache.thrift.protocol.TType.DOUBLE: return (double) ((Float4Vector) data).get(index);
      case org.apache.thrift.protocol.TType.STRING: {
        VarCharVector v = (VarCharVector) data; return Text.decode(v.get(index));
      }
      case org.apache.thrift.protocol.TType.STRUCT: {
        StructMetaData smd = (StructMetaData) vmd;
        StructVector sv = (StructVector) data;
        @SuppressWarnings("unchecked")
        Constructor<? extends TBase<?, ?>> ctor = (Constructor<? extends TBase<?, ?>>) smd.structClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        TBase<?, ?> nested = ctor.newInstance();
        Map<? extends org.apache.thrift.TFieldIdEnum, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(smd.structClass);
        for (Map.Entry<? extends org.apache.thrift.TFieldIdEnum, FieldMetaData> e : meta.entrySet()) {
          FieldVector child = sv.getChild(e.getValue().fieldName);
          if (child == null) continue;
          if (child.isNull(index)) continue;
          Object val = readElement(child, e.getValue().valueMetaData, index);
          if (val != null) ((TBase) nested).setFieldValue(e.getKey(), val);
        }
        return nested;
      }
      case org.apache.thrift.protocol.TType.LIST: {
        ListMetaData lmd = (ListMetaData) vmd;
        ListVector lv = (ListVector) data;
        int start = lv.getOffsetBuffer().getInt((long) index * ListVector.OFFSET_WIDTH);
        int end = lv.getOffsetBuffer().getInt((long) (index + 1) * ListVector.OFFSET_WIDTH);
        FieldVector dv = lv.getDataVector();
        List<Object> out = new ArrayList<>(Math.max(0, end - start));
        for (int i = start; i < end; i++) out.add(readElement(dv, lmd.elemMetaData, i));
        return out;
      }
      case org.apache.thrift.protocol.TType.SET: {
        SetMetaData smd = (SetMetaData) vmd;
        ListVector lv = (ListVector) data;
        int start = lv.getOffsetBuffer().getInt((long) index * ListVector.OFFSET_WIDTH);
        int end = lv.getOffsetBuffer().getInt((long) (index + 1) * ListVector.OFFSET_WIDTH);
        FieldVector dv = lv.getDataVector();
        java.util.Set<Object> out = new java.util.HashSet<>(Math.max(0, end - start));
        for (int i = start; i < end; i++) out.add(readElement(dv, smd.elemMetaData, i));
        return out;
      }
      case org.apache.thrift.protocol.TType.MAP: {
        MapMetaData mmd = (MapMetaData) vmd;
        MapVector mv = (MapVector) data;
        int start = mv.getOffsetBuffer().getInt((long) index * MapVector.OFFSET_WIDTH);
        int end = mv.getOffsetBuffer().getInt((long) (index + 1) * MapVector.OFFSET_WIDTH);
        StructVector entry = (StructVector) mv.getDataVector();
        FieldVector keyVec = entry.getChild("key");
        FieldVector valVec = entry.getChild("value");
        Map<Object, Object> out = new HashMap<>(Math.max(0, end - start));
        for (int i = start; i < end; i++) {
          Object k = readElement(keyVec, mmd.keyMetaData, i);
          Object v = readElement(valVec, mmd.valueMetaData, i);
          out.put(k, v);
        }
        return out;
      }
      default: return null;
    }
  }
}
