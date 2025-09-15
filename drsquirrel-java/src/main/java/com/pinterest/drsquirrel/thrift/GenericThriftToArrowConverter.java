package com.pinterest.drsquirrel.thrift;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;

/**
 * Generic Thrift-to-Arrow converter using Thrift Java metadata.
 * - Supports Binary protocol payloads
 * - Maps common Thrift types to Arrow equivalents
 * - Builds nested Arrow types (struct/list/map)
 */
public final class GenericThriftToArrowConverter implements ThriftToArrowConverter {

  @Override
  public Schema toArrowSchema(Class<? extends TBase<?, ?>> thriftClass) {
    Map<?, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(thriftClass);
    List<Field> fields = new ArrayList<>();
    meta.entrySet().stream()
        .sorted(Comparator.comparingInt(e -> ((org.apache.thrift.TFieldIdEnum) e.getKey()).getThriftFieldId()))
        .forEach(e -> {
          FieldMetaData fmd = e.getValue();
          fields.add(toField(fmd.fieldName, fmd.valueMetaData));
        });
    return new Schema(fields);
  }

  private Field toField(String name, FieldValueMetaData vmd) {
    ArrowType dt = toArrowType(vmd);
    switch (vmd.type) {
      case org.apache.thrift.protocol.TType.LIST: {
        Field elem = toField("element", ((ListMetaData) vmd).elemMetaData);
        return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(elem));
      }
      case org.apache.thrift.protocol.TType.SET: {
        Field elem = toField("element", ((SetMetaData) vmd).elemMetaData);
        return new Field(name, FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(elem));
      }
      case org.apache.thrift.protocol.TType.MAP: {
        MapMetaData mmd = (MapMetaData) vmd;
        Field key = toField("key", mmd.keyMetaData);
        Field val = toField("value", mmd.valueMetaData);
        // Arrow Map is List<Struct<key, value>> with map metadata
        java.util.List<Field> children = java.util.Arrays.asList(key, val);
        Field entry = new Field("entries", FieldType.nullable(new ArrowType.Struct()), children);
        return new Field(name, new FieldType(true, new ArrowType.Map(false), null), java.util.Arrays.asList(entry));
      }
      case org.apache.thrift.protocol.TType.STRUCT: {
        StructMetaData smd = (StructMetaData) vmd;
        Map<?, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(smd.structClass);
        List<Field> children = new ArrayList<>();
        meta.entrySet().stream()
            .sorted(Comparator.comparingInt(e -> ((org.apache.thrift.TFieldIdEnum) e.getKey()).getThriftFieldId()))
            .forEach(e -> {
              FieldMetaData fmd = e.getValue();
              children.add(toField(fmd.fieldName, fmd.valueMetaData));
            });
        return new Field(name, FieldType.nullable(new ArrowType.Struct()), children);
      }
      default: {
        return new Field(name, FieldType.nullable(dt), /*children*/ null);
      }
    }
  }

  private ArrowType toArrowType(FieldValueMetaData vmd) {
    switch (vmd.type) {
      case org.apache.thrift.protocol.TType.BOOL: return ArrowType.Bool.INSTANCE;
      case org.apache.thrift.protocol.TType.BYTE: return new ArrowType.Int(8, true);
      case org.apache.thrift.protocol.TType.I16: return new ArrowType.Int(32, true); // widen to int32 for uniformity
      case org.apache.thrift.protocol.TType.I32: return new ArrowType.Int(32, true);
      case org.apache.thrift.protocol.TType.I64: return new ArrowType.Int(64, true);
      case org.apache.thrift.protocol.TType.DOUBLE: return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE); // map to float32
      case org.apache.thrift.protocol.TType.STRING: return ArrowType.Utf8.INSTANCE;
      // COLLECTION and STRUCT handled by toField
      default: return ArrowType.Null.INSTANCE;
    }
  }

  @Override
  public VectorSchemaRoot convert(byte[] payload, Class<? extends TBase<?, ?>> thriftClass,
                                  BufferAllocator allocator) {
    return convert(Collections.singletonList(payload), thriftClass, allocator);
  }

  @Override
  public VectorSchemaRoot convert(List<byte[]> payloads, Class<? extends TBase<?, ?>> thriftClass,
                                  BufferAllocator allocator) {
    Schema schema = toArrowSchema(thriftClass);
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    // Prepare metadata order matching schema fields
    Map<?, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(thriftClass);
    List<Map.Entry<?, FieldMetaData>> ordered = new ArrayList<>(meta.entrySet());
    ordered.sort(Comparator.comparingInt(e -> ((org.apache.thrift.TFieldIdEnum) e.getKey()).getThriftFieldId()));

    // Prepare deserializer and ctor
    TDeserializer deser = new TDeserializer(new TBinaryProtocol.Factory());
    Constructor<?> ctor;
    try {
      ctor = thriftClass.getDeclaredConstructor();
      ctor.setAccessible(true);
    } catch (Exception e) {
      root.close();
      throw new RuntimeException("No default constructor for Thrift class: " + thriftClass, e);
    }

    for (int row = 0; row < payloads.size(); row++) {
      TBase<?, ?> obj;
      try {
        obj = (TBase<?, ?>) ctor.newInstance();
        deser.deserialize(obj, payloads.get(row));
      } catch (Exception e) {
        root.close();
        throw new RuntimeException("Failed to deserialize Thrift payload", e);
      }
      // Write each field into vectors
      List<FieldVector> vectors = root.getFieldVectors();
      for (int i = 0; i < ordered.size(); i++) {
        FieldMetaData fmd = ordered.get(i).getValue();
        Object val = ((TBase) obj).getFieldValue((org.apache.thrift.TFieldIdEnum) ordered.get(i).getKey());
        writeValue(vectors.get(i), fmd.valueMetaData, val, row);
      }
    }

    // Set row count for all top-level vectors
    root.setRowCount(payloads.size());
    for (FieldVector v : root.getFieldVectors()) {
      v.setValueCount(payloads.size());
    }
    return root;
  }

  private void writeValue(FieldVector vector, FieldValueMetaData vmd, Object value, int row) {
    if (value == null) {
      // leave as null (validity bit unset). We'll set vector value counts later.
      return;
    }
    switch (vmd.type) {
      case org.apache.thrift.protocol.TType.BOOL: {
        BitVector v = (BitVector) vector;
        v.setSafe(row, ((Boolean) value) ? 1 : 0);
        break;
      }
      case org.apache.thrift.protocol.TType.BYTE: {
        TinyIntVector v = (TinyIntVector) vector;
        v.setSafe(row, ((Byte) value).intValue());
        break;
      }
      case org.apache.thrift.protocol.TType.I16: {
        // stored as int32
        IntVector v = (IntVector) vector;
        v.setSafe(row, ((Short) value).intValue());
        break;
      }
      case org.apache.thrift.protocol.TType.I32: {
        IntVector v = (IntVector) vector;
        v.setSafe(row, ((Integer) value).intValue());
        break;
      }
      case org.apache.thrift.protocol.TType.I64: {
        BigIntVector v = (BigIntVector) vector;
        v.setSafe(row, ((Long) value).longValue());
        break;
      }
      case org.apache.thrift.protocol.TType.DOUBLE: {
        // mapped to float32
        Float4Vector v = (Float4Vector) vector;
        v.setSafe(row, ((Double) value).floatValue());
        break;
      }
      case org.apache.thrift.protocol.TType.STRING: {
        VarCharVector v = (VarCharVector) vector;
        if (value instanceof java.nio.ByteBuffer) {
          java.nio.ByteBuffer bb = (java.nio.ByteBuffer) value;
          byte[] buf = new byte[bb.remaining()];
          bb.slice().get(buf);
          v.setSafe(row, buf);
        } else {
          v.setSafe(row, ((String) value).getBytes(StandardCharsets.UTF_8));
        }
        break;
      }
      case org.apache.thrift.protocol.TType.LIST: {
        ListMetaData lmd = (ListMetaData) vmd;
        ListVector lv = (ListVector) vector;
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) value;
        int start = lv.startNewValue(row);
        FieldVector data = lv.getDataVector();
        // Current number of values already in data vector
        int offset = data.getValueCount();
        for (int i = 0; i < list.size(); i++) {
          writeElement(data, lmd.elemMetaData, list.get(i), offset + i);
        }
        data.setValueCount(offset + list.size());
        lv.endValue(row, list.size());
        break;
      }
      case org.apache.thrift.protocol.TType.SET: {
        // Treat set like list
        SetMetaData smd = (SetMetaData) vmd;
        ListVector lv = (ListVector) vector;
        @SuppressWarnings("unchecked")
        java.util.Set<Object> set = (java.util.Set<Object>) value;
        List<Object> list = new ArrayList<>(set);
        int start = lv.startNewValue(row);
        FieldVector data = lv.getDataVector();
        int offset = data.getValueCount();
        for (int i = 0; i < list.size(); i++) {
          writeElement(data, smd.elemMetaData, list.get(i), offset + i);
        }
        data.setValueCount(offset + list.size());
        lv.endValue(row, list.size());
        break;
      }
      case org.apache.thrift.protocol.TType.MAP: {
        MapMetaData mmd = (MapMetaData) vmd;
        MapVector mv = (MapVector) vector;
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) value;
        int start = mv.startNewValue(row);
        StructVector entry = (StructVector) mv.getDataVector();
        FieldVector keyVec = entry.getChild("key");
        FieldVector valVec = entry.getChild("value");
        int offset = entry.getValueCount();
        int i = 0;
        for (Map.Entry<Object, Object> e : map.entrySet()) {
          writeElement(keyVec, mmd.keyMetaData, e.getKey(), offset + i);
          writeElement(valVec, mmd.valueMetaData, e.getValue(), offset + i);
          i++;
        }
        entry.setValueCount(offset + i);
        mv.endValue(row, i);
        break;
      }
      case org.apache.thrift.protocol.TType.STRUCT: {
        StructMetaData smd = (StructMetaData) vmd;
        StructVector sv = (StructVector) vector;
        @SuppressWarnings("unchecked")
        TBase<?, ?> nested = (TBase<?, ?>) value;
        Map<?, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(smd.structClass);
        List<Map.Entry<?, FieldMetaData>> ordered = new ArrayList<>(meta.entrySet());
        ordered.sort(Comparator.comparingInt(e -> ((org.apache.thrift.TFieldIdEnum) e.getKey()).getThriftFieldId()));
        for (Map.Entry<?, FieldMetaData> e : ordered) {
          FieldVector child = sv.getChild(e.getValue().fieldName);
          Object childVal = ((TBase) nested).getFieldValue((org.apache.thrift.TFieldIdEnum) e.getKey());
          writeValue(child, e.getValue().valueMetaData, childVal, row);
        }
        break;
      }
      default: { break; }
    }
  }

  private void writeElement(FieldVector vector, FieldValueMetaData vmd, Object value, int index) {
    // Write element in the data vector of a list/map
    if (value == null) {
      return;
    }
    switch (vmd.type) {
      case org.apache.thrift.protocol.TType.BOOL: ((BitVector) vector).setSafe(index, ((Boolean) value) ? 1 : 0); break;
      case org.apache.thrift.protocol.TType.BYTE: ((TinyIntVector) vector).setSafe(index, ((Byte) value).intValue()); break;
      case org.apache.thrift.protocol.TType.I16: ((IntVector) vector).setSafe(index, ((Short) value).intValue()); break;
      case org.apache.thrift.protocol.TType.I32: ((IntVector) vector).setSafe(index, ((Integer) value)); break;
      case org.apache.thrift.protocol.TType.I64: ((BigIntVector) vector).setSafe(index, ((Long) value)); break;
      case org.apache.thrift.protocol.TType.DOUBLE: ((Float4Vector) vector).setSafe(index, ((Double) value).floatValue()); break;
      case org.apache.thrift.protocol.TType.STRING: {
        VarCharVector v = (VarCharVector) vector;
        if (value instanceof java.nio.ByteBuffer) {
          java.nio.ByteBuffer bb = (java.nio.ByteBuffer) value;
          byte[] buf = new byte[bb.remaining()];
          bb.slice().get(buf);
          v.setSafe(index, buf);
        } else {
          v.setSafe(index, ((String) value).getBytes(StandardCharsets.UTF_8));
        }
        break;
      }
      case org.apache.thrift.protocol.TType.STRUCT: {
        StructMetaData smd = (StructMetaData) vmd;
        StructVector sv = (StructVector) vector;
        @SuppressWarnings("unchecked")
        TBase<?, ?> nested = (TBase<?, ?>) value;
        Map<?, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(smd.structClass);
        List<Map.Entry<?, FieldMetaData>> ordered = new ArrayList<>(meta.entrySet());
        ordered.sort(Comparator.comparingInt(e -> ((org.apache.thrift.TFieldIdEnum) e.getKey()).getThriftFieldId()));
        for (Map.Entry<?, FieldMetaData> e : ordered) {
          FieldVector child = sv.getChild(e.getValue().fieldName);
          Object childVal = ((TBase) nested).getFieldValue((org.apache.thrift.TFieldIdEnum) e.getKey());
          writeElement(child, e.getValue().valueMetaData, childVal, index);
        }
        break;
      }
      case org.apache.thrift.protocol.TType.LIST: {
        ListMetaData lmd = (ListMetaData) vmd;
        ListVector lv = (ListVector) vector;
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) value;
        int start = lv.startNewValue(index);
        FieldVector data = lv.getDataVector();
        int offset = data.getValueCount();
        for (int i = 0; i < list.size(); i++) {
          writeElement(data, lmd.elemMetaData, list.get(i), offset + i);
        }
        data.setValueCount(offset + list.size());
        lv.endValue(index, list.size());
        break;
      }
      case org.apache.thrift.protocol.TType.SET: {
        SetMetaData smd = (SetMetaData) vmd;
        ListVector lv = (ListVector) vector;
        @SuppressWarnings("unchecked")
        java.util.Set<Object> set = (java.util.Set<Object>) value;
        List<Object> list = new ArrayList<>(set);
        int start = lv.startNewValue(index);
        FieldVector data = lv.getDataVector();
        int offset = data.getValueCount();
        for (int i = 0; i < list.size(); i++) {
          writeElement(data, smd.elemMetaData, list.get(i), offset + i);
        }
        data.setValueCount(offset + list.size());
        lv.endValue(index, list.size());
        break;
      }
      case org.apache.thrift.protocol.TType.MAP: {
        MapMetaData mmd = (MapMetaData) vmd;
        MapVector mv = (MapVector) vector;
        @SuppressWarnings("unchecked")
        Map<Object, Object> map = (Map<Object, Object>) value;
        int start = mv.startNewValue(index);
        StructVector entry = (StructVector) mv.getDataVector();
        FieldVector keyVec = entry.getChild("key");
        FieldVector valVec = entry.getChild("value");
        int offset = entry.getValueCount();
        int i = 0;
        for (Map.Entry<Object, Object> e : map.entrySet()) {
          writeElement(keyVec, mmd.keyMetaData, e.getKey(), offset + i);
          writeElement(valVec, mmd.valueMetaData, e.getValue(), offset + i);
          i++;
        }
        entry.setValueCount(offset + i);
        mv.endValue(index, i);
        break;
      }
      default: { /* leave null */ }
    }
  }
}
