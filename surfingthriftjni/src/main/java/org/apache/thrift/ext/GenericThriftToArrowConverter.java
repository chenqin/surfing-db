package org.apache.thrift.ext;

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
        ArrowType keyType = toArrowType(mmd.keyMetaData);
        ArrowType valType = toArrowType(mmd.valueMetaData);
        Field key = new Field("key", new FieldType(false, keyType, null), null);
        Field val = new Field("value", new FieldType(true, valType, null), null);
        java.util.List<Field> kv = java.util.Arrays.asList(key, val);
        Field entry = new Field("entries", new FieldType(false, new ArrowType.Struct(), null), kv);
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
        return new Field(name, FieldType.nullable(dt), null);
      }
    }
  }

  private ArrowType toArrowType(FieldValueMetaData vmd) {
    switch (vmd.type) {
      case org.apache.thrift.protocol.TType.BOOL: return ArrowType.Bool.INSTANCE;
      case org.apache.thrift.protocol.TType.BYTE: return new ArrowType.Int(8, true);
      case org.apache.thrift.protocol.TType.I16: return new ArrowType.Int(32, true);
      case org.apache.thrift.protocol.TType.I32: return new ArrowType.Int(32, true);
      case org.apache.thrift.protocol.TType.I64: return new ArrowType.Int(64, true);
      case org.apache.thrift.protocol.TType.DOUBLE: return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case org.apache.thrift.protocol.TType.STRING: return ArrowType.Utf8.INSTANCE;
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

    Map<?, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(thriftClass);
    List<Map.Entry<?, FieldMetaData>> ordered = new ArrayList<>(meta.entrySet());
    ordered.sort(Comparator.comparingInt(e -> ((org.apache.thrift.TFieldIdEnum) e.getKey()).getThriftFieldId()));

    final TDeserializer deser = new TDeserializer(new TBinaryProtocol.Factory());
    final TBase<?, ?> obj;
    try {
      Constructor<?> ctor = thriftClass.getDeclaredConstructor();
      ctor.setAccessible(true);
      obj = (TBase<?, ?>) ctor.newInstance();
    } catch (Exception e) {
      root.close();
      throw new RuntimeException("Failed to construct Thrift class: " + thriftClass, e);
    }

    final java.util.List<FieldVector> vecList = root.getFieldVectors();
    final FieldVector[] vectors = vecList.toArray(new FieldVector[0]);
    final FieldValueMetaData[] vmd = new FieldValueMetaData[ordered.size()];
    final org.apache.thrift.TFieldIdEnum[] keys = new org.apache.thrift.TFieldIdEnum[ordered.size()];
    for (int i = 0; i < ordered.size(); i++) {
      vmd[i] = ordered.get(i).getValue().valueMetaData;
      keys[i] = (org.apache.thrift.TFieldIdEnum) ordered.get(i).getKey();
    }

    for (int row = 0; row < payloads.size(); row++) {
      try {
        obj.clear();
        deser.deserialize(obj, payloads.get(row));
      } catch (Exception e) {
        root.close();
        throw new RuntimeException("Failed to deserialize Thrift payload", e);
      }
      for (int i = 0; i < vectors.length; i++) {
        Object val = ((TBase) obj).getFieldValue(keys[i]);
        writeValue(vectors[i], vmd[i], val, row);
      }
    }

    root.setRowCount(payloads.size());
    for (FieldVector v : root.getFieldVectors()) v.setValueCount(payloads.size());
    return root;
  }

  private void writeValue(FieldVector vector, FieldValueMetaData vmd, Object value, int row) {
    if (value == null) return;
    switch (vmd.type) {
      case org.apache.thrift.protocol.TType.BOOL: ((BitVector) vector).setSafe(row, ((Boolean) value) ? 1 : 0); break;
      case org.apache.thrift.protocol.TType.BYTE: ((TinyIntVector) vector).setSafe(row, ((Byte) value).intValue()); break;
      case org.apache.thrift.protocol.TType.I16: ((IntVector) vector).setSafe(row, ((Short) value).intValue()); break;
      case org.apache.thrift.protocol.TType.I32: ((IntVector) vector).setSafe(row, ((Integer) value)); break;
      case org.apache.thrift.protocol.TType.I64: ((BigIntVector) vector).setSafe(row, ((Long) value)); break;
      case org.apache.thrift.protocol.TType.DOUBLE: ((Float4Vector) vector).setSafe(row, ((Double) value).floatValue()); break;
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
        @SuppressWarnings("unchecked") List<Object> list = (List<Object>) value;
        int start = lv.startNewValue(row);
        FieldVector data = ensureListDataVector(lv, lmd.elemMetaData);
        int offset = data.getValueCount();
        for (int i = 0; i < list.size(); i++) writeElement(data, lmd.elemMetaData, list.get(i), offset + i);
        data.setValueCount(offset + list.size());
        lv.endValue(row, list.size());
        break;
      }
      case org.apache.thrift.protocol.TType.SET: {
        SetMetaData smd = (SetMetaData) vmd;
        ListVector lv = (ListVector) vector;
        @SuppressWarnings("unchecked") java.util.Set<Object> set = (java.util.Set<Object>) value;
        List<Object> list = new ArrayList<>(set);
        int start = lv.startNewValue(row);
        FieldVector data = ensureListDataVector(lv, smd.elemMetaData);
        int offset = data.getValueCount();
        for (int i = 0; i < list.size(); i++) writeElement(data, smd.elemMetaData, list.get(i), offset + i);
        data.setValueCount(offset + list.size());
        lv.endValue(row, list.size());
        break;
      }
      case org.apache.thrift.protocol.TType.MAP: {
        MapMetaData mmd = (MapMetaData) vmd;
        MapVector mv = (MapVector) vector;
        @SuppressWarnings("unchecked") Map<Object, Object> map = (Map<Object, Object>) value;
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
        @SuppressWarnings("unchecked") TBase<?, ?> nested = (TBase<?, ?>) value;
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
    if (value == null) return;
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
        @SuppressWarnings("unchecked") TBase<?, ?> nested = (TBase<?, ?>) value;
        Map<?, FieldMetaData> meta = FieldMetaData.getStructMetaDataMap(smd.structClass);
        List<Map.Entry<?, FieldMetaData>> order = new ArrayList<>(meta.entrySet());
        order.sort(Comparator.comparingInt(e -> ((org.apache.thrift.TFieldIdEnum) e.getKey()).getThriftFieldId()));
        for (Map.Entry<?, FieldMetaData> e : order) {
          FieldVector child = sv.getChild(e.getValue().fieldName);
          Object childVal = ((TBase) nested).getFieldValue((org.apache.thrift.TFieldIdEnum) e.getKey());
          writeElement(child, e.getValue().valueMetaData, childVal, index);
        }
        break;
      }
      case org.apache.thrift.protocol.TType.LIST: {
        ListMetaData lmd = (ListMetaData) vmd;
        ListVector lv = (ListVector) vector;
        @SuppressWarnings("unchecked") List<Object> list = (List<Object>) value;
        int start = lv.startNewValue(index);
        FieldVector data = ensureListDataVector(lv, lmd.elemMetaData);
        int offset = data.getValueCount();
        for (int i = 0; i < list.size(); i++) writeElement(data, lmd.elemMetaData, list.get(i), offset + i);
        data.setValueCount(offset + list.size());
        lv.endValue(index, list.size());
        break;
      }
      case org.apache.thrift.protocol.TType.SET: {
        SetMetaData smd = (SetMetaData) vmd;
        ListVector lv = (ListVector) vector;
        @SuppressWarnings("unchecked") java.util.Set<Object> set = (java.util.Set<Object>) value;
        List<Object> list = new ArrayList<>(set);
        int start = lv.startNewValue(index);
        FieldVector data = ensureListDataVector(lv, smd.elemMetaData);
        int offset = data.getValueCount();
        for (int i = 0; i < list.size(); i++) writeElement(data, smd.elemMetaData, list.get(i), offset + i);
        data.setValueCount(offset + list.size());
        lv.endValue(index, list.size());
        break;
      }
      case org.apache.thrift.protocol.TType.MAP: {
        MapMetaData mmd = (MapMetaData) vmd;
        MapVector mv = (MapVector) vector;
        @SuppressWarnings("unchecked") Map<Object, Object> map = (Map<Object, Object>) value;
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
      default: { }
    }
  }

  private FieldVector ensureListDataVector(ListVector lv, FieldValueMetaData elemMeta) {
    FieldVector data = lv.getDataVector();
    if (!(data instanceof org.apache.arrow.vector.NullVector)) return data;
    ArrowType childType = toArrowType(elemMeta);
    FieldType ft;
    switch (elemMeta.type) {
      case org.apache.thrift.protocol.TType.LIST: ft = FieldType.nullable(ArrowType.List.INSTANCE); break;
      case org.apache.thrift.protocol.TType.MAP: ft = new FieldType(true, new ArrowType.Map(false), null); break;
      case org.apache.thrift.protocol.TType.STRUCT: ft = FieldType.nullable(new ArrowType.Struct()); break;
      default: ft = FieldType.nullable(childType);
    }
    org.apache.arrow.vector.ValueVector vv = lv.addOrGetVector(ft).getVector();
    FieldVector newData = (FieldVector) vv; newData.allocateNew();
    return newData;
  }
}

