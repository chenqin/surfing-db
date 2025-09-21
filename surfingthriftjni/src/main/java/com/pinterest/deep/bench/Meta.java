package com.pinterest.deep.bench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.StructMetaData;

public class Meta implements TBase<Meta, Meta._Fields> {
  public Map<String, String> labels;
  public List<Attr> kvs;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    LABELS((short)1, "labels"),
    KVS((short)2, "kvs");
    private final short thriftId; private final String fieldName;
    _Fields(short thriftId, String fieldName) { this.thriftId = thriftId; this.fieldName = fieldName; }
    @Override public short getThriftFieldId() { return thriftId; }
    @Override public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int id) { return id == 1 ? LABELS : id == 2 ? KVS : null; }
  }

  public static final java.util.Map<_Fields, FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, FieldMetaData> m = new java.util.EnumMap<>(_Fields.class);
    m.put(_Fields.LABELS, new FieldMetaData("labels", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new FieldValueMetaData(TType.STRING))));
    m.put(_Fields.KVS, new FieldMetaData("kvs", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new StructMetaData(TType.STRUCT, Attr.class))));
    FieldMetaData.addStructMetaDataMap(Meta.class, (java.util.Map) m);
    metaDataMap = m;
  }

  @Override
  public void read(TProtocol iprot) throws TException {
    iprot.readStructBegin();
    while (true) {
      TField f = iprot.readFieldBegin();
      if (f.type == TType.STOP) break;
      switch (f.id) {
        case 1:
          if (f.type == TType.MAP) {
            TMap tm = iprot.readMapBegin();
            labels = new HashMap<>(tm.size);
            for (int i = 0; i < tm.size; i++) { labels.put(iprot.readString(), iprot.readString()); }
            iprot.readMapEnd();
          } else TProtocolUtil.skip(iprot, f.type);
          break;
        case 2:
          if (f.type == TType.LIST) {
            TList tl = iprot.readListBegin();
            kvs = new ArrayList<>(tl.size);
            for (int i = 0; i < tl.size; i++) { Attr a = new Attr(); a.read(iprot); kvs.add(a); }
            iprot.readListEnd();
          } else TProtocolUtil.skip(iprot, f.type);
          break;
        default:
          TProtocolUtil.skip(iprot, f.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  @Override
  public void write(TProtocol oprot) throws TException {
    oprot.writeStructBegin(new TStruct("Meta"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public Meta deepCopy() { Meta m = new Meta(); m.labels = labels == null ? null : new HashMap<>(labels); m.kvs = kvs == null ? null : new ArrayList<>(kvs); return m; }
  @Override public void clear() { labels = null; kvs = null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) { switch (field) { case LABELS: return labels; case KVS: return kvs; default: return null; } }
  @Override @SuppressWarnings("unchecked")
  public void setFieldValue(_Fields field, Object value) { switch (field) { case LABELS: labels = (Map<String, String>) value; break; case KVS: kvs = (List<Attr>) value; break; } }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }
  @Override public int compareTo(Meta other) { int a = labels == null ? 0 : labels.size(); int b = other.labels == null ? 0 : other.labels.size(); return Integer.compare(a, b); }
}
