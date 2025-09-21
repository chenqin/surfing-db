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

public class Bundle implements TBase<Bundle, Bundle._Fields> {
  public List<Reading> items;
  public Map<String, List<String>> extras;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ITEMS((short)1, "items"),
    EXTRAS((short)2, "extras");
    private final short thriftId; private final String fieldName;
    _Fields(short thriftId, String fieldName) { this.thriftId = thriftId; this.fieldName = fieldName; }
    @Override public short getThriftFieldId() { return thriftId; }
    @Override public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int id) { return id == 1 ? ITEMS : id == 2 ? EXTRAS : null; }
  }

  public static final java.util.Map<_Fields, FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, FieldMetaData> m = new java.util.EnumMap<>(_Fields.class);
    m.put(_Fields.ITEMS, new FieldMetaData("items", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new StructMetaData(TType.STRUCT, Reading.class))));
    m.put(_Fields.EXTRAS, new FieldMetaData("extras", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new ListMetaData(TType.LIST, new FieldValueMetaData(TType.STRING)))));
    FieldMetaData.addStructMetaDataMap(Bundle.class, (java.util.Map) m);
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
          if (f.type == TType.LIST) {
            TList tl = iprot.readListBegin();
            items = new ArrayList<>(tl.size);
            for (int i = 0; i < tl.size; i++) { Reading r = new Reading(); r.read(iprot); items.add(r); }
            iprot.readListEnd();
          } else TProtocolUtil.skip(iprot, f.type);
          break;
        case 2:
          if (f.type == TType.MAP) {
            TMap tm = iprot.readMapBegin();
            extras = new HashMap<>(tm.size);
            for (int i = 0; i < tm.size; i++) {
              String key = iprot.readString();
              TList ll = iprot.readListBegin();
              List<String> vals = new ArrayList<>(ll.size);
              for (int j = 0; j < ll.size; j++) vals.add(iprot.readString());
              iprot.readListEnd();
              extras.put(key, vals);
            }
            iprot.readMapEnd();
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
    oprot.writeStructBegin(new TStruct("Bundle"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public Bundle deepCopy() { Bundle b = new Bundle(); b.items = items == null ? null : new ArrayList<>(items); b.extras = extras == null ? null : new HashMap<>(extras); return b; }
  @Override public void clear() { items = null; extras = null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) { switch (field) { case ITEMS: return items; case EXTRAS: return extras; default: return null; } }
  @Override @SuppressWarnings("unchecked")
  public void setFieldValue(_Fields field, Object value) { switch (field) { case ITEMS: items = (List<Reading>) value; break; case EXTRAS: extras = (Map<String, List<String>>) value; break; } }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }
  @Override public int compareTo(Bundle other) { int a = items == null ? 0 : items.size(); int b = other.items == null ? 0 : other.items.size(); return Integer.compare(a, b); }
}
