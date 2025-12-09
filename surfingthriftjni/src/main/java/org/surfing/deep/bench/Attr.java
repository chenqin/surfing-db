package org.surfing.deep.bench;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;

public class Attr implements TBase<Attr, Attr._Fields> {
  public String key;
  public String val;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    KEY((short)1, "key"),
    VAL((short)2, "val");
    private final short thriftId; private final String fieldName;
    _Fields(short thriftId, String fieldName) { this.thriftId = thriftId; this.fieldName = fieldName; }
    @Override public short getThriftFieldId() { return thriftId; }
    @Override public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int id) { return id == 1 ? KEY : id == 2 ? VAL : null; }
  }

  public static final java.util.Map<_Fields, FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, FieldMetaData> m = new java.util.EnumMap<>(_Fields.class);
    m.put(_Fields.KEY, new FieldMetaData("key", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    m.put(_Fields.VAL, new FieldMetaData("val", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    FieldMetaData.addStructMetaDataMap(Attr.class, (java.util.Map) m);
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
          if (f.type == TType.STRING) key = iprot.readString();
          else TProtocolUtil.skip(iprot, f.type);
          break;
        case 2:
          if (f.type == TType.STRING) val = iprot.readString();
          else TProtocolUtil.skip(iprot, f.type);
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
    oprot.writeStructBegin(new TStruct("Attr"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public Attr deepCopy() { Attr a = new Attr(); a.key = key; a.val = val; return a; }
  @Override public void clear() { key = null; val = null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) { switch (field) { case KEY: return key; case VAL: return val; default: return null; } }
  @Override @SuppressWarnings("unchecked")
  public void setFieldValue(_Fields field, Object value) { switch (field) { case KEY: key = (String) value; break; case VAL: val = (String) value; break; } }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }
  @Override public int compareTo(Attr other) { String a = key == null ? "" : key; String b = other.key == null ? "" : other.key; return a.compareTo(b); }
}
