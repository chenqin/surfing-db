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

public class Region implements TBase<Region, Region._Fields> {
  public String country;
  public String city;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COUNTRY((short)1, "country"),
    CITY((short)2, "city");
    private final short thriftId; private final String fieldName;
    _Fields(short thriftId, String fieldName) { this.thriftId = thriftId; this.fieldName = fieldName; }
    @Override public short getThriftFieldId() { return thriftId; }
    @Override public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int id) { return id == 1 ? COUNTRY : id == 2 ? CITY : null; }
  }

  public static final java.util.Map<_Fields, FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, FieldMetaData> m = new java.util.EnumMap<>(_Fields.class);
    m.put(_Fields.COUNTRY, new FieldMetaData("country", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    m.put(_Fields.CITY, new FieldMetaData("city", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    FieldMetaData.addStructMetaDataMap(Region.class, (java.util.Map) m);
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
          if (f.type == TType.STRING) country = iprot.readString();
          else TProtocolUtil.skip(iprot, f.type);
          break;
        case 2:
          if (f.type == TType.STRING) city = iprot.readString();
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
    oprot.writeStructBegin(new TStruct("Region"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public Region deepCopy() { Region r = new Region(); r.country = country; r.city = city; return r; }
  @Override public void clear() { country = null; city = null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) { switch (field) { case COUNTRY: return country; case CITY: return city; default: return null; } }
  @Override @SuppressWarnings("unchecked")
  public void setFieldValue(_Fields field, Object value) { switch (field) { case COUNTRY: country = (String) value; break; case CITY: city = (String) value; break; } }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }
  @Override public int compareTo(Region other) { String a = country == null ? "" : country; String b = other.country == null ? "" : other.country; return a.compareTo(b); }
}
