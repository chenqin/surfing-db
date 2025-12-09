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
import org.apache.thrift.meta_data.StructMetaData;

public class Geo implements TBase<Geo, Geo._Fields> {
  public double lat;
  public double lon;
  public Region region;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    LAT((short)1, "lat"),
    LON((short)2, "lon"),
    REGION((short)3, "region");
    private final short thriftId; private final String fieldName;
    _Fields(short thriftId, String fieldName) { this.thriftId = thriftId; this.fieldName = fieldName; }
    @Override public short getThriftFieldId() { return thriftId; }
    @Override public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int id) {
      switch (id) {
        case 1: return LAT;
        case 2: return LON;
        case 3: return REGION;
        default: return null;
      }
    }
  }

  public static final java.util.Map<_Fields, FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, FieldMetaData> m = new java.util.EnumMap<>(_Fields.class);
    m.put(_Fields.LAT, new FieldMetaData("lat", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.DOUBLE)));
    m.put(_Fields.LON, new FieldMetaData("lon", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.DOUBLE)));
    m.put(_Fields.REGION, new FieldMetaData("region", org.apache.thrift.TFieldRequirementType.OPTIONAL, new StructMetaData(TType.STRUCT, Region.class)));
    FieldMetaData.addStructMetaDataMap(Geo.class, (java.util.Map) m);
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
          if (f.type == TType.DOUBLE) lat = iprot.readDouble();
          else TProtocolUtil.skip(iprot, f.type);
          break;
        case 2:
          if (f.type == TType.DOUBLE) lon = iprot.readDouble();
          else TProtocolUtil.skip(iprot, f.type);
          break;
        case 3:
          if (f.type == TType.STRUCT) { region = new Region(); region.read(iprot); }
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
    oprot.writeStructBegin(new TStruct("Geo"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public Geo deepCopy() { Geo g = new Geo(); g.lat = lat; g.lon = lon; g.region = region == null ? null : region.deepCopy(); return g; }
  @Override public void clear() { lat = 0.0; lon = 0.0; region = null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) {
    switch (field) {
      case LAT: return Double.valueOf(lat);
      case LON: return Double.valueOf(lon);
      case REGION: return region;
      default: return null;
    }
  }
  @Override @SuppressWarnings("unchecked")
  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
      case LAT: lat = (Double) value; break;
      case LON: lon = (Double) value; break;
      case REGION: region = (Region) value; break;
    }
  }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }
  @Override public int compareTo(Geo other) { return Double.compare(lat, other.lat); }
}
