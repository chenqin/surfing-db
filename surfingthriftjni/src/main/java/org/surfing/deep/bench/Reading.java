package org.surfing.deep.bench;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;

public class Reading implements TBase<Reading, Reading._Fields> {
  public long ts;
  public double value;
  public List<String> notes;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TS((short)1, "ts"),
    VALUE((short)2, "value"),
    NOTES((short)3, "notes");
    private final short thriftId; private final String fieldName;
    _Fields(short thriftId, String fieldName) { this.thriftId = thriftId; this.fieldName = fieldName; }
    @Override public short getThriftFieldId() { return thriftId; }
    @Override public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int id) {
      switch (id) {
        case 1: return TS;
        case 2: return VALUE;
        case 3: return NOTES;
        default: return null;
      }
    }
  }

  public static final java.util.Map<_Fields, FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, FieldMetaData> m = new java.util.EnumMap<>(_Fields.class);
    m.put(_Fields.TS, new FieldMetaData("ts", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.I64)));
    m.put(_Fields.VALUE, new FieldMetaData("value", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.DOUBLE)));
    m.put(_Fields.NOTES, new FieldMetaData("notes", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new FieldValueMetaData(TType.STRING))));
    FieldMetaData.addStructMetaDataMap(Reading.class, (java.util.Map) m);
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
          if (f.type == TType.I64) ts = iprot.readI64();
          else TProtocolUtil.skip(iprot, f.type);
          break;
        case 2:
          if (f.type == TType.DOUBLE) value = iprot.readDouble();
          else TProtocolUtil.skip(iprot, f.type);
          break;
        case 3:
          if (f.type == TType.LIST) {
            TList tl = iprot.readListBegin();
            notes = new ArrayList<>(tl.size);
            for (int i = 0; i < tl.size; i++) notes.add(iprot.readString());
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
    oprot.writeStructBegin(new TStruct("Reading"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public Reading deepCopy() { Reading r = new Reading(); r.ts = ts; r.value = value; r.notes = notes == null ? null : new ArrayList<>(notes); return r; }
  @Override public void clear() { ts = 0L; value = 0.0; notes = null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) {
    switch (field) {
      case TS: return Long.valueOf(ts);
      case VALUE: return Double.valueOf(value);
      case NOTES: return notes;
      default: return null;
    }
  }
  @Override @SuppressWarnings("unchecked")
  public void setFieldValue(_Fields field, Object valueObj) {
    switch (field) {
      case TS: ts = (Long) valueObj; break;
      case VALUE: value = (Double) valueObj; break;
      case NOTES: notes = (List<String>) valueObj; break;
    }
  }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }
  @Override public int compareTo(Reading other) { return Long.compare(ts, other.ts); }
}
