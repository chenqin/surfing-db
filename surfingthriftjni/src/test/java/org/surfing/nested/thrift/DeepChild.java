package org.surfing.nested.thrift;

import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.MapMetaData;

public class DeepChild implements TBase<DeepChild, DeepChild._Fields> {
  public String name;
  public Map<String, Long> weights;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NAME((short)1, "name"),
    WEIGHTS((short)2, "weights");
    private final short thriftId; private final String fieldName;
    _Fields(short id, String name) { thriftId = id; fieldName = name; }
    public short getThriftFieldId() { return thriftId; }
    public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int fieldId) { return fieldId==1?NAME:fieldId==2?WEIGHTS:null; }
  }

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    metaDataMap = new java.util.EnumMap<>(_Fields.class);
    metaDataMap.put(_Fields.NAME, new FieldMetaData("name", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    metaDataMap.put(_Fields.WEIGHTS, new FieldMetaData("weights", org.apache.thrift.TFieldRequirementType.OPTIONAL, new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new FieldValueMetaData(TType.I64))));
    FieldMetaData.addStructMetaDataMap(DeepChild.class, (Map) metaDataMap);
  }

  @Override
  public void read(TProtocol iprot) throws TException {
    iprot.readStructBegin();
    while (true) {
      TField f = iprot.readFieldBegin();
      if (f.type == TType.STOP) break;
      switch (f.id) {
        case 1:
          if (f.type == TType.STRING) { this.name = iprot.readString(); }
          else { TProtocolUtil.skip(iprot, f.type); }
          break;
        case 2:
          if (f.type == TType.MAP) {
            TMap tm = iprot.readMapBegin();
            this.weights = new HashMap<>(tm.size);
            for (int i=0;i<tm.size;i++){ String k = iprot.readString(); long v = iprot.readI64(); this.weights.put(k,v);} 
            iprot.readMapEnd();
          } else { TProtocolUtil.skip(iprot, f.type); }
          break;
        default: TProtocolUtil.skip(iprot, f.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  @Override
  public void write(TProtocol oprot) throws TException {
    oprot.writeStructBegin(new TStruct("DeepChild"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public DeepChild deepCopy() { DeepChild c = new DeepChild(); c.name = name; if (weights!=null) c.weights = new HashMap<>(weights); return c; }
  @Override public void clear() { name = null; weights = null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) { switch(field){case NAME:return name; case WEIGHTS:return weights; default:return null;} }
  @SuppressWarnings("unchecked")
  @Override public void setFieldValue(_Fields field, Object value) {
    switch(field){ case NAME: name = (String)value; break; case WEIGHTS: weights = (Map<String,Long>) value; break; }
  }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field)!=null; }
  @Override public int compareTo(DeepChild o) { String a = name==null?"":name; String b = o.name==null?"":o.name; return a.compareTo(b);} 
}

