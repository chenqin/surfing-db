package org.surfing.drsquirrel.bench;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;

/**
 * Minimal Thrift-like class implementing TBase for benchmarking Java decode.
 * Matches the wire layout of fields used by MabsMetrics in payload generator.
 * Fields: 1: i64 timestamp, 2: string service_tags, 3: string node_tags, 7: string service_name
 */
public class MabsLite implements TBase<MabsLite, MabsLite._Fields> {
  public long timestamp;
  public String service_tags;
  public String node_tags;
  public String service_name;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TIMESTAMP((short)1, "timestamp"),
    SERVICE_TAGS((short)2, "service_tags"),
    NODE_TAGS((short)3, "node_tags"),
    SERVICE_NAME((short)7, "service_name");
    private final short thriftId;
    private final String fieldName;
    _Fields(short id, String name) { thriftId = id; fieldName = name; }
    public short getThriftFieldId() { return thriftId; }
    public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
        case 1: return TIMESTAMP;
        case 2: return SERVICE_TAGS;
        case 3: return NODE_TAGS;
        case 7: return SERVICE_NAME;
        default: return null;
      }
    }
  }

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    metaDataMap = new EnumMap<>(_Fields.class);
    metaDataMap.put(_Fields.TIMESTAMP, new FieldMetaData("timestamp", org.apache.thrift.TFieldRequirementType.DEFAULT, new FieldValueMetaData(TType.I64)));
    metaDataMap.put(_Fields.SERVICE_TAGS, new FieldMetaData("service_tags", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    metaDataMap.put(_Fields.NODE_TAGS, new FieldMetaData("node_tags", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    metaDataMap.put(_Fields.SERVICE_NAME, new FieldMetaData("service_name", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    FieldMetaData.addStructMetaDataMap(MabsLite.class, (Map) metaDataMap);
  }

  @Override
  public void read(TProtocol iprot) throws TException {
    iprot.readStructBegin();
    while (true) {
      TField f = iprot.readFieldBegin();
      if (f.type == TType.STOP) break;
      switch (f.id) {
        case 1: if (f.type == TType.I64) { this.timestamp = iprot.readI64(); } else { TProtocolUtil.skip(iprot, f.type); } break;
        case 2: if (f.type == TType.STRING) { this.service_tags = iprot.readString(); } else { TProtocolUtil.skip(iprot, f.type); } break;
        case 3: if (f.type == TType.STRING) { this.node_tags = iprot.readString(); } else { TProtocolUtil.skip(iprot, f.type); } break;
        case 7: if (f.type == TType.STRING) { this.service_name = iprot.readString(); } else { TProtocolUtil.skip(iprot, f.type); } break;
        default: TProtocolUtil.skip(iprot, f.type); break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  @Override
  public void write(TProtocol oprot) throws TException {
    // Not used in benchmark
    oprot.writeStructBegin(new TStruct("MabsLite"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public MabsLite deepCopy() { MabsLite c = new MabsLite(); c.timestamp=timestamp; c.service_tags=service_tags; c.node_tags=node_tags; c.service_name=service_name; return c; }
  @Override
  public void clear() { timestamp=0; service_tags=null; node_tags=null; service_name=null; }
  @Override
  public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override
  public Object getFieldValue(_Fields field) {
    switch (field) {
      case TIMESTAMP: return Long.valueOf(timestamp);
      case SERVICE_TAGS: return service_tags;
      case NODE_TAGS: return node_tags;
      case SERVICE_NAME: return service_name;
      default: return null;
    }
  }
  @Override
  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
      case TIMESTAMP: this.timestamp = (Long) value; break;
      case SERVICE_TAGS: this.service_tags = (String) value; break;
      case NODE_TAGS: this.node_tags = (String) value; break;
      case SERVICE_NAME: this.service_name = (String) value; break;
    }
  }
  @Override
  public boolean isSet(_Fields field) { return getFieldValue(field) != null; }

  @Override
  public int compareTo(MabsLite other) {
    // Compare by timestamp then service_name
    int c = Long.compare(this.timestamp, other.timestamp);
    if (c != 0) return c;
    String a = this.service_name == null ? "" : this.service_name;
    String b = other.service_name == null ? "" : other.service_name;
    return a.compareTo(b);
  }
}
