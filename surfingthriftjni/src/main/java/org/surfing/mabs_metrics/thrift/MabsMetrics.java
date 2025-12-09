package org.surfing.mabs_metrics.thrift;

import java.util.EnumMap;
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

/**
 * Handcrafted Thrift-like class implementing TBase for benchmarking Java decode.
 * Mirrors src/mabs.thrift::MabsMetrics schema and metadata so GenericThriftToArrowConverter
 * can infer Arrow schema and deserialize with TDeserializer.
 */
public class MabsMetrics implements TBase<MabsMetrics, MabsMetrics._Fields> {
  public long timestamp;
  public String service_tags;
  public String node_tags;
  public Map<String, Long> counters;
  public Map<String, Double> gauges;
  public Map<String, String> histograms;
  public String service_name;
  public Map<String, Double> double_counters;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TIMESTAMP((short)1, "timestamp"),
    SERVICE_TAGS((short)2, "service_tags"),
    NODE_TAGS((short)3, "node_tags"),
    COUNTERS((short)4, "counters"),
    GAUGES((short)5, "gauges"),
    HISTOGRAMS((short)6, "histograms"),
    SERVICE_NAME((short)7, "service_name"),
    DOUBLE_COUNTERS((short)8, "double_counters");
    private final short thriftId; private final String fieldName;
    _Fields(short id, String name) { thriftId = id; fieldName = name; }
    public short getThriftFieldId() { return thriftId; }
    public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
        case 1: return TIMESTAMP;
        case 2: return SERVICE_TAGS;
        case 3: return NODE_TAGS;
        case 4: return COUNTERS;
        case 5: return GAUGES;
        case 6: return HISTOGRAMS;
        case 7: return SERVICE_NAME;
        case 8: return DOUBLE_COUNTERS;
        default: return null;
      }
    }
  }

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    metaDataMap = new EnumMap<>(_Fields.class);
    metaDataMap.put(_Fields.TIMESTAMP, new FieldMetaData("timestamp", org.apache.thrift.TFieldRequirementType.REQUIRED, new FieldValueMetaData(TType.I64)));
    metaDataMap.put(_Fields.SERVICE_TAGS, new FieldMetaData("service_tags", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    metaDataMap.put(_Fields.NODE_TAGS, new FieldMetaData("node_tags", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    metaDataMap.put(_Fields.COUNTERS, new FieldMetaData("counters", org.apache.thrift.TFieldRequirementType.OPTIONAL, new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new FieldValueMetaData(TType.I64))));
    metaDataMap.put(_Fields.GAUGES, new FieldMetaData("gauges", org.apache.thrift.TFieldRequirementType.OPTIONAL, new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new FieldValueMetaData(TType.DOUBLE))));
    metaDataMap.put(_Fields.HISTOGRAMS, new FieldMetaData("histograms", org.apache.thrift.TFieldRequirementType.OPTIONAL, new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new FieldValueMetaData(TType.STRING))));
    metaDataMap.put(_Fields.SERVICE_NAME, new FieldMetaData("service_name", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    metaDataMap.put(_Fields.DOUBLE_COUNTERS, new FieldMetaData("double_counters", org.apache.thrift.TFieldRequirementType.OPTIONAL, new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new FieldValueMetaData(TType.DOUBLE))));
    FieldMetaData.addStructMetaDataMap(MabsMetrics.class, (Map) metaDataMap);
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
        case 4: if (f.type == TType.MAP) { TMap tm = iprot.readMapBegin(); this.counters = new HashMap<>(tm.size); for (int i=0;i<tm.size;i++){ String k=iprot.readString(); long v=iprot.readI64(); this.counters.put(k, v);} iprot.readMapEnd(); } else { TProtocolUtil.skip(iprot, f.type); } break;
        case 5: if (f.type == TType.MAP) { TMap tm = iprot.readMapBegin(); this.gauges = new HashMap<>(tm.size); for (int i=0;i<tm.size;i++){ String k=iprot.readString(); double v=iprot.readDouble(); this.gauges.put(k, v);} iprot.readMapEnd(); } else { TProtocolUtil.skip(iprot, f.type); } break;
        case 6: if (f.type == TType.MAP) { TMap tm = iprot.readMapBegin(); this.histograms = new HashMap<>(tm.size); for (int i=0;i<tm.size;i++){ String k=iprot.readString(); String v=iprot.readString(); this.histograms.put(k, v);} iprot.readMapEnd(); } else { TProtocolUtil.skip(iprot, f.type); } break;
        case 7: if (f.type == TType.STRING) { this.service_name = iprot.readString(); } else { TProtocolUtil.skip(iprot, f.type); } break;
        case 8: if (f.type == TType.MAP) { TMap tm = iprot.readMapBegin(); this.double_counters = new HashMap<>(tm.size); for (int i=0;i<tm.size;i++){ String k=iprot.readString(); double v=iprot.readDouble(); this.double_counters.put(k, v);} iprot.readMapEnd(); } else { TProtocolUtil.skip(iprot, f.type); } break;
        default: TProtocolUtil.skip(iprot, f.type); break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  @Override
  public void write(TProtocol oprot) throws TException {
    // Not used by benchmarks
    oprot.writeStructBegin(new TStruct("MabsMetrics"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public MabsMetrics deepCopy() {
    MabsMetrics c = new MabsMetrics();
    c.timestamp = timestamp;
    c.service_tags = service_tags;
    c.node_tags = node_tags;
    if (counters != null) c.counters = new HashMap<>(counters);
    if (gauges != null) c.gauges = new HashMap<>(gauges);
    if (histograms != null) c.histograms = new HashMap<>(histograms);
    c.service_name = service_name;
    if (double_counters != null) c.double_counters = new HashMap<>(double_counters);
    return c;
  }
  @Override public void clear() { timestamp=0; service_tags=null; node_tags=null; service_name=null; counters=null; gauges=null; histograms=null; double_counters=null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) {
    switch (field) {
      case TIMESTAMP: return Long.valueOf(timestamp);
      case SERVICE_TAGS: return service_tags;
      case NODE_TAGS: return node_tags;
      case COUNTERS: return counters;
      case GAUGES: return gauges;
      case HISTOGRAMS: return histograms;
      case SERVICE_NAME: return service_name;
      case DOUBLE_COUNTERS: return double_counters;
      default: return null;
    }
  }
  @Override public void setFieldValue(_Fields field, Object value) {
    switch (field) {
      case TIMESTAMP: this.timestamp = (Long) value; break;
      case SERVICE_TAGS: this.service_tags = (String) value; break;
      case NODE_TAGS: this.node_tags = (String) value; break;
      case COUNTERS: this.counters = (Map<String, Long>) value; break;
      case GAUGES: this.gauges = (Map<String, Double>) value; break;
      case HISTOGRAMS: this.histograms = (Map<String, String>) value; break;
      case SERVICE_NAME: this.service_name = (String) value; break;
      case DOUBLE_COUNTERS: this.double_counters = (Map<String, Double>) value; break;
    }
  }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }
  @Override public int compareTo(MabsMetrics other) {
    int c = Long.compare(this.timestamp, other.timestamp);
    if (c != 0) return c;
    String a = this.service_name == null ? "" : this.service_name;
    String b = other.service_name == null ? "" : other.service_name;
    return a.compareTo(b);
  }
}

