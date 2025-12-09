package org.surfing.deep.bench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;

public class DeepEvent implements TBase<DeepEvent, DeepEvent._Fields> {
  public long event_id;
  public String source;
  public List<List<Integer>> metrics;
  public Set<Long> label_ids;
  public Map<String, List<Long>> counts_by_key;
  public Meta meta;
  public List<Reading> readings;
  public Map<String, Bundle> bundles;
  public List<Map<String, List<Attr>>> attr_maps;
  public Geo geo;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    EVENT_ID((short)1, "event_id"),
    SOURCE((short)2, "source"),
    METRICS((short)3, "metrics"),
    LABEL_IDS((short)4, "label_ids"),
    COUNTS_BY_KEY((short)5, "counts_by_key"),
    META((short)6, "meta"),
    READINGS((short)7, "readings"),
    BUNDLES((short)8, "bundles"),
    ATTR_MAPS((short)9, "attr_maps"),
    GEO((short)10, "geo");
    private final short thriftId; private final String fieldName;
    _Fields(short thriftId, String fieldName) { this.thriftId = thriftId; this.fieldName = fieldName; }
    @Override public short getThriftFieldId() { return thriftId; }
    @Override public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int id) {
      switch (id) {
        case 1: return EVENT_ID;
        case 2: return SOURCE;
        case 3: return METRICS;
        case 4: return LABEL_IDS;
        case 5: return COUNTS_BY_KEY;
        case 6: return META;
        case 7: return READINGS;
        case 8: return BUNDLES;
        case 9: return ATTR_MAPS;
        case 10: return GEO;
        default: return null;
      }
    }
  }

  public static final java.util.Map<_Fields, FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, FieldMetaData> m = new java.util.EnumMap<>(_Fields.class);
    m.put(_Fields.EVENT_ID, new FieldMetaData("event_id", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.I64)));
    m.put(_Fields.SOURCE, new FieldMetaData("source", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.STRING)));
    m.put(_Fields.METRICS, new FieldMetaData("metrics", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new ListMetaData(TType.LIST, new FieldValueMetaData(TType.I32)))));
    m.put(_Fields.LABEL_IDS, new FieldMetaData("label_ids", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new SetMetaData(TType.SET, new FieldValueMetaData(TType.I64))));
    m.put(_Fields.COUNTS_BY_KEY, new FieldMetaData("counts_by_key", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new ListMetaData(TType.LIST, new FieldValueMetaData(TType.I64)))));
    m.put(_Fields.META, new FieldMetaData("meta", org.apache.thrift.TFieldRequirementType.OPTIONAL, new StructMetaData(TType.STRUCT, Meta.class)));
    m.put(_Fields.READINGS, new FieldMetaData("readings", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new StructMetaData(TType.STRUCT, Reading.class))));
    m.put(_Fields.BUNDLES, new FieldMetaData("bundles", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new StructMetaData(TType.STRUCT, Bundle.class))));
    m.put(_Fields.ATTR_MAPS, new FieldMetaData("attr_maps", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING),
            new ListMetaData(TType.LIST, new StructMetaData(TType.STRUCT, Attr.class))))));
    m.put(_Fields.GEO, new FieldMetaData("geo", org.apache.thrift.TFieldRequirementType.OPTIONAL, new StructMetaData(TType.STRUCT, Geo.class)));
    FieldMetaData.addStructMetaDataMap(DeepEvent.class, (java.util.Map) m);
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
          if (f.type == TType.I64) event_id = iprot.readI64();
          else TProtocolUtil.skip(iprot, f.type);
          break;
        case 2:
          if (f.type == TType.STRING) source = iprot.readString();
          else TProtocolUtil.skip(iprot, f.type);
          break;
        case 3:
          if (f.type == TType.LIST) {
            TList outer = iprot.readListBegin();
            metrics = new ArrayList<>(outer.size);
            for (int i = 0; i < outer.size; i++) {
              TList inner = iprot.readListBegin();
              List<Integer> ints = new ArrayList<>(inner.size);
              for (int j = 0; j < inner.size; j++) ints.add(iprot.readI32());
              iprot.readListEnd();
              metrics.add(ints);
            }
            iprot.readListEnd();
          } else TProtocolUtil.skip(iprot, f.type);
          break;
        case 4:
          if (f.type == TType.SET) {
            TSet ts = iprot.readSetBegin();
            label_ids = new HashSet<>(ts.size);
            for (int i = 0; i < ts.size; i++) label_ids.add(iprot.readI64());
            iprot.readSetEnd();
          } else TProtocolUtil.skip(iprot, f.type);
          break;
        case 5:
          if (f.type == TType.MAP) {
            TMap tm = iprot.readMapBegin();
            counts_by_key = new HashMap<>(tm.size);
            for (int i = 0; i < tm.size; i++) {
              String key = iprot.readString();
              TList ll = iprot.readListBegin();
              List<Long> longs = new ArrayList<>(ll.size);
              for (int j = 0; j < ll.size; j++) longs.add(iprot.readI64());
              iprot.readListEnd();
              counts_by_key.put(key, longs);
            }
            iprot.readMapEnd();
          } else TProtocolUtil.skip(iprot, f.type);
          break;
        case 6:
          if (f.type == TType.STRUCT) { meta = new Meta(); meta.read(iprot); }
          else TProtocolUtil.skip(iprot, f.type);
          break;
        case 7:
          if (f.type == TType.LIST) {
            TList tl = iprot.readListBegin();
            readings = new ArrayList<>(tl.size);
            for (int i = 0; i < tl.size; i++) { Reading r = new Reading(); r.read(iprot); readings.add(r); }
            iprot.readListEnd();
          } else TProtocolUtil.skip(iprot, f.type);
          break;
        case 8:
          if (f.type == TType.MAP) {
            TMap tm = iprot.readMapBegin();
            bundles = new HashMap<>(tm.size);
            for (int i = 0; i < tm.size; i++) {
              String key = iprot.readString();
              Bundle b = new Bundle(); b.read(iprot);
              bundles.put(key, b);
            }
            iprot.readMapEnd();
          } else TProtocolUtil.skip(iprot, f.type);
          break;
        case 9:
          if (f.type == TType.LIST) {
            TList outerMap = iprot.readListBegin();
            attr_maps = new ArrayList<>(outerMap.size);
            for (int i = 0; i < outerMap.size; i++) {
              TMap map = iprot.readMapBegin();
              Map<String, List<Attr>> entryMap = new HashMap<>(map.size);
              for (int j = 0; j < map.size; j++) {
                String key = iprot.readString();
                TList attrList = iprot.readListBegin();
                List<Attr> attrs = new ArrayList<>(attrList.size);
                for (int k = 0; k < attrList.size; k++) { Attr a = new Attr(); a.read(iprot); attrs.add(a); }
                iprot.readListEnd();
                entryMap.put(key, attrs);
              }
              iprot.readMapEnd();
              attr_maps.add(entryMap);
            }
            iprot.readListEnd();
          } else TProtocolUtil.skip(iprot, f.type);
          break;
        case 10:
          if (f.type == TType.STRUCT) { geo = new Geo(); geo.read(iprot); }
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
    oprot.writeStructBegin(new TStruct("DeepEvent"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public DeepEvent deepCopy() {
    DeepEvent d = new DeepEvent();
    d.event_id = event_id;
    d.source = source;
    if (metrics != null) {
      d.metrics = new ArrayList<>(metrics.size());
      for (List<Integer> row : metrics) d.metrics.add(row == null ? null : new ArrayList<>(row));
    }
    d.label_ids = label_ids == null ? null : new HashSet<>(label_ids);
    if (counts_by_key != null) {
      d.counts_by_key = new HashMap<>(counts_by_key.size());
      for (Map.Entry<String, List<Long>> e : counts_by_key.entrySet()) {
        List<Long> vals = e.getValue();
        d.counts_by_key.put(e.getKey(), vals == null ? null : new ArrayList<>(vals));
      }
    }
    d.meta = meta == null ? null : meta.deepCopy();
    if (readings != null) {
      d.readings = new ArrayList<>(readings.size());
      for (Reading r : readings) d.readings.add(r == null ? null : r.deepCopy());
    }
    if (bundles != null) {
      d.bundles = new HashMap<>(bundles.size());
      for (Map.Entry<String, Bundle> e : bundles.entrySet()) d.bundles.put(e.getKey(), e.getValue() == null ? null : e.getValue().deepCopy());
    }
    if (attr_maps != null) {
      d.attr_maps = new ArrayList<>(attr_maps.size());
      for (Map<String, List<Attr>> m : attr_maps) {
        if (m == null) { d.attr_maps.add(null); continue; }
        Map<String, List<Attr>> copy = new HashMap<>(m.size());
        for (Map.Entry<String, List<Attr>> e : m.entrySet()) {
          List<Attr> attrs = e.getValue();
          if (attrs == null) copy.put(e.getKey(), null);
          else {
            List<Attr> cc = new ArrayList<>(attrs.size());
            for (Attr a : attrs) cc.add(a == null ? null : a.deepCopy());
            copy.put(e.getKey(), cc);
          }
        }
        d.attr_maps.add(copy);
      }
    }
    d.geo = geo == null ? null : geo.deepCopy();
    return d;
  }

  @Override public void clear() {
    event_id = 0L;
    source = null;
    metrics = null;
    label_ids = null;
    counts_by_key = null;
    meta = null;
    readings = null;
    bundles = null;
    attr_maps = null;
    geo = null;
  }

  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }

  @Override
  public Object getFieldValue(_Fields field) {
    switch (field) {
      case EVENT_ID: return Long.valueOf(event_id);
      case SOURCE: return source;
      case METRICS: return metrics;
      case LABEL_IDS: return label_ids;
      case COUNTS_BY_KEY: return counts_by_key;
      case META: return meta;
      case READINGS: return readings;
      case BUNDLES: return bundles;
      case ATTR_MAPS: return attr_maps;
      case GEO: return geo;
      default: return null;
    }
  }

  @Override @SuppressWarnings("unchecked")
  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
      case EVENT_ID: event_id = (Long) value; break;
      case SOURCE: source = (String) value; break;
      case METRICS: metrics = (List<List<Integer>>) value; break;
      case LABEL_IDS: label_ids = (Set<Long>) value; break;
      case COUNTS_BY_KEY: counts_by_key = (Map<String, List<Long>>) value; break;
      case META: meta = (Meta) value; break;
      case READINGS: readings = (List<Reading>) value; break;
      case BUNDLES: bundles = (Map<String, Bundle>) value; break;
      case ATTR_MAPS: attr_maps = (List<Map<String, List<Attr>>>) value; break;
      case GEO: geo = (Geo) value; break;
    }
  }

  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }

  @Override public int compareTo(DeepEvent other) { return Long.compare(event_id, other.event_id); }
}
