package org.surfing.nested.thrift;

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

public class NestedItem implements TBase<NestedItem, NestedItem._Fields> {
  public long id;
  public List<String> tags;
  public Map<String, Long> props;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ID((short)1, "id"),
    TAGS((short)2, "tags"),
    PROPS((short)3, "props");
    private final short thriftId; private final String fieldName;
    _Fields(short id, String name) { thriftId = id; fieldName = name; }
    public short getThriftFieldId() { return thriftId; }
    public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
        case 1: return ID;
        case 2: return TAGS;
        case 3: return PROPS;
        default: return null;
      }
    }
  }

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    metaDataMap = new java.util.EnumMap<>(_Fields.class);
    metaDataMap.put(_Fields.ID, new FieldMetaData("id", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.I64)));
    metaDataMap.put(_Fields.TAGS, new FieldMetaData("tags", org.apache.thrift.TFieldRequirementType.OPTIONAL, new ListMetaData(TType.LIST, new FieldValueMetaData(TType.STRING))));
    metaDataMap.put(_Fields.PROPS, new FieldMetaData("props", org.apache.thrift.TFieldRequirementType.OPTIONAL, new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new FieldValueMetaData(TType.I64))));
    FieldMetaData.addStructMetaDataMap(NestedItem.class, (Map) metaDataMap);
  }

  @Override
  public void read(TProtocol iprot) throws TException {
    iprot.readStructBegin();
    while (true) {
      TField f = iprot.readFieldBegin();
      if (f.type == TType.STOP) break;
      switch (f.id) {
        case 1:
          if (f.type == TType.I64) { this.id = iprot.readI64(); }
          else { TProtocolUtil.skip(iprot, f.type); }
          break;
        case 2:
          if (f.type == TType.LIST) {
            TList tl = iprot.readListBegin();
            this.tags = new ArrayList<>(tl.size);
            for (int i = 0; i < tl.size; i++) this.tags.add(iprot.readString());
            iprot.readListEnd();
          } else { TProtocolUtil.skip(iprot, f.type); }
          break;
        case 3:
          if (f.type == TType.MAP) {
            TMap tm = iprot.readMapBegin();
            this.props = new HashMap<>(tm.size);
            for (int i = 0; i < tm.size; i++) {
              String k = iprot.readString();
              long v = iprot.readI64();
              this.props.put(k, v);
            }
            iprot.readMapEnd();
          } else { TProtocolUtil.skip(iprot, f.type); }
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
    // Not used in tests; provide minimal stub
    oprot.writeStructBegin(new TStruct("NestedItem"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public NestedItem deepCopy() {
    NestedItem n = new NestedItem();
    n.id = id;
    if (tags != null) n.tags = new ArrayList<>(tags);
    if (props != null) n.props = new HashMap<>(props);
    return n;
  }
  @Override public void clear() { id = 0; tags = null; props = null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) {
    switch (field) {
      case ID: return Long.valueOf(id);
      case TAGS: return tags;
      case PROPS: return props;
      default: return null;
    }
  }
  @SuppressWarnings("unchecked")
  @Override public void setFieldValue(_Fields field, Object value) {
    switch (field) {
      case ID: this.id = (Long) value; break;
      case TAGS: this.tags = (List<String>) value; break;
      case PROPS: this.props = (Map<String, Long>) value; break;
    }
  }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }
  @Override public int compareTo(NestedItem other) { return Long.compare(this.id, other.id); }
}

