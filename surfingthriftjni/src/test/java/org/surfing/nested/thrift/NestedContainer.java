package org.surfing.nested.thrift;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.StructMetaData;

public class NestedContainer implements TBase<NestedContainer, NestedContainer._Fields> {
  public List<NestedItem> items;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ITEMS((short)1, "items");
    private final short thriftId; private final String fieldName;
    _Fields(short id, String name) { thriftId = id; fieldName = name; }
    public short getThriftFieldId() { return thriftId; }
    public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int fieldId) { return fieldId == 1 ? ITEMS : null; }
  }

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    metaDataMap = new java.util.EnumMap<>(_Fields.class);
    metaDataMap.put(_Fields.ITEMS, new FieldMetaData("items", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new StructMetaData(TType.STRUCT, NestedItem.class))));
    FieldMetaData.addStructMetaDataMap(NestedContainer.class, (Map) metaDataMap);
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
            this.items = new ArrayList<>(tl.size);
            for (int i = 0; i < tl.size; i++) {
              NestedItem it = new NestedItem();
              it.read(iprot);
              this.items.add(it);
            }
            iprot.readListEnd();
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
    oprot.writeStructBegin(new TStruct("NestedContainer"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public NestedContainer deepCopy() {
    NestedContainer c = new NestedContainer();
    if (items != null) c.items = new ArrayList<>(items);
    return c;
  }
  @Override public void clear() { items = null; }
  @Override public _Fields fieldForId(int fieldId) { return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields field) {
    switch (field) { case ITEMS: return items; default: return null; }
  }
  @SuppressWarnings("unchecked")
  @Override public void setFieldValue(_Fields field, Object value) {
    switch (field) { case ITEMS: this.items = (List<NestedItem>) value; break; }
  }
  @Override public boolean isSet(_Fields field) { return getFieldValue(field) != null; }
  @Override public int compareTo(NestedContainer other) { return Integer.compare(this.items == null ? 0 : this.items.size(), other.items == null ? 0 : other.items.size()); }
}

