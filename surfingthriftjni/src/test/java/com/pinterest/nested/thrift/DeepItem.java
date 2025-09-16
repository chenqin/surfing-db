package com.pinterest.nested.thrift;

import java.util.ArrayList;
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
import org.apache.thrift.meta_data.StructMetaData;

public class DeepItem implements TBase<DeepItem, DeepItem._Fields> {
  public long id;
  public List<DeepChild> children;                // list<struct>
  public List<List<String>> tags;                 // list<list<string>>
  public Map<String, List<Long>> props;           // map<string, list<i64>>

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ID((short)1, "id"),
    CHILDREN((short)2, "children"),
    TAGS((short)3, "tags"),
    PROPS((short)4, "props");
    private final short thriftId; private final String fieldName;
    _Fields(short id, String name) { thriftId = id; fieldName = name; }
    public short getThriftFieldId() { return thriftId; }
    public String getFieldName() { return fieldName; }
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId){ case 1:return ID; case 2:return CHILDREN; case 3:return TAGS; case 4:return PROPS; default:return null; }
    }
  }

  public static final java.util.Map<_Fields, FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, FieldMetaData> m = new java.util.EnumMap<>(_Fields.class);
    m.put(_Fields.ID, new FieldMetaData("id", org.apache.thrift.TFieldRequirementType.OPTIONAL, new FieldValueMetaData(TType.I64)));
    m.put(_Fields.CHILDREN, new FieldMetaData("children", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new StructMetaData(TType.STRUCT, DeepChild.class))));
    m.put(_Fields.TAGS, new FieldMetaData("tags", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new ListMetaData(TType.LIST, new FieldValueMetaData(TType.STRING)))));
    m.put(_Fields.PROPS, new FieldMetaData("props", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new ListMetaData(TType.LIST, new FieldValueMetaData(TType.I64)))));
    FieldMetaData.addStructMetaDataMap(DeepItem.class, (java.util.Map) m);
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
          if (f.type == TType.I64) { this.id = iprot.readI64(); }
          else { TProtocolUtil.skip(iprot, f.type); }
          break;
        case 2:
          if (f.type == TType.LIST) {
            TList tl = iprot.readListBegin();
            this.children = new ArrayList<>(tl.size);
            for (int i=0;i<tl.size;i++){ DeepChild c = new DeepChild(); c.read(iprot); this.children.add(c);} 
            iprot.readListEnd();
          } else { TProtocolUtil.skip(iprot, f.type); }
          break;
        case 3:
          if (f.type == TType.LIST) {
            TList l1 = iprot.readListBegin();
            this.tags = new ArrayList<>(l1.size);
            for(int i=0;i<l1.size;i++){
              TList l2 = iprot.readListBegin();
              java.util.List<String> inner = new java.util.ArrayList<>(l2.size);
              for(int j=0;j<l2.size;j++){ inner.add(iprot.readString()); }
              iprot.readListEnd();
              this.tags.add(inner);
            }
            iprot.readListEnd();
          } else { TProtocolUtil.skip(iprot, f.type); }
          break;
        case 4:
          if (f.type == TType.MAP) {
            TMap tm = iprot.readMapBegin();
            this.props = new java.util.HashMap<>(tm.size);
            for (int i=0;i<tm.size;i++){
              String k = iprot.readString();
              TList lv = iprot.readListBegin();
              java.util.List<Long> lst = new java.util.ArrayList<>(lv.size);
              for (int j=0;j<lv.size;j++){ lst.add(iprot.readI64()); }
              iprot.readListEnd();
              this.props.put(k, lst);
            }
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
    oprot.writeStructBegin(new TStruct("DeepItem"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public DeepItem deepCopy(){ DeepItem d = new DeepItem(); d.id=id; d.children = children==null?null:new ArrayList<>(children); d.tags=tags==null?null:new ArrayList<>(tags); d.props=props==null?null:new java.util.HashMap<>(props); return d; }
  @Override public void clear(){ id=0; children=null; tags=null; props=null; }
  @Override public _Fields fieldForId(int fieldId){ return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields f){ switch(f){case ID:return Long.valueOf(id); case CHILDREN:return children; case TAGS:return tags; case PROPS:return props; default:return null;} }
  @SuppressWarnings("unchecked")
  @Override public void setFieldValue(_Fields f, Object v){ switch(f){case ID:id=(Long)v; break; case CHILDREN:children=(List<DeepChild>)v; break; case TAGS: tags=(List<List<String>>)v; break; case PROPS: props=(Map<String,List<Long>>)v; break;} }
  @Override public boolean isSet(_Fields f){ return getFieldValue(f)!=null; }
  @Override public int compareTo(DeepItem o){ return Long.compare(this.id,o.id); }
}

