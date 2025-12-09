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

public class DeepContainer implements TBase<DeepContainer, DeepContainer._Fields> {
  public List<DeepItem> items;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ITEMS((short)1, "items");
    private final short thriftId; private final String fieldName;
    _Fields(short id, String name){ thriftId=id; fieldName=name; }
    public short getThriftFieldId(){ return thriftId; }
    public String getFieldName(){ return fieldName; }
    public static _Fields findByThriftId(int fieldId){ return fieldId==1?ITEMS:null; }
  }

  public static final Map<_Fields, FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, FieldMetaData> m = new java.util.EnumMap<>(_Fields.class);
    m.put(_Fields.ITEMS, new FieldMetaData("items", org.apache.thrift.TFieldRequirementType.OPTIONAL,
        new ListMetaData(TType.LIST, new StructMetaData(TType.STRUCT, DeepItem.class))));
    FieldMetaData.addStructMetaDataMap(DeepContainer.class, (Map) m);
    metaDataMap = m;
  }

  @Override
  public void read(TProtocol iprot) throws TException {
    iprot.readStructBegin();
    while(true){
      TField f = iprot.readFieldBegin();
      if (f.type == TType.STOP) break;
      switch(f.id){
        case 1:
          if (f.type == TType.LIST){
            TList tl = iprot.readListBegin();
            items = new ArrayList<>(tl.size);
            for (int i=0;i<tl.size;i++){ DeepItem it = new DeepItem(); it.read(iprot); items.add(it);} 
            iprot.readListEnd();
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
    oprot.writeStructBegin(new TStruct("DeepContainer"));
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override public DeepContainer deepCopy(){ DeepContainer c = new DeepContainer(); c.items = items==null?null:new ArrayList<>(items); return c; }
  @Override public void clear(){ items = null; }
  @Override public _Fields fieldForId(int fieldId){ return _Fields.findByThriftId(fieldId); }
  @Override public Object getFieldValue(_Fields f){ return f==_Fields.ITEMS?items:null; }
  @SuppressWarnings("unchecked")
  @Override public void setFieldValue(_Fields f, Object v){ if (f==_Fields.ITEMS) items = (List<DeepItem>) v; }
  @Override public boolean isSet(_Fields f){ return getFieldValue(f)!=null; }
  @Override public int compareTo(DeepContainer o){ int a = items==null?0:items.size(); int b = o.items==null?0:o.items.size(); return Integer.compare(a,b);} 
}

