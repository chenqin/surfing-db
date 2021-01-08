namespace cpp surfingdb.table.schema //

enum RowType {
    VOID,
    BOOL,
    INT,
    LONG,
    DOUBLE,
    STRING,
    LIST,
    MAP
}

struct Field {
    1: required string name
    2: required RowType type
    3: required i64    unit_size // size of single unit or max size of list/map
    4: optional RowType list_type = RowType.VOID //type within list
    5: optional i64 list_unit_size // max_size of element in list
    6: optional RowType map_key_type = RowType.VOID //map key type
    7: optional RowType map_value_type = RowType.VOID //map value type
    8: optional i64 map_key_unit_size //max_size of map key
    9: optional i64 map_value_unit_size //max_size of map value
}

struct PValue {
    1: optional bool bool_val;
    2: optional byte byte_val;
    3: optional i16 short_val;
    4: optional i32 int_val;
    5: optional i64 long_val;
    6: optional double double_val;
    7: optional binary binary_val;
    8: optional string string_val;
}

struct Value {
    1: optional PValue p_val;
    2: optional list<PValue> list_value;
    3: optional map<PValue, PValue> map_value; //index to value
}

struct RowSchema {
    1: required list<Field> fields;
    2: required list<Value> values;
}