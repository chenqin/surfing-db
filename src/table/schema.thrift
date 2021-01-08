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

const map<RowType,i64> Type_Size = {RowType.BOOL: 1, RowType.INT: 4}

struct Field {
    1: optional string name
    2: required RowType type
    3: required i64    unit_size // size of single unit or max size of list/map
    4: optional RowType list_type = RowType.VOID //type within list
    5: optional RowType map_key_type = RowType.VOID //map key type
    6: optional RowType map_value_type = RowType.VOID //map value type
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

struct Pair {
    1: required PValue key;
    2: required PValue value;
}

struct Value {
    1: optional PValue p_val;
    2: optional list<PValue> list_value;
    3: optional list<Pair> map_value; //index to value
}

struct RowSchema {
    1: required list<Field> fields;
    2: required list<Value> values;
}