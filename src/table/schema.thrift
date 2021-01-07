namespace cpp surfingdb.table.schema //

enum RowType {
    BOOL
    INT,
    LONG,
    DOUBLE,
    BINARY,
    STRING,
    LIST,
    MAP
}

struct Field {
    1: required RowType type
    2: required i64    len // (sum of all nested value size)
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
    3: optional map<Field, PValue> map_value;
}

struct RowSchema {
    1: required list<Field> fields;
    2: required list<Value> values;
}