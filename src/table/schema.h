//
// Created by cq on 1/10/21.
//

#ifndef SURFINGDB_SCHEMA_H
#define SURFINGDB_SCHEMA_H
#include "table/gen-cpp/schema_types.h"

#include <glog/logging.h>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <unordered_map>

namespace surfingdb {
namespace table {

#define MAX_STR_LEN 128
#define HEADER_SIZE sizeof(long)
#define MEM_PAGE_SIZE 65536 //64KB
#define FILE_IO_VECTOR 8
#define SSD_CHUNK_SIZ 65536 // read/write ssd per 64KB chunk

/**
 * build a continous memory buffer
 */
using namespace surfingdb::table::schema;
using std::hash;
using std::string;

struct FieldHasher {
  std::size_t operator()(const Field& k) const {
    using std::hash;
    using std::size_t;
    using std::string;

    return (hash<string>()(k.name));
  }
} field_hasher;

struct SchemaHasher {
  std::size_t operator()(const RowSchema& k) const {
    using std::hash;
    using std::size_t;
    using std::string;
    std::size_t result = (hash<int>()(k.fields.size()));
    for (size_t i = 0; i < k.fields.size(); i++) {
      result = result ^ (field_hasher.operator()(k.fields.at(i)) >> i);
    }
    return result;
  }
} schema_hasher;

struct ValueHasher {
  std::size_t operator()(const PValue& k) const {
    size_t result = hash<string>()(k.string_val);
    result ^= hash<int>()(k.int_val) << 1;
    result ^= hash<long>()(k.long_val) << 2;
    result ^= hash<double>()(k.double_val) << 3;
    result ^= hash<bool>()(k.bool_val) << 4;
    return result;
  }

  std::size_t operator()(const Value& k) const {
    size_t result = hash<string>()(k.p_val.string_val);
    result ^= hash<int>()(k.p_val.int_val) << 1;
    result ^= hash<long>()(k.p_val.long_val) << 2;
    result ^= hash<double>()(k.p_val.double_val) << 3;
    result ^= hash<bool>()(k.p_val.bool_val) << 4;
    for (auto l : k.list_value) {
      result ^= operator()(l);
    }
    for (auto p : k.map_value) {
      result ^= operator()(p.first) << 1;
      result ^= operator()(p.second) << 2;
    }
    return result;
  }
} value_hasher;

bool surfingdb::table::schema::PValue::operator<(const PValue& k) const {
  return (hash<string>()(this->string_val)) < (hash<string>()(k.string_val))
         || this->double_val < k.double_val
         || this->long_val < k.long_val
         || this->int_val < k.int_val
         || this->bool_val == k.bool_val;
}

/**
 * valid if schema is accepted
 * @param rowSchema
 */
void validSchema(const RowSchema& rowSchema) {
  CHECK_GT(rowSchema.fields.size(), 0);
  std::set<std::string> name_set;
  for (auto& field : rowSchema.fields) {
    CHECK(name_set.find(field.name) == name_set.end());

    name_set.insert(field.name);

    if (field.type == RowType::LIST || field.type == RowType::MAP) {
      CHECK(field.list_type != RowType::LIST);
      CHECK(field.list_type != RowType::MAP);
    } else {
      CHECK(field.list_type == RowType::VOID);
      CHECK(field.map_key_type == RowType::VOID);
      CHECK(field.map_value_type == RowType::VOID);
    }
  }
}

inline size_t getFieldSize(const Field& f) {
  switch (f.type) {

  case RowType::VOID: {
    return 0;
  }
  case RowType::BOOL: {
    return sizeof(bool);
  }
  case RowType::INT: {
    return sizeof(int);
  }
  case RowType::LONG: {
    return sizeof(long);
  }
  case RowType::DOUBLE: {
    return sizeof(double);
  }
    //  |size|string|
  case RowType::STRING: {
    // max support string column MAX_STR_LEN character
    long str_size = f.max_unit_size < MAX_STR_LEN ? f.max_unit_size : MAX_STR_LEN;
    return sizeof(char) * str_size + HEADER_SIZE;
  }
    //  |size|array|
  case RowType::LIST: {
    Field l;
    l.type = f.list_type;
    return getFieldSize(l) * f.max_unit_size + HEADER_SIZE;
  }
    // | size | key val key val|
  case RowType::MAP: {
    Field k, v;
    k.type = f.map_key_type;
    v.type = f.map_value_type;
    return getFieldSize(k) * f.max_map_key_unit_size + getFieldSize(v) * v.max_map_value_unit_size + HEADER_SIZE;
  }
  }
  assert(false);
}

inline void checkStringLength(const RowType::type type, const uint64_t& max_size) {
  if (type == RowType::STRING) {
    CHECK_LE(max_size, MAX_STR_LEN);
  }
}
/**
 * helper function to init a primitive field
 * @param field
 * @param name
 * @param type
 * @param max_size
 */
inline void initField(Field& field, const std::string& name, const RowType::type type, const uint64_t& max_size) {
  field.name = name;
  field.type = type;
  field.max_unit_size = max_size;
  // IF string should check max_size no more than MAX_STR_LEN
  CHECK(type != RowType::LIST);
  CHECK(type != RowType::MAP);
  checkStringLength(type, max_size);
}

/**
 * helper function to init a list field
 * @param field
 * @param name
 * @param type
 * @param max_size
 */
inline void initListField(Field& field, const std::string& name, const RowType::type list_type, const uint64_t& max_list_size, const uint64_t& max_element_size) {
  field.name = name;
  field.type = RowType::LIST;
  field.list_type = list_type;
  field.max_unit_size = max_list_size;
  field.max_list_unit_size = max_element_size;
  CHECK(list_type != RowType::LIST);
  CHECK(list_type != RowType::MAP);
  checkStringLength(list_type, max_element_size);
}

/**
 * helper function to init a map field
 * @param field
 * @param name
 * @param key_type
 * @param value_type
 * @param max_map_size
 * @param max_key_size
 * @param max_value_size
 */
inline void initMapField(Field& field, const std::string& name, const RowType::type key_type, const RowType::type value_type, const uint64_t& max_map_size, const uint64_t& max_key_size, const uint64_t& max_value_size) {
  field.name = name;
  field.type = RowType::MAP;
  field.map_key_type = key_type;
  field.map_value_type = value_type;
  field.max_unit_size = max_map_size;
  field.max_map_key_unit_size = max_key_size;
  field.max_map_value_unit_size = max_value_size;
  CHECK(key_type != RowType::LIST);
  CHECK(key_type != RowType::MAP);
  CHECK(value_type != RowType::LIST);
  CHECK(value_type != RowType::MAP);
  checkStringLength(key_type, max_key_size);
  checkStringLength(value_type, max_value_size);
}

class TableSchema : public RowSchema {
private:
  int _size;              // fixed size of each row
  size_t _schema_sig;     //schema fields hash
  MPI_Datatype _row_type; //type of entire row
  bool _is_testing = false;

public:
  std::shared_ptr<std::unordered_map<Field, uint64_t, FieldHasher>> _offsets;
  std::shared_ptr<std::unordered_map<Field, uint64_t, FieldHasher>> _max_unit;
  std::shared_ptr<std::unordered_map<Field, MPI_Datatype, FieldHasher>> _field_types;
  int size() {
    CHECK_GT(_size, 0);
    return _size;
  }
  size_t schema_sig() {
    CHECK_NE(_schema_sig, 0);
    return _schema_sig;
  }

  MPI_Datatype* getType() {
    return &_row_type;
  }

  bool exist(Field field) {
    CHECK_GT(fields.size(), 0);
    for (auto f : fields) {
      if (field_hasher.operator()(field) == field_hasher.operator()(f)) return true;
    }
    return false;
  }

  TableSchema(const RowSchema& schema) {
    this->fields = schema.fields;
    _offsets = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
    _max_unit = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
    _field_types = std::make_shared<std::unordered_map<Field, MPI_Datatype, FieldHasher>>();
    validSchema(schema);
    _schema_sig = schema_hasher.operator()(schema);
    _size = sizeof(size_t); // store _schema_sig
    for (size_t i = 0; i < schema.fields.size(); i++) {
      auto f = schema.fields.at(i);
      _offsets->emplace(f, _size);
      _max_unit->emplace(f, f.max_unit_size);
      _size += getFieldSize(f);
    }
    if (!_is_testing) {
      MPI_Type_contiguous(_size, MPI_CHAR, &_row_type);
      MPI_Type_commit(&_row_type);
    }
  }
  ~TableSchema() {
    if (!_is_testing) {
      MPI_Type_free(&_row_type);
    }
  }
};

} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_SCHEMA_H
