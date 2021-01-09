//
// Created by cq on 1/7/21.
//

#ifndef SURFINGDB_ROW_H
#define SURFINGDB_ROW_H

#include "table/gen-cpp/schema_types.h"

#include <glog/logging.h>
#include <iostream>
#include <jemalloc/jemalloc.h>
#include <stdlib.h>

#pragma once

namespace surfingdb {
namespace table {

#define MAX_STR_LEN 128
#define HEADER_SIZE sizeof(long)
/**
 * build a continous memory buffer
 */
using namespace surfingdb::table::schema;
/**
 * fixed memory layout buffer per schema
 */

struct FieldHasher {
  std::size_t operator()(const Field& k) const {
    using std::hash;
    using std::size_t;
    using std::string;

    return (hash<string>()(k.name));
  }
};

/**
 * valid if schema is accepted
 * @param rowSchema
 */
void validSchema(const RowSchema &rowSchema) {
  CHECK_GT(rowSchema.fields.size(), 0);
  std::set<std::string> name_set;
  for(auto& field : rowSchema.fields) {
    CHECK(name_set.find(field.name) ==name_set.end());

    name_set.insert(field.name);

    if(field.type == RowType::LIST || field.type == RowType::MAP) {
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
    return sizeof(char) * MAX_STR_LEN + HEADER_SIZE;
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
  CHECK(type != RowType::LIST);
  CHECK(type != RowType::MAP);
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
}

/**
 * a large piece of memory to store all fields in a row
 * RowSchema is superset of normal row schema allow each field place in fixed
 * offset regarding to starting address, it helps MPI collective communication and cache loading faster
 */
class RowBuffer {
private:
  Field header;
  std::shared_ptr<std::unordered_map<Field, uint64_t, FieldHasher>> _offsets;

  /**
   * those fields will be send to other processes as a Row
   * **/
  uint8_t* _payload; // consider using vector std::vector<uint8_t>
  size_t _size;

  inline void _pwrite(const Field& f, const void* data, const uint64_t& offset) {
    switch (f.type) {
    case surfingdb::table::schema::RowType::VOID: {
      break;
    }
    case surfingdb::table::schema::RowType::INT: {
      memcpy((int*)(_payload + offset), data, sizeof(int));
      break;
    }
    case surfingdb::table::schema::RowType::BOOL: {
      memcpy((bool*)(_payload + offset), data, sizeof(bool));
      break;
    }
    case RowType::LONG: {
      memcpy((long*)(_payload + offset), data, sizeof(long));
      break;
    }
    case RowType::DOUBLE: {
      memcpy((double*)(_payload + offset), data, sizeof(double));
      break;
    }
      /*
       * |int_64_t length|yourstring balba\n0000|
       */
    case RowType::STRING: {
      // be careful here with truncation, char* requires extra char \0 to end
      char* str_ptr = (char*)data;
      CHECK_LE(strlen(str_ptr), sizeof(char) * f.max_unit_size - 1);

      size_t length = strlen(str_ptr);
      memcpy((int64_t*)(_payload + offset), &(length), sizeof(int64_t));
      memcpy((char*)(_payload + offset + sizeof(int64_t)), data, length + 1);
      memset((char*)(_payload + offset + sizeof(int64_t) + length + 1), 0, f.max_unit_size - length - 1);
      break;
    }
    case RowType::LIST: {
      assert(false);
    }
    case RowType::MAP: {
      assert(false);
    }
    }
  }

  inline size_t _pread(const Field& f, void* dataptr, const uint64_t& offset) {
    switch (f.type) {
    case surfingdb::table::schema::RowType::VOID: {
      assert(false);
    }
    case surfingdb::table::schema::RowType::INT: {
      int* int_ptr = (int*)(_payload + offset);
      memcpy(dataptr, int_ptr, sizeof(int));
      return sizeof(int);
    }
    case surfingdb::table::schema::RowType::BOOL: {
      bool* bool_ptr = (bool*)(_payload + offset);
      memcpy(dataptr, bool_ptr, sizeof(bool));
      return sizeof(bool);
    }
    case RowType::LONG: {
      long* long_ptr = (long*)(_payload + offset);
      memcpy(dataptr, long_ptr, sizeof(long));
      return sizeof(long);
    }
    case RowType::DOUBLE: {
      double* double_ptr = (double*)(_payload + offset);
      memcpy(dataptr, double_ptr, sizeof(double));
      return sizeof(double);
    }
    case RowType::STRING: {
      // TODO(chenqin): use header
      int64_t* len = (int64_t*)(_payload + offset);
      char* char_ptr = (char*)(_payload + offset + sizeof(int64_t));
      //avoid truncation
      assert(*len == (int64_t)strlen(char_ptr));
      memcpy(dataptr, char_ptr, strlen(char_ptr));
      return *len;
    }
    case RowType::LIST: {
      assert(false);
    }
    case RowType::MAP: {
      assert(false);
    }
    }
    return 0;
  }

public:
  explicit RowBuffer(const RowSchema& schema) {
    validSchema(schema);
    header.type = RowType::LONG;
    header.max_unit_size = getFieldSize(header);

    CHECK_EQ(sizeof(uint64_t), getFieldSize(header));

    _offsets = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
    _size = 0;
    for (size_t i = 0; i < schema.fields.size(); i++) {
      auto f = schema.fields.at(i);
      this->_offsets.get()->emplace(f, _size);
      _size += getFieldSize(f);
    }

    CHECK_GT(_size, 0);
    _payload = static_cast<uint8_t*>(malloc(_size));
    memset(_payload, 0, _size);
  }

  ~RowBuffer() {
    _size = 0;
    _offsets->clear();
    free(_payload);
  }

  /**
  * copy read RowBuffer of given field
  * @param f
  * @param v
  */
  void read(const Field& f, Value& v) {
    uint64_t offset = _offsets->at(f);
    read(f, v, offset);
  }
  /**
   * copy value into row buffer
   * @param f
   * @param v
   */
  void write(const Field& f, const Value& v) {
    uint64_t offset = _offsets->at(f);
    write(f, v, offset);
  }

  inline void write(const Field& f, const Value& v, uint64_t& offset) {
    switch (f.type) {
    case surfingdb::table::schema::RowType::VOID: {
      assert(false);
      break;
    }
    case surfingdb::table::schema::RowType::INT: {
      _pwrite(f, &v.p_val.int_val, offset);
      break;
    }
    case surfingdb::table::schema::RowType::BOOL: {
      _pwrite(f, &v.p_val.bool_val, offset);
      break;
    }
    case RowType::LONG: {
      _pwrite(f, &v.p_val.long_val, offset);
      break;
    }
    case RowType::DOUBLE: {
      _pwrite(f, &v.p_val.double_val, offset);
      break;
    }
    case RowType::STRING: {
      _pwrite(f, v.p_val.string_val.c_str(), offset);
      break;
    }
    case RowType::LIST: {
      //guard overflow
      CHECK_LE(v.list_value.size(), (size_t)f.max_list_unit_size);

      int64_t size = v.list_value.size();
      _pwrite(header, &size, offset);
      offset += sizeof(int64_t);

      //  |size|string|
      Field listField;
      listField.type = f.list_type;
      listField.max_unit_size = getFieldSize(listField);
      for (auto pv : v.list_value) {
        //hard code
        Value item;
        item.p_val = pv;
        write(listField, item, offset);
        offset += listField.max_unit_size;
      }
      break;
    }
    case RowType::MAP: {
      assert(false);
    }
    }
  }

  inline void read(const Field& f, Value& v, uint64_t& offset) {
    switch (f.type) {
    case surfingdb::table::schema::RowType::VOID: {
      break;
    }
    case surfingdb::table::schema::RowType::INT: {
      _pread(f, &v.p_val.int_val, offset);
      break;
    }
    case surfingdb::table::schema::RowType::BOOL: {
      _pread(f, &v.p_val.bool_val, offset);
      break;
    }
    case RowType::LONG: {
      _pread(f, &v.p_val.long_val, offset);
      break;
    }
    case RowType::DOUBLE: {
      _pread(f, &v.p_val.double_val, offset);
      break;
    }
    case RowType::STRING: {
      std::vector<char> buff(f.max_unit_size);
      size_t strlen = _pread(f, &buff[0], offset);
      buff.resize(strlen);
      v.p_val.string_val = std::string(buff.data());
      break;
    }
    case RowType::LIST: {
      Field l;
      l.type = RowType::LONG;
      long len;
      _pread(l, &len, offset);
      offset += sizeof(int64_t);

      Field listItem;
      listItem.type = f.list_type;
      for (int i = 0; i < len; i++) {
        Value item;
        read(listItem, item, offset);
        v.list_value.push_back(item.p_val);
        offset += (size_t)getFieldSize(listItem);
      }
      break;
    }
    case RowType::MAP: {
      std::shared_ptr<char> map_ptr((char*)(_payload + offset));
      break;
    }
    }
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_ROW_H
