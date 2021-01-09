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

//    return (hash<string>()(k.string_val)) ^ (hash<int>()(k.int_val) >> 1) ^ (hash<long>()(k.long_val) >> 2) ^ (hash<double>()(k.double_val) >> 3) ^ (hash<double>()(k.bool_val) >> 4);

namespace surfingdb {
namespace table {

#define MAX_STR_LEN 128
#define HEADER_SIZE sizeof(long)
/**
 * build a continous memory buffer
 */
using namespace surfingdb::table::schema;
using std::hash;
using std::string;
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

inline MPI_Datatype getFieldMPIType(const Field& f) {
  switch (f.type) {

  case RowType::VOID: {
    return MPI_DATATYPE_NULL;
  }
  case RowType::BOOL: {
    return MPI_C_BOOL;
  }
  case RowType::INT: {
    return MPI_INT;
  }
  case RowType::LONG: {
    return MPI_LONG;
  }
  case RowType::DOUBLE: {
    return MPI_DOUBLE;
  }
    //  |size|string|
  case RowType::STRING: {
    // max support string column MAX_STR_LEN character
    return MPI_CHAR;
  }
    //  |size|array|
  case RowType::LIST: {
    return MPI_CHAR;
  }
    // | size | key val key val|
  case RowType::MAP: {
    return MPI_CHAR;
  }
  }
  assert(false);
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

Field _header;
// TODO(add map
std::shared_ptr<std::unordered_map<Field, uint64_t, FieldHasher>> _offsets;
/**
 * a large piece of memory to store all fields in a row
 * RowSchema is superset of normal row schema allow each field place in fixed
 * offset regarding to starting address, it helps MPI collective communication and cache loading faster
 */
class RowBuffer {
private:
  /**
   * those fields will be send to other processes as a Row
   * **/
  size_t _schema_sig; // hash of schema fields
  MPI_Datatype _row_type;
  size_t _size;      // size of payload
  uint8_t* _payload; // consider using vector std::vector<uint8_t>

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
      CHECK_EQ(*len, (int64_t)strlen(char_ptr));
      size_t resid = f.max_unit_size - strlen(char_ptr);
      CHECK_GE(resid, 0);
      memcpy(dataptr, char_ptr, strlen(char_ptr));
      memset(_payload + strlen(char_ptr), 0, resid);
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
    //hardcoded
    _schema_sig = 1;
    _header.type = RowType::LONG;
    _header.max_unit_size = getFieldSize(_header);

    CHECK_EQ(sizeof(uint64_t), getFieldSize(_header));

    _offsets = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
    _size = 0;
    for (size_t i = 0; i < schema.fields.size(); i++) {
      auto f = schema.fields.at(i);
      _offsets.get()->emplace(f, _size);
      _size += getFieldSize(f);
    }

    CHECK_GT(_size, 0);
    _payload = static_cast<uint8_t*>(malloc(_size));
    memset(_payload, 0, _size);
  }

  MPI_Datatype row_type() {
    return _row_type;
  }

  void reg_row_type(const RowSchema& rowSchema) {
    int count = rowSchema.fields.size() + 3; // sig & row_type & _size
    int array_of_blocklengths[count];
    MPI_Aint array_of_displacements[count];
    MPI_Datatype array_of_types[count];
    array_of_types[0] = array_of_types[2] = MPI_LONG;
    array_of_types[1] = MPI_INT;
    array_of_blocklengths[0] = array_of_blocklengths[1] = array_of_blocklengths[2] = 1;
    array_of_displacements[0] = offsetof(RowBuffer, _schema_sig);
    array_of_displacements[1] = offsetof(RowBuffer, _row_type);
    array_of_displacements[2] = offsetof(RowBuffer, _size);
    MPI_Aint offset = offsetof(RowBuffer, _payload); // base address
    int i = 3;
    for (const auto& f : rowSchema.fields) {
      array_of_types[i] = getFieldMPIType(f);
      if (array_of_types[i] != MPI_CHAR) {
        array_of_blocklengths[i] = 1;
      } else {
        //use array of char as container type
        array_of_blocklengths[i] = getFieldSize(f);
      }
      array_of_displacements[i] = offset;
      offset += getFieldSize(f);
      ++i;
    }
    //for(int j = 0 ; j < count ; j++) {
    //  LOG(INFO) << array_of_blocklengths[j] << " " << array_of_displacements[j] << " " << array_of_types[j];
    //}
    CHECK_EQ(i, count);
    MPI_Datatype tmp_type;
    MPI_Aint lb, extent;
    MPI_Type_create_struct(count, array_of_blocklengths, array_of_displacements,
                           array_of_types, &tmp_type);
    MPI_Type_get_extent(tmp_type, &lb, &extent);
    MPI_Type_create_resized(tmp_type, lb, extent, &_row_type);
    MPI_Type_commit(&_row_type);
  }

  ~RowBuffer() {
    _size = 0;
    _offsets->clear();
    free(_payload);
  }

  size_t size() {
    return this->_size;
  }

  uint8_t* payload() {
    return this->_payload;
  }

  /**
  * copy read RowBuffer of given field
  * @param f
  * @param v
  */
  void read(const Field& f, Value& v) {
    CHECK_NOTNULL(_offsets);
    CHECK_GE(_offsets->size(), 0);
    CHECK(_offsets->find(f) != _offsets->end());
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
      CHECK_LE(v.list_value.size(), (size_t)f.max_unit_size);

      int64_t size = v.list_value.size();
      _pwrite(_header, &size, offset);
      offset += sizeof(int64_t);

      //  |size|string|
      Field listField;
      listField.type = f.list_type;
      listField.max_unit_size = f.max_list_unit_size;
      for (auto pv : v.list_value) {
        //hard code
        Value item;
        item.p_val = pv;
        write(listField, item, offset);
        offset += listField.max_unit_size;
      }
      int resid = f.max_unit_size - v.list_value.size();
      memset(_payload + offset, 0, resid * listField.max_unit_size);
      break;
    }
    case RowType::MAP: {
      CHECK_LE(v.map_value.size(), (size_t)f.max_unit_size);
      int64_t size = v.map_value.size();
      _pwrite(_header, &size, offset);
      offset += sizeof(int64_t);

      Field keyField, valueField;
      keyField.type = f.map_key_type;
      valueField.type = f.map_value_type;
      keyField.max_unit_size = f.max_map_key_unit_size;
      valueField.max_unit_size = f.max_map_value_unit_size;

      for (auto& pair : v.map_value) {
        Value key, value;
        key.p_val = pair.first;
        value.p_val = pair.second;
        write(keyField, key, offset);
        offset += keyField.max_unit_size;
        write(valueField, value, offset);
        offset += valueField.max_unit_size;
      }
      int resid = f.max_unit_size - v.map_value.size();
      memset(_payload + offset, 0, resid * (f.max_map_key_unit_size + f.max_map_value_unit_size));
      break;
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

      Field listField;
      listField.type = f.list_type;
      listField.max_unit_size = f.max_list_unit_size;
      for (int i = 0; i < len; i++) {
        Value listVal;
        read(listField, listVal, offset);
        v.list_value.push_back(listVal.p_val);
        offset += listField.max_unit_size;
      }
      break;
    }
    case RowType::MAP: {
      Field l;
      l.type = RowType::LONG;
      long len;
      _pread(l, &len, offset);
      offset += sizeof(int64_t);

      Field keyField, valueField;
      keyField.type = f.map_key_type;
      valueField.type = f.map_value_type;
      keyField.max_unit_size = f.max_map_key_unit_size;
      valueField.max_unit_size = f.max_map_value_unit_size;
      for (int i = 0; i < len; i++) {
        Value keyVal, valueVal;
        read(keyField, keyVal, offset);
        offset += keyField.max_unit_size;
        read(valueField, valueVal, offset);
        offset += valueField.max_unit_size;
        v.map_value.insert({ keyVal.p_val, valueVal.p_val });
      }
      break;
    }
    }
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_ROW_H
