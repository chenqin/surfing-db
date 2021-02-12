//
// Created by cq on 1/7/21.
//

#ifndef SURFINGDB_ROW_H
#define SURFINGDB_ROW_H

#include "schema.h"
#pragma once

namespace surfingdb {
namespace table {

/**
 * build a continous memory buffer
 */
using namespace surfingdb::table::schema;
using std::hash;
using std::string;
using namespace surfingdb::table;

inline void sign(uint8_t* _payload, size_t _schema_sig) {
  CHECK_NOTNULL(_payload);
  memcpy(_payload, &_schema_sig, sizeof(size_t));
}

inline size_t readSig(uint8_t* _payload) {
  CHECK_NOTNULL(_payload);
  size_t result = 0;
  memcpy(&result, _payload, sizeof(size_t));
  return result;
}

/**
 * a large piece of memory to store all fields in a row
 * RowSchema is superset of normal row schema allow each field place in fixed
 * offset regarding to starting address, it helps MPI collective communication and cache loading faster
 * |SchemaSignature|Field1|Field2|Field3Count|Field3Value|PADDING|
 */
class RowBuffer {
private:
  Field _header;
  std::shared_ptr<TableSchema> schema_ptr;
  size_t _schema_sig;
  uint8_t* _payload; // consider using vector std::vector<uint8_t>
  std::vector<char> _vpayload;

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
      memcpy((DOUBLE_TYPE*)(_payload + offset), data, sizeof(DOUBLE_TYPE));
      break;
    }
      /*
       * |int_64_t length|yourstring balba\n0000|
       */
    case RowType::STRING: {
      // be careful here with truncation, char* requires extra char \0 to end
      char* str_ptr = (char*)data;

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
      DOUBLE_TYPE* double_ptr = (DOUBLE_TYPE*)(_payload + offset);
      memcpy(dataptr, double_ptr, sizeof(DOUBLE_TYPE));
      return sizeof(DOUBLE_TYPE);
    }
    case RowType::STRING: {
      // TODO(chenqin): use header
      int64_t* len = (int64_t*)(_payload + offset);
      char* char_ptr = (char*)(_payload + offset + sizeof(int64_t));
      //avoid truncation
      CHECK_EQ(*len, (int64_t)strlen(char_ptr));
      size_t resid = f.max_unit_size - strlen(char_ptr) - 1; //leave extra byte'\0'
      CHECK_GE(resid, 0);
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
  explicit RowBuffer(std::shared_ptr<TableSchema> schemaptr) {
    _header.type = RowType::LONG;
    _header.max_unit_size = SchemaUtils::getFieldSize(_header);
    CHECK_EQ(sizeof(uint64_t), SchemaUtils::getFieldSize(_header));
    this->schema_ptr = schemaptr;
    _schema_sig = schemaptr->schema_sig();
    CHECK_GT(schemaptr->size(), 0);
    _vpayload.resize(schemaptr->size());
    _payload = (uint8_t*)&_vpayload[0];
    sign(_payload, _schema_sig);
  }

  /**
   * only used in Temptable point to piece of memory to readRow/append fields
   * @param schema
   * @param payload
   */
  explicit RowBuffer(std::shared_ptr<TableSchema> schemaptr, uint8_t* payloadptr) {
    _header.type = RowType::LONG;
    _header.max_unit_size = SchemaUtils::getFieldSize(_header);
    CHECK_EQ(sizeof(uint64_t), SchemaUtils::getFieldSize(_header));
    this->schema_ptr = schemaptr;
    _schema_sig = schemaptr->schema_sig();
    CHECK_GT(schemaptr->size(), 0);
    CHECK_NE(payloadptr, _payload);
    // CHECK_EQ(_schema_sig, readSig(payloadptr));
    _payload = payloadptr;
  }

  ~RowBuffer() {
    _payload = nullptr;
    _vpayload.clear();
  }

  size_t schema_sig() {
    CHECK_NE(_schema_sig, 0);
    return this->_schema_sig;
  }

  size_t size() {
    return this->schema_ptr->size();
  }

  uint8_t* payload_ptr() {
    return this->_payload;
  }

  /**
  * copy readRow RowBuffer of given field
  * @param f
  * @param v
  */
  inline void read(const Field& f, Value& v) {
    CHECK_GE(schema_ptr->_offsets->size(), 0);
    CHECK(schema_ptr->_offsets->find(f) != schema_ptr->_offsets->end());
    uint64_t offset = schema_ptr->_offsets->at(f);
    size_t unit = read(f, v, offset);
    CHECK_LE(unit, schema_ptr->_max_unit->at(f));
  }
  /**
   * copy value into row buffer
   * @param f
   * @param v
   */
  void write(const Field& f, const Value& v) {
    uint64_t offset = schema_ptr->_offsets->at(f);
    CHECK_GT(offset, 0);
    write(f, v, offset);
  }
  /**
   * allow append to list and map elements
   * @param f
   * @param v
   * @param offset
   */
  void write(const Field& f, const Value& v, const uint64_t& offset) {
    switch (f.type) {
    case surfingdb::table::schema::RowType::VOID: {
      assert(false);
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
      DOUBLE_TYPE dv = (DOUBLE_TYPE) v.p_val.double_val;
      _pwrite(f, &dv, offset);
      break;
    }
    case RowType::STRING: {
      CHECK_LE(strlen(v.p_val.string_val.c_str()), f.max_unit_size - 1); // include extra \0
      _pwrite(f, v.p_val.string_val.c_str(), offset);
      break;
    }
    case RowType::LIST: {
      CHECK_LE(v.list_value.size(), (size_t)f.max_unit_size);

      int64_t size = v.list_value.size();
      _pwrite(_header, &size, offset);
      size_t list_offset = offset + sizeof(int64_t);

      //  |size|string|
      Field listField;
      listField.type = f.list_type;
      listField.max_unit_size = f.max_list_unit_size;
      for (auto pv : v.list_value) {
        //hard code
        Value item;
        item.p_val = pv;
        write(listField, item, list_offset);
        list_offset += listField.max_unit_size;
      }
      break;
    }
    case RowType::MAP: {
      CHECK_LE(v.map_value.size(), (size_t)f.max_unit_size);

      int64_t size = v.map_value.size();
      _pwrite(_header, &size, offset);
      size_t map_offset = offset + sizeof(int64_t);

      Field keyField, valueField;
      keyField.type = f.map_key_type;
      valueField.type = f.map_value_type;
      keyField.max_unit_size = f.max_map_key_unit_size;
      valueField.max_unit_size = f.max_map_value_unit_size;

      for (auto& pair : v.map_value) {
        Value key, value;
        key.p_val = pair.first;
        value.p_val = pair.second;
        write(keyField, key, map_offset);
        map_offset += keyField.max_unit_size;
        write(valueField, value, map_offset);
        map_offset += valueField.max_unit_size;
      }
      break;
    }
    }
  }

  inline size_t read(const Field& f, Value& v, const uint64_t& offset) {
    switch (f.type) {
    case surfingdb::table::schema::RowType::VOID: {
      return 0;
    }
    case surfingdb::table::schema::RowType::INT: {
      _pread(f, &v.p_val.int_val, offset);
      return 1;
    }
    case surfingdb::table::schema::RowType::BOOL: {
      _pread(f, &v.p_val.bool_val, offset);
      return 1;
    }
    case RowType::LONG: {
      _pread(f, &v.p_val.long_val, offset);
      return 1;
    }
    case RowType::DOUBLE: {
      DOUBLE_TYPE dv;
      _pread(f, &dv, offset);
      v.p_val.double_val = (double) dv;
      return 1;
    }
    case RowType::STRING: {
      std::vector<char> buff(f.max_unit_size);
      size_t strlen = _pread(f, &buff[0], offset);
      buff.resize(strlen);
      v.p_val.string_val = std::string(buff.data());
      return strlen;
    }
    case RowType::LIST: {
      v.list_value.clear();
      size_t _offset = offset;
      Field l;
      l.type = RowType::LONG;
      size_t len;
      _pread(l, &len, _offset);
      _offset += sizeof(int64_t);

      Field listField;
      listField.type = f.list_type;
      listField.max_unit_size = f.max_list_unit_size;
      for (size_t i = 0; i < len; i++) {
        Value listVal;
        read(listField, listVal, _offset);
        v.list_value.push_back(listVal.p_val);
        _offset += listField.max_unit_size;
      }
      return len;
    }
    case RowType::MAP: {
      v.map_value.clear();
      size_t _offset = offset;
      Field l;
      l.type = RowType::LONG;
      size_t len;
      _pread(l, &len, _offset);
      _offset += sizeof(int64_t);

      Field keyField, valueField;
      keyField.type = f.map_key_type;
      valueField.type = f.map_value_type;
      keyField.max_unit_size = f.max_map_key_unit_size;
      valueField.max_unit_size = f.max_map_value_unit_size;
      for (size_t i = 0; i < len; i++) {
        Value keyVal, valueVal;
        read(keyField, keyVal, offset);
        _offset += keyField.max_unit_size;
        read(valueField, valueVal, offset);
        _offset += valueField.max_unit_size;
        v.map_value.insert({ keyVal.p_val, valueVal.p_val });
      }
      return len;
    }
    }
    return -1;
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_ROW_H
