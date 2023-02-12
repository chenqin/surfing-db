/*
 * Copyright Chen Qin on 12/30/20.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "mrow.h"

namespace surfingdb {
namespace table {

mrow::mrow(std::shared_ptr<meta::mschema> schemaptr) {
  _header.type = RowType::LONG;
  _header.max_unit_size = meta::SchemaUtils::getFieldSize(_header);
  CHECK_EQ(sizeof(uint64_t), meta::SchemaUtils::getFieldSize(_header));
  this->schema_ptr = schemaptr;
  _schema_sig = schemaptr->signature();
  CHECK_GT(schemaptr->rowSize(), 0);
  _vpayload.resize(schemaptr->rowSize());
  _payload = (uint8_t*)&_vpayload[0];
}

mrow::mrow(std::shared_ptr<meta::mschema> schemaptr, uint8_t* payloadptr) {
  _header.type = RowType::LONG;
  _header.max_unit_size = meta::SchemaUtils::getFieldSize(_header);
  CHECK_EQ(sizeof(uint64_t), meta::SchemaUtils::getFieldSize(_header));
  this->schema_ptr = schemaptr;
  _schema_sig = schemaptr->signature();
  CHECK_GT(schemaptr->rowSize(), 0);
  CHECK_NE(payloadptr, _payload);
  _vpayload.clear();
  _vpayload.shrink_to_fit();
  _payload = payloadptr;
}

mrow::~mrow() {
  _payload = nullptr;
  _vpayload.clear();
  _vpayload.shrink_to_fit();
}

size_t mrow::row_size() {
  return this->schema_ptr->rowSize();
}

uint8_t* mrow::payload_ptr() {
  return this->_payload;
}

size_t mrow::schema_sig() {
  CHECK_NE(_schema_sig, 0);
  return this->_schema_sig;
}

size_t mrow::read(const Field& f, Value& v) {
  CHECK_GE(schema_ptr->_offsets->size(), 0);
  CHECK(schema_ptr->_offsets->find(f) != schema_ptr->_offsets->end());
  uint64_t offset = schema_ptr->_offsets->at(f);
  size_t unit = read(f, v, offset);
  CHECK_LE(unit, schema_ptr->_max_unit->at(f));
  return unit;
}

void mrow::write(const Field& f, const Value& v) {
  uint64_t offset = schema_ptr->_offsets->at(f);
  CHECK_GT(offset, 0);
  write(f, v, offset);
}

void mrow::write(const Field& f, const Value& v, const uint64_t& offset) {
  switch (f.type) {
  case surfingdb::meta::schema::RowType::VOID: {
    assert(false);
  }
  case surfingdb::meta::schema::RowType::INT: {
    _pwrite(f, &v.p_val.int_val, offset);
    break;
  }
  case surfingdb::meta::schema::RowType::BOOL: {
    _pwrite(f, &v.p_val.bool_val, offset);
    break;
  }
  case RowType::LONG: {
    _pwrite(f, &v.p_val.long_val, offset);
    break;
  }
  case RowType::DOUBLE: {
    DOUBLE_TYPE dv = (DOUBLE_TYPE)v.p_val.double_val;
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
      // hard code
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

size_t mrow::read(const Field& f, Value& v, const uint64_t& offset) {
  switch (f.type) {
  case surfingdb::meta::schema::RowType::VOID: {
    return 0;
  }
  case surfingdb::meta::schema::RowType::INT: {
    _pread(f, &v.p_val.int_val, offset);
    return 1;
  }
  case surfingdb::meta::schema::RowType::BOOL: {
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
    v.p_val.double_val = (double)dv;
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

void mrow::_pwrite(const Field& f, const void* data, const uint64_t& offset) {
  switch (f.type) {
  case surfingdb::meta::schema::RowType::VOID: {
    break;
  }
  case surfingdb::meta::schema::RowType::INT: {
    memcpy((int*)(_payload + offset), data, sizeof(int));
    break;
  }
  case surfingdb::meta::schema::RowType::BOOL: {
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

size_t mrow::_pread(const Field& f, void* dataptr, const uint64_t& offset) {
  switch (f.type) {
  case surfingdb::meta::schema::RowType::VOID: {
    assert(false);
  }
  case surfingdb::meta::schema::RowType::INT: {
    int* int_ptr = (int*)(_payload + offset);
    memcpy(dataptr, int_ptr, sizeof(int));
    return sizeof(int);
  }
  case surfingdb::meta::schema::RowType::BOOL: {
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
    // avoid truncation
    CHECK_EQ(*len, (int64_t)strlen(char_ptr));
    size_t resid = f.max_unit_size - strlen(char_ptr) - 1; // leave extra byte'\0'
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
std::vector<size_t> mrow::readLen(const Field& f, size_t offset) {
  std::vector<size_t> lens;
  switch (f.type) {
  case surfingdb::meta::schema::RowType::VOID: {
    return lens;
  }
  case surfingdb::meta::schema::RowType::INT: {
    lens.push_back(1);
    return lens;
  }
  case surfingdb::meta::schema::RowType::BOOL: {
    lens.push_back(1);
    return lens;
  }
  case RowType::LONG: {
    lens.push_back(1);
    return lens;
  }
  case RowType::DOUBLE: {
    lens.push_back(1);
    return lens;
  }
  case RowType::STRING: {
    size_t _offset = offset;
    Field l;
    l.type = RowType::LONG;
    size_t len;
    _pread(l, &len, _offset);
    lens.push_back(len);
    return lens;
  }
  case RowType::LIST: {
    size_t _offset = offset;
    Field l;
    l.type = RowType::LONG;
    size_t len;
    _pread(l, &len, _offset);
    _offset += sizeof(int64_t);
    lens.push_back(len);

    Field listField;
    listField.type = f.list_type;
    listField.max_unit_size = f.max_list_unit_size;
    size_t list_len = 0;

    for (size_t i = 0; i < len; i++) {
      Value listVal;
      std::vector<size_t> l1 = readLen(listField, _offset);
      if (list_len < l1.at(0)) {
        list_len = l1.at(0);
      }
      _offset += listField.max_unit_size;
    }
    lens.push_back(list_len);
    return lens;
  }
  case RowType::MAP: {
    size_t _offset = offset;
    Field l;
    l.type = RowType::LONG;
    size_t len;
    _pread(l, &len, _offset);
    _offset += sizeof(int64_t);
    lens.push_back(len);

    Field keyField, valueField;
    keyField.type = f.map_key_type;
    valueField.type = f.map_value_type;
    keyField.max_unit_size = f.max_map_key_unit_size;
    valueField.max_unit_size = f.max_map_value_unit_size;
    size_t key_len = 0;
    size_t value_len = 0;
    for (size_t i = 0; i < len; i++) {
      Value keyVal, valueVal;
      read(keyField, keyVal, offset);
      std::vector<size_t> m1 = readLen(keyField, _offset);
      if (key_len < m1.at(0)) {
        key_len = m1.at(0);
      }
      _offset += keyField.max_unit_size;

      std::vector<size_t> m2 = readLen(valueField, _offset);
      if (value_len < m2.at(0)) {
        value_len = m2.at(0);
      }
      _offset += valueField.max_unit_size;
    }
    lens.push_back(key_len);
    lens.push_back(value_len);
    return lens;
  }
  }
}
} // namespace table
} // namespace surfingdb