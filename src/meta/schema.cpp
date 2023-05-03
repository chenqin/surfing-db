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

#include "schema.h"

namespace surfingdb {
namespace meta {

void SchemaUtils::validSchema(const RowSchema& rowSchema) {
  CHECK_GT(rowSchema.fields.size(), 0);
}

size_t SchemaUtils::getFieldSize(const Field& f) {
  switch (f.type) {

  case RowType::VOID: {
    return 0;
  }
  case RowType::BOOL: {
    return sizeof(bool);
  }
  case RowType::CHAR: {
    return sizeof(char);
  }
  case RowType::INT: {
    return sizeof(int);
  }
  case RowType::LONG: {
    return sizeof(long);
  }
  case RowType::DOUBLE: {
    return sizeof(DOUBLE_TYPE);
  }
  case RowType::STRING: {
    return MAX_STR_LEN;
  }
    //  |size|array|
  case RowType::LIST: {
    Field l;
    l.type = f.list_type;
    size_t units = f.max_unit_size;
    return getFieldSize(l) * units + HEADER_SIZE;
  }
    // | size | key val key val|
  case RowType::MAP: {
    Field k, v;
    k.type = f.map_key_type;
    v.type = f.map_value_type;
    size_t units = f.max_unit_size;
    /**
     * @brief size of each key , value pair multiple by number of pairs in map
     * HEADER stores number of pairs
     *
     */
    return (getFieldSize(k) + getFieldSize(v)) * units + HEADER_SIZE;
  }
  }
  assert(false);
}

uint64_t getTypeSize(const RowType::type type) {
  switch (type) {
  case RowType::BOOL:
    return sizeof(bool);
  case RowType::CHAR:
    return sizeof(char);
  case RowType::INT:
    return sizeof(int32_t);
  case RowType::LONG:
    return sizeof(int64_t);
  case RowType::DOUBLE:
    return sizeof(DOUBLE_TYPE);
  case RowType::STRING:
    return MAX_STR_LEN;
  default:
    CHECK(false);
    return -1;
  }
}

Field SchemaUtils::initField(RowSchema& r, const std::string& name, const RowType::type type, const uint64_t& max_size) {
  Field field;
  field.name = name;
  field.type = type;
  field.max_unit_size = type != RowType::STRING ? SchemaUtils::getFieldSize(field) : max_size;
  CHECK(type != RowType::LIST);
  CHECK(type != RowType::MAP);
  checkStringLength(type, max_size);
  r.fields.push_back(field);
  return field;
}

Field SchemaUtils::initListField(RowSchema& r, const std::string& name, const RowType::type list_type, const uint64_t& max_list_size, const uint64_t& max_element_size) {
  Field field;
  field.name = name;
  field.type = RowType::LIST;
  field.list_type = list_type;
  field.max_unit_size = max_list_size;
  Field listfield;
  listfield.type = list_type;
  listfield.max_unit_size = max_element_size;
  field.max_list_unit_size = list_type != RowType::STRING ? SchemaUtils::getFieldSize(listfield) : max_element_size;
  CHECK(list_type != RowType::LIST);
  CHECK(list_type != RowType::MAP);
  checkStringLength(list_type, max_element_size);
  r.fields.push_back(field);
  return field;
}

Field SchemaUtils::initMapField(RowSchema& r, const std::string& name, const RowType::type key_type, const RowType::type value_type, const uint64_t& max_pair_count, const uint64_t& max_key_size, const uint64_t& max_value_size) {
  Field field, keyvield, valuefield;
  keyvield.type = key_type;
  keyvield.max_unit_size = max_key_size;
  valuefield.type = value_type;
  valuefield.max_unit_size = max_value_size;

  field.name = name;
  field.type = RowType::MAP;
  field.map_key_type = key_type;
  field.map_value_type = value_type;
  field.max_unit_size = max_pair_count;
  field.max_map_key_unit_size = key_type != RowType::STRING ? SchemaUtils::getFieldSize(keyvield) : max_key_size;
  field.max_map_value_unit_size = value_type != RowType::STRING ? SchemaUtils::getFieldSize(valuefield) : max_value_size;
  CHECK(key_type != RowType::LIST);
  CHECK(key_type != RowType::MAP);
  CHECK(value_type != RowType::LIST);
  CHECK(value_type != RowType::MAP);
  checkStringLength(key_type, max_key_size);
  checkStringLength(value_type, max_value_size);
  r.fields.push_back(field);
  return field;
}

mschema::mschema(const RowSchema& schema) {
  _type_set = false;
  _offsets = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
  _max_unit = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
  _field_types = std::make_shared<std::unordered_map<Field, MPI_Datatype, FieldHasher>>();
  SchemaUtils::validSchema(schema);
  _schema_sig = schema_hasher.operator()(schema);
  name = std::to_string(_schema_sig);
  _size = sizeof(size_t); // store _schema_sig

  // allocate primitive types first CPU cache friendly
  for (size_t i = 0; i < schema.fields.size(); i++) {
    auto f = schema.fields.at(i);
    if (f.type == RowType::LIST || f.type == RowType::MAP) continue;
    this->fields.push_back(f);
    _offsets->emplace(f, _size);
    _max_unit->emplace(f, f.max_unit_size);
    _size += SchemaUtils::getFieldSize(f);
  }

  // collective types
  for (size_t i = 0; i < schema.fields.size(); i++) {
    auto f = schema.fields.at(i);
    if (f.type != RowType::LIST && f.type != RowType::MAP) continue;
    this->fields.push_back(f);
    _offsets->emplace(f, _size);
    _max_unit->emplace(f, f.max_unit_size);
    _size += SchemaUtils::getFieldSize(f);
  }
}

mschema::~mschema() {
  if (_type_set) {
    MPI_Type_free(&_row_type);
    for (auto s : *_field_types.get()) {
      MPI_Type_free(&s.second);
    }
  }
}

bool mschema::containField(Field field) {
  CHECK_GT(fields.size(), 0);
  for (auto f : fields) {
    if (field_hasher.operator()(field) == field_hasher.operator()(f)) return true;
  }
  return false;
}

MPI_Datatype* mschema::schemaMPIType() {
#pragma omp critical
  {
    if (!_type_set) {
      _type_set = true;

      for (size_t i = 0; i < fields.size(); i++) {
        auto f = fields.at(i);
        MPI_Datatype type;
        int blocklength;
        MPI_Aint displ;
        blocklength = SchemaUtils::getFieldSize(f);
        displ = _size - SchemaUtils::getFieldSize(f);
        MPI_Type_vector(1, blocklength, displ, MPI_CHAR, &type);
        MPI_Type_commit(&type);
        _field_types->insert({ f, type });
      }
      MPI_Type_contiguous(_size, MPI_CHAR, &_row_type);
      MPI_Type_commit(&_row_type);
    }
  }
  return &_row_type;
}
} // namespace meta
} // namespace surfingdb
