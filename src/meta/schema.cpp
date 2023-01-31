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
#include <fmt/core.h>

namespace surfingdb {
namespace meta {

void SchemaUtils::validSchema(const RowSchema& rowSchema) {
  CHECK_GT(rowSchema.fields.size(), 0);
  std::set<std::string> name_set;
  // force memory layout put primitive types before collective types
  bool enterCollectiveFields = false;
  for (auto& field : rowSchema.fields) {
    CHECK(name_set.find(field.name) == name_set.end());

    name_set.insert(field.name);

    if (field.type == RowType::LIST || field.type == RowType::MAP) {
      CHECK(field.list_type != RowType::LIST);
      CHECK(field.list_type != RowType::MAP);
      enterCollectiveFields = true;
    } else {
      CHECK(field.list_type == RowType::VOID);
      CHECK(field.map_key_type == RowType::VOID);
      CHECK(field.map_value_type == RowType::VOID);
      CHECK(!enterCollectiveFields);
    }
  }
}

size_t SchemaUtils::getFieldSize(const Field& f) {
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
    return sizeof(DOUBLE_TYPE);
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
    k.max_unit_size = f.max_map_key_unit_size;
    v.type = f.map_value_type;
    v.max_unit_size = v.max_map_value_unit_size;
    return getFieldSize(k) * f.max_unit_size + getFieldSize(v) * v.max_unit_size + HEADER_SIZE;
  }
  }
  assert(false);
}

uint64_t getTypeSize(const RowType::type type) {
  switch (type) {
  case RowType::BOOL:
    return sizeof(bool);
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

int16_t SchemaUtils::appendElements(RowSchema& r, const std::string& name, const RowType::type type, const uint64_t& max_element) {
  if (r.fields.size() == 0) {
    r.fields = std::vector<Field>();
  }

  // avoid duplicated name
  for (size_t i = 0; i < r.fields.size(); i++) {
    Field field = r.fields.at(i);
    CHECK(field.name != name);
  }

  uint64_t element_max = getTypeSize(type);

  Field f;
  // more than one element as list
  if (max_element > 1) {
    initListField(f, name, type, max_element, element_max);
  } else {
    initField(f, name, type, element_max);
  }
  r.fields.push_back(f);
  return r.fields.size();
}

int16_t SchemaUtils::appendPairs(RowSchema& r, const std::string& name, const RowType::type key_type, const RowType::type val_type, const uint64_t& max_element) {
  if (r.fields.size() == 0) {
    r.fields = std::vector<Field>();
  }
  // avoid duplicated name
  for (size_t i = 0; i < r.fields.size(); i++) {
    Field field = r.fields.at(i);
    CHECK(field.name != name);
  }

  uint64_t key_element_max = getTypeSize(key_type);
  uint64_t val_element_max = getTypeSize(val_type);
  Field f;
  initMapField(f, name, key_type, val_type, key_element_max, val_element_max, max_element);
  r.fields.push_back(f);
  return r.fields.size();
}

void SchemaUtils::initField(Field& field, const std::string& name, const RowType::type type, const uint64_t& max_size) {
  field.name = name;
  field.type = type;
  field.max_unit_size = max_size;
  // IF string should check max_size no more than MAX_STR_LEN
  CHECK(type != RowType::LIST);
  CHECK(type != RowType::MAP);
  checkStringLength(type, max_size);
}

void SchemaUtils::initListField(Field& field, const std::string& name, const RowType::type list_type, const uint64_t& max_list_size, const uint64_t& max_element_size) {
  field.name = name;
  field.type = RowType::LIST;
  field.list_type = list_type;
  field.max_unit_size = max_list_size;
  field.max_list_unit_size = max_element_size;
  CHECK(list_type != RowType::LIST);
  CHECK(list_type != RowType::MAP);
  checkStringLength(list_type, max_element_size);
}

void SchemaUtils::initMapField(Field& field, const std::string& name, const RowType::type key_type, const RowType::type value_type, const uint64_t& max_map_size, const uint64_t& max_key_size, const uint64_t& max_value_size) {
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

/**
 * convert to arrow schema types
 */
auto getArrowType(const RowType::type& type, const RowType::type& type1, const RowType::type& type2) {
  if (type == RowType::BOOL) {
    return arrow::boolean();
  } else if (type == RowType::INT) {
    return arrow::int32();
  } else if (type == RowType::LONG) {
    return arrow::int64();
  } else if (type == RowType::DOUBLE) {
    return arrow::float32();
  } else if (type == RowType::STRING) {
    return arrow::utf8();
  } else if (type == RowType::LIST) {
    return arrow::list(getArrowType(type1, RowType::VOID, RowType::VOID));
  } else if (type == RowType::MAP) {
    return arrow::map(getArrowType(type1, RowType::VOID, RowType::VOID), getArrowType(type2, RowType::VOID, RowType::VOID), true);
  } else {
    return arrow::null();
  }
}

TableSchema::TableSchema(const RowSchema& schema) {
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

  /**
   * arrow schema conversion
   */
  std::vector<std::shared_ptr<arrow::Field>> field_vector;
  for (auto j = 0; j < schema.fields.size(); j++) {
    auto fj = schema.fields.at(j);
    std::shared_ptr<arrow::Field> afield;
    if (fj.type == RowType::MAP) {
      afield = arrow::field(fj.name, getArrowType(fj.type, fj.map_key_type, fj.map_value_type));
    } else if (fj.type == RowType::LIST) {
      afield = arrow::field(fj.name, getArrowType(fj.type, fj.list_type, RowType::VOID));
    } else {
      afield = arrow::field(fj.name, getArrowType(fj.type, RowType::VOID, RowType::VOID));
    }
    field_vector.push_back(afield);
  }
  arrowSchema = arrow::schema(field_vector);
  CHECK(arrowSchema->num_fields() == schema.fields.size());
}

std::shared_ptr<arrow::Schema> TableSchema::getArrowSchema() {
  CHECK(this->arrowSchema->fields().size() > 0);
  return this->arrowSchema;
}

TableSchema::~TableSchema() {
  if (_type_set) {
    MPI_Type_free(&_row_type);
    for (auto s : *_field_types.get()) {
      MPI_Type_free(&s.second);
    }
  }
}

bool TableSchema::containField(Field field) {
  CHECK_GT(fields.size(), 0);
  for (auto f : fields) {
    if (field_hasher.operator()(field) == field_hasher.operator()(f)) return true;
  }
  return false;
}

MPI_Datatype* TableSchema::schemaMPIType() {
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
/**
 * covert surfing-db schema type to duckdb schema
 * @param f
 * @return
 */
inline std::string cloumnType(Field& f) {
  if (f.type == RowType::STRING || f.type == RowType::LIST || f.type == RowType::MAP) {
    return fmt::format("VARCHAR({})", SchemaUtils::getFieldSize(f));
  }
  if (f.type == RowType::INT) {
    return "INTEGER";
  }
  if (f.type == RowType::DOUBLE) {
    return "DOUBLE";
  }
  if (f.type == RowType::LONG) {
    return "BIGINT";
  }
  if (f.type == RowType::BOOL) {
    return "BOOLEAN";
  }
}

} // namespace meta
} // namespace surfingdb
