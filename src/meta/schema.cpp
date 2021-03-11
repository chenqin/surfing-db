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

TableSchema::TableSchema(const RowSchema& schema) {
  _type_set = false;
  _offsets = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
  _max_unit = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
  _field_types = std::make_shared<std::unordered_map<Field, MPI_Datatype, FieldHasher>>();
  SchemaUtils::validSchema(schema);
  _schema_sig = schema_hasher.operator()(schema);
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
        MPI_Type_hvector(1, blocklength, displ, MPI_CHAR, &type);
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
			if(f.type == RowType::STRING || f.type == RowType::LIST || f.type == RowType::MAP) {
				return fmt::format("VARCHAR({})", SchemaUtils::getFieldSize(f));
			}
			if(f.type == RowType::INT) {
				return "INTEGER";
			}
			if(f.type == RowType::DOUBLE) {
				return "DOUBLE";
			}
			if (f.type == RowType::LONG) {
				return "BIGINT";
			}
			if(f.type == RowType::BOOL) {
				return "BOOLEAN";
			}
		}

		void TableSchema::registerTable(std::shared_ptr<duckdb::Connection> connection, std::string name) {
			std::string statement = fmt::format("CREATE TABLE {} (", name);
			for(auto f : fields) {
				statement  += fmt::format("{} {}, ", f.name, cloumnType(f));
			}
			statement = statement.substr (0, statement.size()-2) + ")";
			LOG(INFO) << statement;
			connection->Query(statement);
		}


} // namespace meta
} // namespace surfingdb