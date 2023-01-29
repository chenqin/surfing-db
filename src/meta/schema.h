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

#ifndef SURFINGDB_SCHEMA_H
#define SURFINGDB_SCHEMA_H
#include "meta/gen-cpp/schema_constants.h"
#include "meta/gen-cpp/schema_types.h"

#include <duckdb.hpp>
#include <glog/logging.h>
#include <iostream>
#include <mpi.h>
#include <stdlib.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <unordered_map>

#pragma once

namespace surfingdb {
namespace meta {

#define MAX_STR_LEN 2048
#define HEADER_SIZE sizeof(long)
#define MEM_PAGE_SIZE 1073741824 // 1GB
#define FLUSH_SIZE 10737418240   // 10GB
#define FILE_IO_VECTOR 8
#define SSD_CHUNK_SIZ 65536 // read/write ssd per 64KB chunk
#define DOUBLE_TYPE float   // thrift lack float type
#define FLUSH_DIR "/tmp/"
#define CONCURRENCY 2
/**
 * build a continous memory buffer
 */
using namespace surfingdb::meta::schema;
using std::hash;
using std::string;

struct FieldHasher {
  std::size_t operator()(const Field& k) const {
    using std::hash;
    using std::size_t;
    using std::string;
    std::size_t result = (hash<string>()(k.name));
    result ^= (hash<int>()(k.type)) << 1;
    result ^= (hash<int>()(k.list_type)) << 2;
    result ^= (hash<int>()(k.map_key_type)) << 3;
    result ^= (hash<int>()(k.map_value_type)) << 4;
    return result;
  }
};

struct SchemaHasher {
  std::size_t operator()(const RowSchema& k) const {
    FieldHasher field_hasher;
    using std::hash;
    using std::size_t;
    using std::string;
    std::size_t result = (hash<int>()(k.fields.size()));
    for (size_t i = 0; i < k.fields.size(); i++) {
      result = result ^ (field_hasher.operator()(k.fields.at(i)) >> i);
    }
    return result;
  }
};

struct ValueHasher {
  std::size_t operator()(const PValue& k) const {
    size_t result = 0;
    if (!k.string_val.empty()) result = hash<string>()(k.string_val);
    if (!k.int_val) result ^= hash<int>()(k.int_val) << 1;
    if (!k.long_val) result ^= hash<long>()(k.long_val) << 2;
    if (std::abs(k.double_val) <= 1e-5) result ^= hash<double>()(k.double_val) << 3;
    if (k.bool_val) result ^= hash<bool>()(k.bool_val) << 4;
    return result;
  }

  std::size_t operator()(const Value& k) const {
    size_t result = operator()(k.p_val);
    for (auto l : k.list_value) {
      result ^= operator()(l);
    }
    for (auto p : k.map_value) {
      result ^= operator()(p.first) << 1;
      result ^= operator()(p.second) << 2;
    }
    return result;
  }
};

class SchemaUtils {
public:
  /**
   * valid if schema is accepted
   * @param rowSchema
   */
  static void validSchema(const RowSchema& rowSchema);

  static size_t getFieldSize(const Field& f);

  static void checkStringLength(const RowType::type type,
                                const uint64_t& max_size) {
    if (type == RowType::STRING) {
      CHECK_LE(max_size, MAX_STR_LEN);
    }
  }
  /// @brief adding a new list or primitive field to schema
  /// @param r
  /// @param name
  /// @param type
  /// @param max_size
  static void addElements(RowSchema& r, const std::string& name, const RowType::type type, const uint64_t& max_element);

  /// @brief adding new map field to schema
  /// @param r
  /// @param name
  /// @param key_type
  /// @param val_type
  /// @param max_element
  static void addPairs(RowSchema& r, const std::string& name, const RowType::type key_type, const RowType::type val_type, const uint64_t& max_element);

  /**
   * helper function to init a primitive field
   * @param field
   * @param name
   * @param type
   * @param max_size
   */
  static void initField(Field& field,
                        const std::string& name,
                        const RowType::type type,
                        const uint64_t& max_size);

  /**
   * helper function to init a list field
   * @param field
   * @param name
   * @param type
   * @param max_size
   */
  static void initListField(Field& field,
                            const std::string& name,
                            const RowType::type list_type,
                            const uint64_t& max_list_size,
                            const uint64_t& max_element_size);

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
  static void initMapField(Field& field,
                           const std::string& name,
                           const RowType::type key_type,
                           const RowType::type value_type,
                           const uint64_t& max_map_size,
                           const uint64_t& max_key_size,
                           const uint64_t& max_value_size);
};

class TableSchema : public RowSchema {
private:
  std::string name;
  size_t _size;       // fixed size of each row
  size_t _schema_sig; // schema fields hash
  bool _type_set;
  MPI_Datatype _row_type; // type of entire row
  FieldHasher field_hasher;
  SchemaHasher schema_hasher;

public:
  std::shared_ptr<std::unordered_map<Field, uint64_t, FieldHasher>> _offsets;
  std::shared_ptr<std::unordered_map<Field, uint64_t, FieldHasher>> _max_unit;
  std::shared_ptr<std::unordered_map<Field, MPI_Datatype, FieldHasher>> _field_types;

  size_t rowSize() {
    CHECK_GT(_size, 0);
    return _size;
  }

  std::string getName() {
    CHECK(!this->name.empty());
    return this->name;
  }

  size_t signature() {
    CHECK_NE(_schema_sig, 0);
    return _schema_sig;
  }

  void updateRowSize() {
    _offsets = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
    _max_unit = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
    _field_types = std::make_shared<std::unordered_map<Field, MPI_Datatype, FieldHasher>>();
    _size = sizeof(size_t); // store _schema_sig
    for (size_t i = 0; i < fields.size(); i++) {
      // LOG(INFO) << fields.at(i).name;
      auto f = fields.at(i);
      _offsets->emplace(f, _size);
      _max_unit->emplace(f, f.max_unit_size);
      _size += SchemaUtils::getFieldSize(f);
    }
  }

  MPI_Datatype* schemaMPIType();

  void registerTable(std::shared_ptr<duckdb::Connection> connection, const std::string name);
  void registerIndex(std::shared_ptr<duckdb::Connection> connection, const std::string name, const Field& f);

  bool containField(Field field);

  TableSchema() {
    _size = 0;
    _schema_sig = 0;
    _type_set = false;
  }
  TableSchema(const RowSchema& schema);

  ~TableSchema();
};

} // namespace meta
} // namespace surfingdb
#endif // SURFINGDB_SCHEMA_H
