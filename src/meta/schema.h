//
// Created by cq on 1/10/21.
//

#ifndef SURFINGDB_SCHEMA_H
#define SURFINGDB_SCHEMA_H
#include "meta/gen-cpp/schema_constants.h"
#include "meta/gen-cpp/schema_types.h"

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

#define MAX_STR_LEN 128
#define HEADER_SIZE sizeof(long)
#define MEM_PAGE_SIZE 1073741824 // 1GB
#define FLUSH_SIZE 10737418240 // 10GB
#define FILE_IO_VECTOR 8
#define SSD_CHUNK_SIZ 65536 // read/write ssd per 64KB chunk
#define DOUBLE_TYPE float //thrift lack float type
#define FLUSH_DIR "/tmp/"
#define CONCURRENCY 3
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

    return (hash<string>()(k.name));
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
  size_t _primitive_size; // fixed size of primitive fields
  size_t _size;          // fixed size of each row
  size_t _schema_sig; //schema fields hash
  bool _type_set;
  MPI_Datatype _row_type; //type of entire row
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

  size_t rowPrimitiveSize() {
    CHECK_GT(_primitive_size, 0);
    CHECK_LE(_primitive_size, _size);
    return _primitive_size;
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
      //LOG(INFO) << fields.at(i).name;
      auto f = fields.at(i);
      _offsets->emplace(f, _size);
      _max_unit->emplace(f, f.max_unit_size);
      _size += SchemaUtils::getFieldSize(f);
    }
  }

  MPI_Datatype* schemaMPIType();

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
#endif //SURFINGDB_SCHEMA_H
