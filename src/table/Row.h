//
// Created by cq on 1/7/21.
//

#ifndef SURFINGDB_ROW_H
#define SURFINGDB_ROW_H

#include "table/gen-cpp/schema_types.h"

#include <glog/logging.h>
#include <iostream>
#include <stdlib.h>

#pragma once

namespace surfingdb {
namespace table {
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

    return ((hash<string>()(k.name)
             ^ (hash<int>()(k.type) << 1))
            >> 1)
           ^ (hash<int>()(k.list_type) << 1);
  }
};
/**
 * a large piece of memory to store all fields in a row
 * RowSchema is superset of normal row schema allow each field place in fixed
 * offset regarding to starting address, it helps MPI collective communication and cache loading faster
 */
class RowBuffer {
private:
  uint8_t* _payload;
  size_t _size;
  std::shared_ptr<std::unordered_map<Field, uint64_t, FieldHasher>> _offsets;

public:
  explicit RowBuffer(const RowSchema& schema) {
    _offsets = std::make_shared<std::unordered_map<Field, uint64_t, FieldHasher>>();
    _size = 0;
    for (size_t i = 0; i < schema.fields.size(); i++) {
      auto f = schema.fields.at(i);
      this->_offsets.get()->emplace(f, _size);
      if (f.type == RowType::MAP) {
        // don't know
      } else if (f.list_type != RowType::VOID) {
        _size += schema.fields.at(i).unit_size * (f.list_type == RowType::INT ? sizeof(int) : sizeof(long));
      } else {
        _size += schema.fields.at(i).unit_size;
      }
    }
    _payload = static_cast<uint8_t*>(malloc(_size));
  }

  ~RowBuffer() {
    _size = 0;
    _offsets->clear();
    free(_payload);
  }
  /**
   * copy value into row buffer
   * @param f
   * @param v
   */
  void write(const Field& f, const Value& v) {
    uint64_t offset = _offsets->at(f);
    switch (f.type) {
    case surfingdb::table::schema::RowType::VOID: {
      break;
    }
    case surfingdb::table::schema::RowType::INT: {
      memcpy((int*)(_payload + offset), &(v.p_val.int_val), sizeof(int));
      break;
    }
    case surfingdb::table::schema::RowType::BOOL: {
      memcpy((bool*)(_payload + offset), &(v.p_val.bool_val), sizeof(bool));
      break;
    }
    case RowType::LONG: {
      memcpy((long*)(_payload + offset), &(v.p_val.long_val), sizeof(long));
      break;
    }
    case RowType::DOUBLE: {
      memcpy((double*)(_payload + offset), &(v.p_val.double_val), sizeof(double));
      break;
    }
    case RowType::STRING: {
      // be careful here with truncation, assume unit_size is equal or larger than string length
      memcpy((char*)(_payload + offset), (v.p_val.string_val.c_str()), sizeof(char) * f.unit_size);
      break;
    }
    case RowType::LIST: {
      //hack here
      assert(false);
      //memcpy((void*) vecotr_ptr.get(), (void*)&v.list_value[0], s.Type_Size.at(f.list_type) * f.unit_size);
      break;
    }
    case RowType::MAP: {
      assert(false);
      break;
    }
    }
  }

  /**
 * copy read RowBuffer of given field
 * @param f
 * @param v
 */
  void read(const Field& f, Value& v) {
    uint64_t offset = _offsets->at(f);
    switch (f.type) {
    case surfingdb::table::schema::RowType::VOID: {
      break;
    }
    case surfingdb::table::schema::RowType::INT: {
      int* int_ptr = (int*)(_payload + offset);
      v.p_val.int_val = *int_ptr;
      break;
    }
    case surfingdb::table::schema::RowType::BOOL: {
      bool* bool_ptr = (bool*)(_payload + offset);
      v.p_val.bool_val = *bool_ptr;
      break;
    }
    case RowType::LONG: {
      long* long_ptr = (long*)(_payload + offset);
      v.p_val.long_val = *long_ptr;
      break;
    }
    case RowType::DOUBLE: {
      double* double_ptr = (double*)(_payload + offset);
      v.p_val.double_val = *double_ptr;
      break;
    }
    case RowType::STRING: {
      char* char_ptr = (char*)(_payload + offset);
      v.p_val.string_val = std::string(char_ptr);
      break;
    }
    case RowType::LIST: {
      //hack here
      std::shared_ptr<char> vecotr_ptr((char*)(_payload + offset));
      //memcpy((void*) vecotr_ptr.get(), (void*)&v.list_value[0], s.Type_Size.at(f.list_type) * f.unit_size);
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
