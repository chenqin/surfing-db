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
  RowBuffer(RowSchema& schema);

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
  void write(Field& f, Value& v);

  /**
 * copy read RowBuffer of given field
 * @param f
 * @param v
 */
  void read(Field& f, Value& v);
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_ROW_H
