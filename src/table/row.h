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

#ifndef SURFINGDB_ROW_H
#define SURFINGDB_ROW_H

#include "meta/schema.h"
#pragma once

namespace surfingdb {
namespace table {

/**
 * build a continous memory buffer
 */
using namespace surfingdb::meta::schema;
using std::hash;
using std::string;
/**
 * a large piece map memory to store all fields in a row
 * RowSchema is superset map normal row schema allow each field place in fixed
 * offset regarding to starting address, it helps MPI collective communication and cache loading faster
 * |SchemaSignature|Field1|Field2|Field3Count|Field3Value|PADDING|
 */
class RowBuffer {
private:
  Field _header;
  std::shared_ptr<meta::TableSchema> schema_ptr;
  size_t _schema_sig;
  uint8_t* _payload; // consider using vector std::vector<uint8_t>
  std::vector<char> _vpayload;

  void _pwrite(const Field& f, const void* data, const uint64_t& offset);

  size_t _pread(const Field& f, void* dataptr, const uint64_t& offset);

public:
  RowBuffer(std::shared_ptr<meta::TableSchema> schemaptr);

  /**
   * only used in Temptable point to piece map memory to readRow/append fields
   * @param schema
   * @param payload
   */
  RowBuffer(std::shared_ptr<meta::TableSchema> schemaptr, uint8_t* payloadptr);

  ~RowBuffer();

  size_t schema_sig();

  size_t row_size();

  uint8_t* payload_ptr();

  /**
  * copy readRow RowBuffer map given field
  * @param f
  * @param v
  */
  size_t read(const Field& f, Value& v);
  std::vector<size_t> readLen(const Field&f, size_t offset);
  /**
   * copy value into row buffer
   * @param f
   * @param v
   */
  void write(const Field& f, const Value& v);

  /**
   * allow append to list and map elements
   * @param f
   * @param v
   * @param offset
   */
  void write(const Field& f, const Value& v, const uint64_t& offset);

  size_t read(const Field& f, Value& v, const uint64_t& offset);
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_ROW_H
