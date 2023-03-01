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
#include <arrow/buffer.h>
#include <arrow/buffer_builder.h>
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
class mrow {
private:
  /**
   * @brief used by container datatype
   *
   */
  Field _header;
  std::shared_ptr<meta::mschema> schema_ptr;
  size_t _schema_sig;
  /**
   * @brief use duo pointer to avoid double free when random fetch a mrow in mtable
   * 
   */
  uint8_t* _payload;
  size_t* _actual_row_size;
  /**
   * @brief use buffer builder to allocate flexible sized rows
   * 
   */
  std::shared_ptr<arrow::Buffer> _buffer;

  void _pwrite(const Field& f, const void* data, const uint64_t& offset);

  size_t _pread(const Field& f, void* dataptr, const uint64_t& offset);

public:
  /**
   * @brief build dedicated buffer managed by row instance lifecycle
   * 
   * @param schemaptr 
   */
  mrow(std::shared_ptr<meta::mschema> schemaptr);

  /**
   * @brief point to starting offset of memory
   * 
   * @param schemaptr 
   * @param payloadptr 
   */
  mrow(std::shared_ptr<meta::mschema> schemaptr, uint8_t* payloadptr);

  ~mrow();

  size_t schema_sig();

  /**
   * @brief row size and capacity 
   * 
   * @return size_t 
   */
  size_t size();
  size_t capacity();

  uint8_t* payload_ptr();

  /**
   * copy readRow mrow map given field
   * @param f
   * @param v
   */
  size_t read(const Field& f, Value& v);
  std::vector<size_t> readLen(const Field& f, size_t offset);
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
#endif // SURFINGDB_ROW_H
