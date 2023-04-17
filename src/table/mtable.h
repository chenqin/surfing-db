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

#ifndef SURFINGDB_MTABLE_H
#define SURFINGDB_MTABLE_H
#pragma once

#include <arrow/api.h>
#include <cstdarg>
#include <fcntl.h>
#include <future>
#include <math.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include "meta/node.h"
#include "mrow.h"

#pragma once

namespace surfingdb {
namespace table {
using surfingdb::meta::node;
using namespace surfingdb::meta;

/**
 * mtable is foundation data management unit.
 * by default, it served as continous memory in row based layout during map, shuffle time
 * after shuffle, table is stored in columnar vectorized processing
 */
class mtable {
private:
  uint8_t* payload;
  size_t capacity = 0;
  ValueHasher value_hasher;

  size_t schedule_size = 0; // RMA memory size

  // defines the node row table bind to
  std::shared_ptr<node> node_ptr;
  std::shared_ptr<mschema> schema_ptr;

public:
  bool sorted = false;
  int rank;
  int world;
  
  size_t offset = 0;    // current offset position
  MPI_Win win = MPI_WIN_NULL;
  uint8_t* schedule;
  size_t row_count = 0; // number of rows in table
  // partition field
  std::unique_ptr<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>> key_dist; // key hash and per node counts
  std::unique_ptr<std::map<size_t, std::vector<size_t>, std::less<size_t>>> key_groups;               // local key, offsets map
  std::unique_ptr<std::map<int, size_t>> placement_index;                                             // placement , start index of rows
  std::unordered_map<Field, size_t, FieldHasher> max_unit_size;

  mtable(const std::shared_ptr<node>, const std::shared_ptr<mschema>, size_t capacity);
  ~mtable();
  void release();
  void group(const Field& f, bool);
  std::shared_ptr<mtable> placement_sort(const Field& f, std::function<size_t(size_t, int, int)>);
  uint8_t* range_ptr(int dest);
  /**
   * @brief build RDMA window for remote memory access, local memory no longer able to mutabe window, remote allow to mutate only once
   * 
   */
  void build_window();
  void release_window();
  size_t range_row_size(int dest);
  uint8_t* payload_ptr();
  size_t placement(size_t key);
  size_t row_size();
  std::shared_ptr<mschema> getSchema();
  std::unique_ptr<mrow> readRow(int index);
  void appendRow(mrow& row);
  void appendRows(std::vector<std::shared_ptr<mrow>>& rows);
  void verifyShuffle(const Field& field, std::function<size_t(size_t, int, int)>);
  void reserveRow(size_t rows);
  void load(const string& path);
  void flush(const string&, uint8_t*, size_t, size_t);
  void readFields(std::vector<Field> fields, float* data);
  void readField(const Field& field, float* data);
  void writeField(const Field& field, const float* data);
  std::shared_ptr<node> getNodePtr();
  void print();
};

} // namespace table
} // namespace surfingdb
#endif // SURFINGDB_MTABLE_H
