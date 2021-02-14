//
// Created by cq on 2/6/21.
//

#ifndef SURFINGDB_MTABLE_H
#define SURFINGDB_MTABLE_H
#pragma once

#include "table.h"

namespace surfingdb {
namespace table {
using surfingdb::meta::node;
using namespace surfingdb::meta;

class mtable {
private:
  ValueHasher value_hasher;

  std::vector<uint8_t> payload;
  size_t row_count = 0;     // number of rows in table
  size_t offset = 0;        //current offset position
  size_t schedule_size = 0; // RMA memory size

  // defines the node row table bind to
  std::shared_ptr<node> node_ptr;
  std::shared_ptr<TableSchema> schema_ptr;

public:
  MPI_Win win;
  uint8_t* schedule;
  //partition field
  std::unique_ptr<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>> key_dist; // key hash and per node counts
  std::unique_ptr<std::map<size_t, std::vector<size_t>, std::less<size_t>>> key_groups;               // local key, offsets map
  std::unique_ptr<std::map<int, size_t>> placement_index;                                             // placement , start index of rows

  mtable(const std::shared_ptr<node>, const std::shared_ptr<TableSchema>, size_t capacity);
  ~mtable();
  void group(const Field& f);
  void placement_sort(const Field& f);
  void flush_rma_memory(size_t rows);
  void copy_rma_memory(size_t rows); //deprecated
  void reserve_rma_memory(size_t rows);
  uint8_t* range_ptr(int dest);
  uint8_t* payload_ptr();
  size_t placement(size_t key);
  size_t row_size();
  std::shared_ptr<TableSchema> getSchema();
  std::unique_ptr<RowBuffer> readRow(int index);
  void appendRow(RowBuffer& row);
  void verify(const Field& field);
  void reserveRow(size_t rows);
  void load(const string& path);
  void flush(const string&, uint8_t*, size_t, size_t);
  void readFields(std::vector<Field> fields, float* data);
  void readField(const Field& field, float* data);
  void writeField(const Field& field, const float* data);
  std::shared_ptr<node> getNodePtr();
};

} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_MTABLE_H
