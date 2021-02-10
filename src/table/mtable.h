//
// Created by cq on 2/6/21.
//

#ifndef SURFINGDB_MTABLE_H
#define SURFINGDB_MTABLE_H
#pragma once

#include "table.h"
#include "mchunk.h"

namespace surfingdb {
namespace table {
using surfingdb::node::Node;

class mtable {
private:
  ValueHasher value_hasher;

  MPI_Win win;
  uint8_t* schedule;

  std::vector<uint8_t> payload;
  size_t row_count = 0; // number of rows in table
  size_t offset = 0;    //current offset position
  size_t schedule_size = 0; // RMA memory size

  // defines the node row table bind to
  std::shared_ptr<Node> node_ptr;
  std::shared_ptr<TableSchema> schema_ptr;

  //partition field
  std::unique_ptr<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>> key_dist; // key hash and per node counts
  std::unique_ptr<std::map<size_t, std::vector<size_t>, std::less<size_t>>> key_groups;               // local key, offsets map
  std::unique_ptr<std::map<int, size_t>> placement_index;                                             // placement , start index of rows
  void group(const Field& f);
  void placement_sort(const Field& f);
  void flush_rma_memory(size_t rows);
  void reserve_rma_memory(size_t rows);
  uint8_t* range_ptr(int dest);
  uint8_t* payload_ptr();
  size_t placement(size_t key);
public:
  mtable(const std::shared_ptr<Node>, const std::shared_ptr<TableSchema>, size_t capacity);
  ~mtable();
  std::unique_ptr<RowBuffer> read(int index);
  void shuffle(const Field& f);
  void ingest(RowBuffer& row);
  void verify(const Field& field);
  void reserve(size_t rows);
  void load(const string& path);
  void flush(const string& path);
};

} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_MTABLE_H
