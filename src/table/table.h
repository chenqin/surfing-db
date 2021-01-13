//
// Created by Chen Qin on 12/31/20.
//

#ifndef SURFINGDB_TABLE_H
#define SURFINGDB_TABLE_H

#include <cstdarg>
#include <future>
#include <math.h>
#include <mpi.h>
#include "KMeanOperator.h"
#include "Node.h"
#include "row.h"
#include "table/gen-cpp/schema_constants.h"
#include "table/gen-cpp/schema_types.h"

#pragma once

namespace surfingdb {
namespace table {
using surfingdb::node::Node;

/**
         * use class operator+ to get row count of a key
         * @tparam T
         * @param a
         * @param b
         * @param len
         */
template <class T, auto op>
void reducer(void* a, void* b, int* len, MPI_Datatype*) {
  T* aa = static_cast<T*>(a);
  T* bb = static_cast<T*>(b);
  //#pragma omp simd
  for (int i = 0; i < *len; ++i) {
    bb[i] = op(aa[i], bb[i]);
  }
  //#pragma omp barrier
}

/**
 * stores vector of RowBuffer with same schema
 */
class TempTable {
private:
  // defines the node row table bind to
  std::shared_ptr<Node> ptr;
  std::shared_ptr<TableSchema> schema_ptr;
  std::vector<uint8_t> _payload;

  size_t _count = 0; // number of rows in table
  size_t _pending = 0;
  size_t offset = 0; //current offset position
  MPI_Datatype type; //used to run communication with other processes

  std::shared_ptr<std::unordered_map<size_t, std::set<size_t>>> _groups;

public:
  TempTable(const std::shared_ptr<TableSchema> sharedPtr) {
    this->schema_ptr = sharedPtr;
    offset = 0;
    _payload.resize(HUGE_PAGE_SIZE);
    _count = 0;
    _groups = std::make_shared<std::unordered_map<size_t, std::set<size_t>>>();
    LOG(INFO) << "for testing only";
  }
  TempTable(const std::shared_ptr<Node> node, const std::shared_ptr<TableSchema> sharedPtr) {
    this->schema_ptr = sharedPtr;
    _payload.resize(HUGE_PAGE_SIZE);
    offset = 0;
    _count = 0;
    _groups = std::make_shared<std::unordered_map<size_t, std::set<size_t>>>();
    this->ptr = node;
    MPI_Type_contiguous(sharedPtr->size(), MPI_CHAR, &type);
    MPI_Type_commit(&type);
  }

  ~TempTable() {
    _payload.clear();
    offset = 0;
    _count = 0;
    MPI_Type_free(&type);
  }

  void complete() {
    this->_count = _pending;
    _pending = 0;
  }

  uint8_t* payload_ptr() {
    return &this->_payload[0];
  }

  void ingest(RowBuffer row) {
    CHECK_EQ(row.schema_sig(), schema_ptr->schema_sig()); //check schema signature
    CHECK_EQ(schema_ptr->size(), row.size());             //check row size
    CHECK_LT(row.size() + offset, _payload.max_size());   // check capacity of temp table
    memcpy(&_payload[offset], row.payload_ptr(), row.size());
    offset += row.size();
    _count++;
  }

  void async_send(int d, size_t count, MPI_Request& request) {
    CHECK_LE(count, _count);
    MPI_Send(&count, 1, MPI_UNSIGNED_LONG, d, 0, MPI_COMM_WORLD);
    MPI_Isend(&_payload[0], count, type, d, 0, MPI_COMM_WORLD, &request);
  }

  void async_recv(int s, MPI_Request& request) {
    CHECK_EQ(_pending, 0);
    size_t count;
    MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, s, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    _pending = count;
    CHECK_LE(schema_ptr->size() * count, HUGE_PAGE_SIZE);
    MPI_Irecv(&_payload[0], count, type, s, 0, MPI_COMM_WORLD, &request);
  }
  /**
   * directly read from temptable memory
   * @param index
   * @return
   */
  std::unique_ptr<RowBuffer> read(int index) {
    CHECK_LT(index, _count);
    CHECK_NOTNULL(schema_ptr);
    return std::make_unique<RowBuffer>(schema_ptr, &_payload[schema_ptr->size() * index]);
  }

  size_t count() {
    return this->_count;
  }

  void k_mean(int k, const std::vector<Field> fields) {
    KMeanOperator op(schema_ptr, k, fields);

    // step 1: figure out all rows in all processes
    size_t data_size = 0;
    MPI_Allreduce(&this->_count, &data_size, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
    size_t local_data_size[ptr->world], recv[ptr->world];
    memset(&local_data_size[0], 0, sizeof(size_t));
    memset(&recv[0], 0, sizeof(size_t));
    local_data_size[ptr->rank] = this->_count;
    MPI_Allreduce(&local_data_size[0], &recv[0], ptr->world, MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);

    // step 2: random pick k as centriod
    std::vector<size_t> offsets(data_size);
    if (ptr->rank == 0) {
      for (size_t i = 0; i < data_size; i++) {
        offsets.at(i) = i;
      }
      random_unique(offsets.begin(), offsets.end(), k);
    }
    std::vector<size_t> centriod(data_size);
    centriod.resize(k);
    // step 3, send cetriods to all nodes
    MPI_Allreduce(&offsets[0], &centriod[0], k, MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);
    size_t local_start_index = 0, local_end_index;
    for (int i = 0; i <= ptr->rank - 1; i++) {
      local_start_index += recv[i];
    }
    local_end_index = local_start_index + _count;
    //LOG(INFO) << ptr->rank << " " << local_start_index << " " << local_end_index;
    double centers[k][fields.size()];
    size_t j = 0;
    for (auto item : centriod) {
      if (item >= local_start_index && item < local_end_index) {
        auto pick = this->read(item - local_start_index);
        LOG(INFO) << "pick";
        for (size_t i = 0; i < fields.size(); i++) {
          Value v;
          pick->read(fields.at(i), v);
          centers[j][i] = v.p_val.double_val;
        }
      }
      j++;
    }
    double recvcenters[k][fields.size()];
    MPI_Allreduce(&centers, &recvcenters, k * fields.size(), MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);

    // step 4, caculate distance and group to k
    double dist[k];
    for (int i = 0; i < k; i++) {
      LOG(INFO) << i << " " << recvcenters[i][0];
      // center is recvcenters[i];
      // for each value
      for(j = 0 ; j < _count; j++) {
        Value v;
        double sum = 0;
        k =0;
        for(auto f : fields) {
          read(j)->read(f, v);
          sum += std::abs(v.p_val.double_val - recvcenters[i][k]) * std::abs(v.p_val.double_val - recvcenters[i][k]);
          k++;
        }
        double distance = sqrt(sum);
        dist[i] = distance;
        LOG(INFO) << dist[i];
      }
    }

    // step 5, group to k clusters

    // step 6, find mean position of each group
    // step 7, repeat step 1
    ptr->forward();
  }

  void group_by(Field f) {
    CHECK(schema_ptr->exist(f));
    _groups->clear();
    size_t max_size = 0;
    for (size_t i = 0; i < _count; i++) {
      auto r = read(i);
      Value v;
      r->read(f, v);
      size_t key = value_hasher.operator()(v);

      if (_groups->find(key) == _groups->end()) {
        std::set<size_t> set;
        _groups->insert({ key, set });
      }

      _groups->at(key).insert(i);
      max_size = max_size > _groups->at(key).size() ? max_size : _groups->at(key).size();
    }
    // EXCHANGE MAX_SIZE
    // run all reduce to merge hash(value) | node | offsets,,,|
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_TABLE_H
