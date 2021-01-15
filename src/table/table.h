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
#include "XGBOperator.h"
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

  /**
   * for xgboost, we need to extract list of numerical fields and pass as array of float
   * @param fields features
   * @param data pointer to external float array
   */
  void readFields(std::vector<Field> fields, float* data) {
    CHECK_NOTNULL(data);
    CHECK_GT(fields.size(), 0);

    for(const auto& f : fields) {
      CHECK_EQ(f.type, RowType::DOUBLE);
    }

    size_t columns = fields.size();
    memset(data, 0, sizeof(float)*columns*_count);
    for(size_t i = 0 ; i < _count; i++) {
      for(size_t j = 0 ; j < columns ; j++) {
        Value v;
        read(i)->read(fields[j], v);
        data[i * columns + j] = (float) v.p_val.double_val;
      }
    }
  }

  size_t count() {
    return this->_count;
  }

  void process(XGBOperator& op) {
    float data[op.features()*_count];
    readFields(op.fields, data);
    //TODO(test with a different temptable data)
    op.fillTrainingData(data, data, _count, op.features());
    op.train();
    op.predict();
    free(data);
  }

  void process(KMeanOperator& op) {
    auto k = op.k;
    auto fields = op.fields;
    double centers[k][fields.size()]; //more efficient ds
    double final_centers[k][fields.size()];

    if(!op.inited) {
      std::unordered_map<int, size_t> local_picks;
      op.init(_count, ptr->rank, ptr->world, local_picks);
      for (auto pick : local_picks) {
        for (size_t i = 0; i < fields.size(); i++) {
          Value v;
          read(pick.second)->read(fields.at(i), v);
          centers[pick.first][i] = v.p_val.double_val;
        }
      }

      MPI_Allreduce(&centers, &final_centers, k * fields.size(), MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);
      op.inited = true;
      memcpy(op.centers, centers, sizeof(double) * k * fields.size());
    } else {
      memcpy(final_centers, op.centers, sizeof(double) * k * fields.size());
    }
    // step 4, caculate distance and group to k, group to k clusters
    double dist[k]; // each row distance to recvcenter[i]

    for (size_t j = 0; j < _count; j++) {
      for (int point = 0; point < k; point++) {
        auto center = final_centers[point];
        dist[point] = distance(fields, j, center);
      }
      int g = 0; //put on first group
      int shortest = dist[0];
      for (int m = 1; m < k; m++) {
        if (dist[m] < shortest) {
          shortest = dist[m];
          g = m;
        }
      }
      // put row j to group g
      op.addGroup(g, j);
    }
    // step 6, find mean position of each group
    size_t members[k];
    double total[k][fields.size()];
    // group
    for (int i = 0; i < k; i++) {
      members[i] = op.groups.at(i).size();
      // each row in group i
      for (auto index : op.groups.at(i)) {
        // field j in a row index in group i
        for (size_t j = 0; j < fields.size(); j++) {
          Value v;
          read(index)->read(fields.at(j), v);
          total[i][j] += v.p_val.double_val;
        }
      }
    }
    size_t total_members[k];
    MPI_Allreduce(members, total_members, k, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    double grandtotal[k][fields.size()];
    MPI_Allreduce(total, grandtotal, k * fields.size(), MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);

    // update centers with mean value of each field centers[k][fields.size()];
    for (int i = 0; i < k; i++) {
      for (size_t j = 0; j < fields.size(); j++) {
        centers[i][j] = grandtotal[i][j] / total_members[i];
      }
    }
    memcpy(op.centers, centers, sizeof(double)*k*fields.size());
    op.iteration++;

    if (op.shouldStop()) {
      LOG(INFO) << "complete k-mean training";
      ptr->forward();
    } else {
      //LOG(INFO) << "next iteration of k-mean";
      process(op);
    }
  }

  inline double distance(const std::vector<Field>& fields, size_t index, double center[]) {
    double sum = 0;
    size_t k = 0;
    for (auto f : fields) {
      Value v;
      read(index)->read(f, v);
      sum += std::abs(v.p_val.double_val - center[k]) * std::abs(v.p_val.double_val - center[k]);
      k++;
    }
    return sqrt(sum);
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
