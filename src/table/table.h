//
// Created by Chen Qin on 12/31/20.
//

#ifndef SURFINGDB_TABLE_H
#define SURFINGDB_TABLE_H

#include <cstdarg>
#include <fcntl.h>
#include <future>
#include <math.h>
#include <mpi.h>
#include <omp.h>
#include <stdio.h>
#include <string.h>

#include "KMeanOperator.h"
#include "row.h"
#include "xgbop.h"

#pragma once

namespace surfingdb {
namespace table {
using surfingdb::node::node;

/**
         * use class operator+ to get row count map a key
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
 * stores vector map RowBuffer with same schema
 */
class TempTable {
private:
  // defines the node row table bind to
  std::shared_ptr<node> node_ptr;
  std::shared_ptr<TableSchema> schema_ptr;
  //partition field
  std::unique_ptr<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>> key_dist; // key hash and per node counts
  std::unique_ptr<std::map<size_t, std::vector<size_t>, std::less<size_t>>> key_groups;               // local key, offsets map
  std::unique_ptr<std::map<int, size_t>> placement_index;                                             // placement , start index of rows

  uint8_t* rma_payload_ptr; //used to manage RMA shared memory
  size_t row_count = 0; // number of rows in table
  size_t offset = 0;    //current offset position

  struct iovec iov[FILE_IO_VECTOR];
  ssize_t nr;
  int fd;

  ValueHasher value_hasher;

public:
  std::vector<uint8_t> payload;
  TempTable(const std::shared_ptr<TableSchema> sharedPtr) {
    this->schema_ptr = sharedPtr;
    offset = 0;
    payload.reserve(MEM_PAGE_SIZE);
    rma_payload_ptr = &payload[0];
    row_count = 0;
    key_dist = std::make_unique<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>>();
    key_groups = std::make_unique<std::map<size_t, std::vector<size_t>, std::less<size_t>>>();
    placement_index = std::make_unique<std::map<int, size_t>>();
    LOG(INFO) << "for testing only";
  }
  TempTable(const std::shared_ptr<node> node, const std::shared_ptr<TableSchema> sharedPtr) {
    this->schema_ptr = sharedPtr;
    payload.resize(MEM_PAGE_SIZE);
    rma_payload_ptr = &payload[0];
    offset = 0;
    row_count = 0;
    key_dist = std::make_unique<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>>();
    key_groups = std::make_unique<std::map<size_t, std::vector<size_t>, std::less<size_t>>>();
    placement_index = std::make_unique<std::map<int, size_t>>();
    this->node_ptr = node;
  }

  ~TempTable() {
    clear();
  }

  void clear() {
    payload.clear();
    payload.shrink_to_fit();
    offset = 0;
    row_count = 0;
    key_groups->clear();
    key_dist->clear();
    placement_index->clear();
  }

  uint8_t* payload_ptr() {
    return &this->payload[0];
  }

  void ingest(RowBuffer& row) {
    CHECK_EQ(row.schema_sig(), schema_ptr->schema_sig()); //check schema signature
    CHECK_EQ(schema_ptr->size(), row.row_size());             //check row size
    CHECK_LT(row.row_size() + offset, payload.max_size());    // check capacity of temp table
    memcpy(&payload[offset], row.payload_ptr(), row.row_size());
    offset += row.row_size();
    row_count++;

    // expand table size if needed
    if (offset >= payload.capacity() - row.row_size()) {
      payload.resize(payload.size() + MEM_PAGE_SIZE);
    }
  }

  /**
   * find better hash function
   * @param key
   * @return
   */
  size_t decidePlacement(size_t key) {
    int base = node_ptr->world % 2 == 0 ? node_ptr->world - 1 : node_ptr->world;
    return key % base;
  }
  /**
   * for dest process, find all aim to send data
   * @param key
   * @param dest
   * @param request
   */
  void recv_per_rank(int source, TempTable& out) {
    CHECK(out.schema_ptr->schema_sig() == this->schema_ptr->schema_sig());
    size_t rows = 0;
    MPI_Status status;
    MPI_Request request;

    for (auto g : *key_dist) {
      size_t placement = decidePlacement(g.first);
      if (placement == (size_t)node_ptr->rank) {
        for (auto item : g.second) {
          if (item.first == source) {
            rows += item.second;
          }
        }
      }
    }
    TempTable tempTable(node_ptr, schema_ptr);
    tempTable.payload.resize(schema_ptr->size() * rows);
    tempTable.row_count += rows;

    CHECK_EQ(MPI_Irecv(tempTable.payload_ptr(), rows, *(schema_ptr->getType()), source, source * 100 + node_ptr->rank, MPI_COMM_WORLD, &request), MPI_SUCCESS);
    MPI_Wait(&request, &status);
    for (size_t s = 0; s < tempTable.count(); s++) {
      Value v;
      auto item = tempTable.read(s);
#pragma omp critical
      out.ingest(*item.get());
    }
  }

  /**
   * directly readRow from temptable memory
   * @param index
   * @return
   */
  inline std::unique_ptr<RowBuffer> read(int index) {
    CHECK_LT(index, row_count);
    CHECK_NOTNULL(schema_ptr);
    return std::make_unique<RowBuffer>(schema_ptr, &payload[schema_ptr->size() * index]);
  }

  size_t count() {
    return this->row_count;
  }

  void process(KMeanOperator& op) {
    auto k = op.k;
    auto fields = op.fields;
    double centers[k][fields.size()]; //more efficient ds
    double final_centers[k][fields.size()];

    if (!op.inited) {
      std::unordered_map<int, size_t> local_picks;
      op.init(row_count, node_ptr->rank, node_ptr->world, local_picks);
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

    for (size_t j = 0; j < row_count; j++) {
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
    memcpy(op.centers, centers, sizeof(double) * k * fields.size());
    op.iteration++;

    if (op.shouldStop()) {
      LOG(INFO) << "complete k-mean training";
      node_ptr->forward();
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
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_TABLE_H
