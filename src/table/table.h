//
// Created by Chen Qin on 12/31/20.
//

#ifndef SURFINGDB_TABLE_H
#define SURFINGDB_TABLE_H

#include "table/gen-cpp/schema_constants.h"
#include "table/gen-cpp/schema_types.h"

#include <future>
#include <mpi.h>
#include "CombineOp.h"
#include "Node.h"
#include "Operator.h"
#include "ParDoOp.h"
#include "PartitionOp.h"
#include "row.h"

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
  uint8_t* _payload;

  size_t _count = 0;    // number of rows in table
  size_t _pending = 0;
  size_t offset = 0;    //current offset position
  MPI_Datatype type;    //used to run communication with other processes
public:
  TempTable(const std::shared_ptr<TableSchema> sharedPtr) {
    this->schema_ptr = sharedPtr;
    offset = 0;
    _payload = (uint8_t*) malloc(HUGE_PAGE_SIZE);
    memset(_payload, 0, HUGE_PAGE_SIZE);
    _count = 0;
    LOG(INFO) << "for testing only";
  }
  TempTable(const std::shared_ptr<Node> node, const std::shared_ptr<TableSchema> sharedPtr) {
    this->schema_ptr = sharedPtr;
    _payload = (uint8_t*) malloc(HUGE_PAGE_SIZE);
    memset(_payload, 0, HUGE_PAGE_SIZE);
    offset = 0;
    _count = 0;
    this->ptr = node;
    MPI_Type_contiguous(sharedPtr->_size, MPI_CHAR, &type);
    MPI_Type_commit(&type);
  }

  ~TempTable() {
    free(_payload);
    offset = 0;
    _count = 0;
    MPI_Type_free(&type);
  }

  void complete() {
    this->_count = _pending;
    _pending = 0;
  }

  void ingest(RowBuffer row) {
    //CHECK_EQ(row._schema_sig, schema_ptr->_schema_sig);
    CHECK_EQ(schema_ptr->_size, row.size());
    // CHECK_LT(row.size() + offset, _payload.max_size());
    memcpy(_payload + offset, row.payload_ptr(), row.size());
    LOG(INFO) << "write "<< &_payload <<" offset " << offset << " size " << row.size();
    int* int_ptr = (int*) row.payload_ptr();
    LOG(INFO) << "write int " << *int_ptr;
    offset += row.size();
    _count++;
  }

  void async_send(int d, size_t count, MPI_Request& request) {
    CHECK_LE(count, _count);
    MPI_Send(&count, 1, MPI_UNSIGNED_LONG, d, 0, MPI_COMM_WORLD);
    MPI_Isend(_payload, count, type, d, 0, MPI_COMM_WORLD, &request);
  }

  void async_recv(int s, MPI_Request& request) {
    CHECK_EQ(_pending, 0);
    size_t count;
    MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, s, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    _pending = count;
    CHECK_LE(schema_ptr->_size * count, HUGE_PAGE_SIZE);
    MPI_Irecv(_payload, count, type, s, 0, MPI_COMM_WORLD, &request);
  }
  /**
   * directly read from temptable memory
   * @param index
   * @return
   */
  std::unique_ptr<RowBuffer> read(int index) {
    CHECK_LT(index, _count);
    CHECK_NOTNULL(schema_ptr);
    auto s =  std::make_unique<RowBuffer>(schema_ptr);
    LOG(INFO) << "read " << &_payload << " index " << index << " size " << schema_ptr->_size;
    memcpy(s->payload_ptr(), _payload + (schema_ptr->_size * index), schema_ptr->_size);
    return s;
  }

  size_t count() {
    return this->_count;
  }
};

/**
         * RowTable hold ingested chunks of Row in memory
         * - shuffle row with other MPI processes
         * - join with other columnar tables
         * - periodical flush to columnar table
         */
template <class Row>
class RowTable {
public:
  // defines the node row table bind to
  std::shared_ptr<Node> ptr;
  std::shared_ptr<std::vector<Row>> _chunks;

  RowTable(const std::shared_ptr<Node> node) {
    this->ptr = node;
    this->_chunks = std::make_shared<std::vector<Row>>();
  }

  ~RowTable() {
    // MPI_Type_free(&row_type);
  }

  MPI_Op reducer_op;
  MPI_Datatype row_type = 0;

  void ingest(const std::vector<Row>& rows) {
    _chunks.get()->insert(_chunks.get()->end(), rows.begin(), rows.end());
  }

  void ingest(const Row& row) {
    _chunks.get()->push_back(row);
  }

  void partition(PartitionOp<Row>& op) {
    std::vector<Row>* payload = this->_chunks.get();
    op.process(*payload, *payload, *payload);
  }

  /**
             * combine two row table (left, right) into out table
             * @param otherTable
             */
  void combine(const RowTable<Row>& rightTable, RowTable<Row>& outTable) {
    CombineOp<Row> op;
    const std::vector<Row> rowL = *(this->_chunks.get());
    const std::vector<Row> rowR = *(rightTable._chunks.get());
    std::vector<Row> rowOut = *(outTable._chunks.get());
    op.process(rowL, rowR, rowOut);
  }

  /**
             * vectorized "map"
             * @tparam RowInR
             * @tparam RowOut
             * @param rightTable
             * @param outTable
             * @param op
             */
  template <class RowInR, class RowOut>
  void parDo(const RowTable<RowInR>& rightTable, RowTable<int>& outTable, ParDoOp<Row, RowInR, RowOut>& op) {
    assert(op.Optype() == OperatorType::ParDo);
    const std::vector<Row> rowL = *(this->_chunks.get());
    const std::vector<RowInR> rowR = *(rightTable._chunks.get());
    std::vector<RowOut> rowOut = *(outTable._chunks.get());
    op.process(rowL, rowR, rowOut);
  }

  void send(int source, int dest, char* ptr1, int size) {
    if (this->ptr->rank == source) {
      MPI_Send((void*)ptr1, size, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
    } else if (this->ptr->rank == dest) {
      MPI_Recv((void*)ptr1, size, MPI_CHAR, source, 0, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);
    }
  }

  /**
             * blocking send chunk of data to another node
             */
  void send(int source, int dest, const Row& data, const MPI_Datatype& row_type1) {
    if (this->ptr->rank == source) {
      MPI_Send((void*)&data, 1, row_type1, dest, 0, MPI_COMM_WORLD);
    } else if (this->ptr->rank == dest) {
      MPI_Recv((void*)&data, 1, row_type1, source, 0, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);
    }
  }

  /**
             * blocking send vector of struct mychunk
             */
  void sendAll(int source, int dest, int size, const std::vector<Row>& chunks) {
    assert(!chunks.empty());
    if (this->ptr->rank == source) {
      MPI_Send((void*)&chunks[0], size, this->row_type, dest, 0, MPI_COMM_WORLD);
    } else if (this->ptr->rank == dest) {
      std::vector<Row> results(size);
      MPI_Recv((void*)&results[0], size, this->row_type, source, 0, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);
    }
  }

  void allreduce(const std::vector<Row>& chunks) {
    // create list of user defined MPI_OPs
    auto op = [](Row a, Row b) { return a + b; };

    MPI_Op_create(&reducer<Row, +op>, true, &this->reducer_op);
    std::vector<Row> result(chunks.size());
    // used to collective get quantile sketch of each columns https://datasketches.apache.org/docs/Quantiles/QuantilesCppExample.html
    MPI_Allreduce((void*)&chunks[0], (void*)&result[0], chunks.size(), row_type, reducer_op,
                  MPI_COMM_WORLD);
    MPI_Op_free(&reducer_op);
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_TABLE_H
