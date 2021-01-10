//
// Created by Chen Qin on 12/31/20.
//

#ifndef SURFINGDB_TABLE_H
#define SURFINGDB_TABLE_H

#include "table/gen-cpp/schema_constants.h"
#include "table/gen-cpp/schema_types.h"

#include <arrow/api.h>
#include <mpi.h>
#include "CombineOp.h"
#include "Node.h"
#include "Operator.h"
#include "ParDoOp.h"
#include "PartitionOp.h"
#include "Row.h"

#pragma once

namespace surfingdb {
namespace table {
using surfingdb::node::Node;

/**
         * columnarTable is maintained collectively to as one logical arrow table
         */
template <class Row>
class ColumnarTable {
private:
  std::shared_ptr<arrow::Table> tableptr;

public:
  ColumnarTable() {
  }

  void toTable(std::shared_ptr<std::vector<Row>>& rowptr) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    arrow::Int32Builder a_builder(pool);
    arrow::Int64Builder b_builder(pool);
    for (auto chunk : *rowptr.get()) {
      a_builder.Append(chunk.a);
      b_builder.Append(chunk.b);
    }
    std::shared_ptr<arrow::Array> a_array;
    a_builder.Finish(&a_array);
    std::shared_ptr<arrow::Array> b_array;
    b_builder.Finish(&b_array);
    tableptr = arrow::Table::Make(Row::schema(), { a_array, b_array });
  }
};

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
  std::shared_ptr<RowSchema> schema_ptr;
  std::vector<char> _payload;

  size_t schema_hash;   // hash of schema fields
  size_t _count = 0;    // number of rows in table
  size_t unit_size = 0; //size of earch rowbuffer payload
  size_t offset = 0;    //current offset position
  MPI_Datatype type;    //used to run communication with other processes
public:
  TempTable(const std::shared_ptr<Node> node, const std::shared_ptr<RowSchema> schema) {
    this->ptr = node;
    this->schema_ptr = schema;
    SchemaHasher s;
    schema_hash = s.operator()(*schema.get());
    this->_payload.resize(HUGE_PAGE_SIZE);
    _count = 0;
    unit_size = getSchemaSize(*schema.get());

    MPI_Type_contiguous(unit_size, MPI_CHAR, &type);
    MPI_Type_commit(&type);
  }

  ~TempTable() {
    _payload.clear();
    MPI_Type_free(&type);
  }

  void ingest(RowBuffer row) {
    CHECK_EQ(row._schema_sig, schema_hash);
    CHECK_EQ(unit_size, row.size());
    CHECK_LT(row.size() + offset, _payload.max_size());
    memcpy(&_payload[0], row.payload(), row.size());
    offset += row.size();
    _count++;
  }

  void send(int s, int d, size_t count) {
    if (ptr->rank == s) {
      CHECK_LE(count, _count);
      MPI_Send(&_payload[0], count, type, d, 0, MPI_COMM_WORLD);
    }
    if (ptr->rank == d) {
      CHECK_LE(unit_size * count, HUGE_PAGE_SIZE);
      MPI_Recv(&_payload[0], count, type, s, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      _count = count;
    }
  }

  std::unique_ptr<RowBuffer> read(int index) {
    CHECK_LT(index, _count);
    return std::make_unique<RowBuffer>(*schema_ptr.get(), reinterpret_cast<uint8_t*>(&_payload[0] + unit_size * index));
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

  void flush(std::shared_ptr<ColumnarTable<Row>> colptr) {
    colptr.get()->toTable(this->_chunks);
    _chunks.get()->clear();
  }

  /**
             * register MPI_struct type based on row schema
             * @param schemaptr arrow Schema
             */
  void regType(std::shared_ptr<arrow::Schema> schemaptr) {
    int count = schemaptr->num_fields();
    int array_of_blocklengths[count + 1];
    MPI_Aint array_of_displacements[count + 1];
    MPI_Datatype array_of_types[count + 1];
    int i = 0;
    for (auto field : schemaptr->fields()) {
      auto t = field->type();

      int prev_displ = i == 0 ? 0 : array_of_displacements[i - 1];
      bool isInt32 = t->Equals(arrow::int32());
      bool isInt64 = t->Equals(arrow::int64());
      bool isPrimitive = isInt32 || isInt64;

      if (isPrimitive) {
        array_of_blocklengths[i] = 1;
      }
      if (isInt32) {
        array_of_displacements[i] = prev_displ + sizeof(int);
        array_of_types[i] = MPI_INT;
      } else if (isInt64) {
        array_of_displacements[i] = prev_displ + sizeof(long);
        array_of_types[i] = MPI_LONG;
      }
      i++;
      continue;
    }
    array_of_blocklengths[i] = 10;
    array_of_displacements[i] = array_of_displacements[i - 1] + sizeof(char);
    array_of_types[i] = MPI_CHAR;

    MPI_Datatype tmp_type;
    MPI_Aint lb, extent;

    MPI_Type_create_struct(count, array_of_blocklengths, array_of_displacements,
                           array_of_types, &tmp_type);
    MPI_Type_get_extent(tmp_type, &lb, &extent);
    MPI_Type_create_resized(tmp_type, lb, extent, &row_type);
    MPI_Type_commit(&row_type);
  }

  /**
             * partition run in place shuffle
             * @param op
             */
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
