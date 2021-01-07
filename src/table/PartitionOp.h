//
// Created by cq on 1/5/21.
//

#ifndef SURFINGDB_PARTITIONOP_H
#define SURFINGDB_PARTITIONOP_H

#include <mpi.h>
#include "Operator.h"

#pragma once

namespace surfingdb {
namespace table {
/**
        * in place shuffle records based on partitioner to processors
        * @tparam Row
        */
template <typename Row>
class PartitionOp : public Operator<Row, Row, Row> {
protected:
  int rank;
  int world;
  std::function<int(const int&, const int&, const Row&)> partitioner;

public:
  PartitionOp(const int rank, const int world, const MPI_Datatype& datatype,
              const std::function<int(const int&, const int&, const Row&)>& par)
    : Operator<Row, Row, Row>() {
    this->rank = rank;
    this->world = world;
    this->_type_out = datatype;
    this->partitioner = par;
    this->_type = Partition;
  }

  void process(const std::vector<Row>&, const std::vector<Row>&, std::vector<Row>& rowOut) {
    //LOG(INFO) << "output row " << rowOut.size();
    //LOG(INFO) << "mpi datatype " << this->_type_out;
    // organize record per rank
    std::vector<Row> send_buffer[world];
    int j;

    for (size_t ii = 0; ii < rowOut.size(); ii++) {
      auto shard = partitioner(rank, world, rowOut[ii]);
      send_buffer[shard].push_back(rowOut[ii]);
    }

    rowOut.clear();
    MPI_Barrier(MPI_COMM_WORLD);

    //number of records send to each process
    int send[world];
    //number of records recv from each process
    int recv[world];
    int total_recv;

    for (j = 0; j < world; j++) {
      send[j] = send_buffer[j].size();
      if (j == rank) {
        recv[j] = send[j];
        total_recv += recv[j];
        continue;
      }

      MPI_Send(&send[j], 1, MPI_INT, j, 0, MPI_COMM_WORLD);
      MPI_Recv(&recv[j], 1, MPI_INT, j, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      total_recv += recv[j];
    }

    // place received record in array index start with 0
    int displ[world];
    displ[0] = 0;
    for (j = 1; j < world; j++) {
      displ[j] = displ[j - 1] + recv[j - 1];
    }
    rowOut.resize(total_recv);
    // for each process, gather rows shard to that process
    for (j = 0; j < world; j++) {
      MPI_Gatherv(&send_buffer[j][0], send[j], this->_type_out,
                  &rowOut[0], &recv[0], &displ[0], this->_type_out, j,
                  MPI_COMM_WORLD);
    }
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_PARTITIONOP_H
